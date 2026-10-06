"""SQLite-based daily pricing store for the Sharadar zipline bundle.

Provides readers and writers for daily OHLCV price data and adjustment
data (splits, dividends, mergers) stored in SQLite databases.
"""
import os
import sqlite3
from contextlib import closing

import click
import numpy as np
import pandas as pd
from exchange_calendars import get_calendar

from sharadar.util.logger import log
from sharadar.util.output_dir import get_data_dir
from six import (
    iteritems,
)
from zipline.data.adjustments import SQLiteAdjustmentWriter
from zipline.data.bar_reader import (
    NoDataBeforeDate,
)
from zipline.data.session_bars import SessionBarReader
from zipline.utils.numpy_utils import (
    float64_dtype,
    uint32_dtype,
    uint64_dtype,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS "properties" (
"key" TEXT,
  "0" TEXT
);
CREATE INDEX "ix_properties_key" ON "properties" ("key");

CREATE TABLE IF NOT EXISTS "prices" (
  "date" TIMESTAMP NOT NULL,
  "sid" INTEGER NOT NULL,
  "open" REAL NOT NULL,
  "high" REAL NOT NULL,
  "low" REAL NOT NULL,
  "close" REAL NOT NULL,
  "volume" REAL NOT NULL,
  PRIMARY KEY (date, sid)
);
CREATE INDEX "ix_prices_date" ON "prices" ("date");
CREATE INDEX "ix_prices_sid" ON "prices" ("sid");
"""

SCHEMA_ADJUST = """
CREATE TABLE IF NOT EXISTS "splits" (
"index" INTEGER,
  "effective_date" INTEGER,
  "ratio" REAL,
  "sid" INTEGER,
  PRIMARY KEY (effective_date, sid)
);
CREATE INDEX IF NOT EXISTS "ix_splits_index"ON "splits" ("index");

CREATE TABLE IF NOT EXISTS "mergers" (
"index" INTEGER,
  "effective_date" INTEGER,
  "ratio" REAL,
  "sid" INTEGER,
  PRIMARY KEY (effective_date, sid)
);
CREATE INDEX IF NOT EXISTS "ix_mergers_index" ON "mergers" ("index");

CREATE TABLE IF NOT EXISTS "dividend_payouts" (
"date" TIMESTAMP,
  "amount" REAL,
  "sid" INTEGER,
  "record_date" INTEGER,
  "declared_date" INTEGER,
  "pay_date" INTEGER,
  "ex_date" INTEGER,
  PRIMARY KEY (date, sid)
);
CREATE INDEX IF NOT EXISTS "ix_dividend_payouts_date"ON "dividend_payouts" ("date");

CREATE TABLE IF NOT EXISTS "stock_dividend_payouts" (
"index" INTEGER,
  "sid" INTEGER,
  "ex_date" INTEGER,
  "declared_date" INTEGER,
  "record_date" INTEGER,
  "pay_date" INTEGER,
  "payment_sid" INTEGER,
  "ratio" REAL,
  PRIMARY KEY (sid, ex_date)
);
CREATE INDEX IF NOT EXISTS "ix_stock_dividend_payouts_index"ON "stock_dividend_payouts" ("index");

CREATE TABLE IF NOT EXISTS "dividends" (
"index" INTEGER,
  "effective_date" INTEGER,
  "ratio" REAL,
  "sid" INTEGER,
  PRIMARY KEY (effective_date, sid)
);

CREATE INDEX IF NOT EXISTS "ix_dividends_index"ON "dividends" ("index");
CREATE INDEX IF NOT EXISTS splits_sids ON splits(sid);
CREATE INDEX IF NOT EXISTS splits_effective_date ON splits(effective_date);
CREATE INDEX IF NOT EXISTS mergers_sids ON mergers(sid);
CREATE INDEX IF NOT EXISTS mergers_effective_date ON mergers(effective_date);
CREATE INDEX IF NOT EXISTS dividends_sid ON dividends(sid);
CREATE INDEX IF NOT EXISTS dividends_effective_date ON dividends(effective_date);
CREATE INDEX IF NOT EXISTS dividend_payouts_sid ON dividend_payouts(sid);
CREATE INDEX IF NOT EXISTS dividends_payouts_ex_date ON dividend_payouts(ex_date);
CREATE INDEX IF NOT EXISTS stock_dividend_payouts_sid ON stock_dividend_payouts(sid);
CREATE INDEX IF NOT EXISTS stock_dividends_payouts_ex_date ON stock_dividend_payouts(ex_date);
"""
# Sqlite Maximum Number Of Columns in a table or query
SQLITE_MAX_COLUMN = 2000


class SQLiteDailyBarWriter(object):
    """Writes daily OHLCV bar data to a SQLite database.

    Creates the prices schema on first use and supports incremental writes
    with INSERT OR REPLACE semantics.

    Attributes:
        _filename: Path to the SQLite database file.
        _calendar: Trading calendar used for session alignment.
    """
    def __init__(self, filename, calendar):
        self._filename = filename
        self._calendar = calendar

        # Create schema, if not exists
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='prices'")
            if c.fetchone()[0] == 0:
                c.executescript(SCHEMA)

    def _validate(self, data):
        """Validate that input data has the expected format.

        Args:
            data: DataFrame to validate.

        Raises:
            ValueError: If data is not a DataFrame or lacks ['date', 'sid'] index.
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("data must be an instance of DataFrame.")
        if data.index.names != ['date', 'sid']:
            raise ValueError("data indexes must be ['date', 'sid'].")

    def write(self, data):
        """Write daily OHLCV bars, replacing existing rows with the same (date, sid).

        Args:
            data: DataFrame indexed by ['date', 'sid'] with open, high, low,
                close and volume columns.
        """
        self._validate(data)

        df = data[['open', 'high', 'low', 'close', 'volume']]
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            properties = pd.Series({'calendar_name': self._calendar.name})
            properties.to_sql('properties', con, index_label='key', if_exists="replace")

            insert_sql = "INSERT OR REPLACE INTO prices (date, sid, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)"
            batch_size = 100000
            total = len(df)
            dates = pd.DatetimeIndex(df.index.get_level_values('date'))
            sids = df.index.get_level_values('sid').to_numpy(dtype='int64')
            values = df.to_numpy(dtype='float64')

            with click.progressbar(length=total, label="Inserting price data...") as pbar:
                for start in range(0, total, batch_size):
                    end = min(start + batch_size, total)
                    date_strs = dates[start:end].strftime('%Y-%m-%d 00:00:00')
                    batch = list(zip(date_strs, sids[start:end].tolist(), *values[start:end].T.tolist()))
                    try:
                        c.executemany(insert_sql, batch)
                        con.commit()
                    except sqlite3.OperationalError as e:
                        log.error("SqlError %s: %s" % (e, str(batch[:1])))
                    pbar.update(end - start)


class SQLiteDailyBarReader(SessionBarReader):
    """
    Reader for pricing data written by SQLiteDailyBarWriter.


    See Also
    --------
    zipline.data.us_equity_pricing.BcolzDailyBarReader
    """
    def __init__(self, filename=os.path.join(get_data_dir(), "prices.sqlite")):
        self._filename = filename

    def _query(self, sql):
        """Execute a SQL query and return all results.

        Args:
            sql: SQL query string.

        Returns:
            List of result tuples.
        """
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            c.execute(sql)
            return c.fetchall()

    def _exist_sid(self, sid):
        """Check if a security ID exists in the prices table.

        Args:
            sid: Security identifier.

        Returns:
            bool: True if the sid has price data.
        """
        sql = "SELECT COUNT(DISTINCT(sid)) FROM prices WHERE sid = %d" % sid
        res = self._query(sql)
        return res[0][0] == 1

    def _fmt_date(self, dt):
        """Format a datetime to the string format used in the database.

        Args:
            dt: Datetime-like object.

        Returns:
            str: Date formatted as 'YYYY-MM-DD 00:00:00'.
        """
        return pd.to_datetime(dt).strftime('%Y-%m-%d') + " 00:00:00"

    # @cached
    def get_value(self, sid, dt, field):
        """Get a single field value for a sid on a specific date.

        Args:
            sid: Security identifier.
            dt: Date to query.
            field: Column name (e.g., 'close', 'volume').

        Returns:
            The scalar value for the requested field.

        Raises:
            NoDataBeforeDate: If no data exists on or before the date.
            KeyError: If the sid does not exist.
        """
        day = self._fmt_date(dt)
        sql = "SELECT %s FROM prices WHERE sid = %d and date = '%s'" % (field, sid, day)
        res = self._query(sql)
        if len(res) == 0:
            if self._exist_sid(sid):
                raise NoDataBeforeDate("No data on or before day={0} for sid={1}".format(dt, sid))
            else:
                raise KeyError(sid)
        return res[0][0]

    # @cached
    def load_dataframe(self, field, start_dt, end_dt, sids):
        """Load price data as a DataFrame with sessions as index.

        Args:
            field: Column name to load.
            start_dt: Start date (inclusive).
            end_dt: End date (inclusive).
            sids: List of security identifiers.

        Returns:
            pd.DataFrame: With trading sessions as index and sids as columns.
        """
        data = self.load_raw_arrays([field], start_dt, end_dt, sids)
        sessions = self.trading_calendar.sessions_in_range(start_dt, end_dt)
        df = pd.DataFrame(data[0], index=sessions)
        df.columns = sids
        return df

    # @cached
    def load_series(self, field, start_dt, end_dt, sid):
        """Load price data for a single sid as a Series.

        Args:
            field: Column name to load.
            start_dt: Start date (inclusive).
            end_dt: End date (inclusive).
            sid: Single security identifier.

        Returns:
            pd.Series: With trading sessions as index.
        """
        data = self.load_raw_arrays([field], start_dt, end_dt, [sid])
        sessions = self.trading_calendar.sessions_in_range(start_dt, end_dt)
        return pd.Series(data[0][:, 0], index=sessions)

    # @cached
    def load_raw_arrays(self, fields, start_dt, end_dt, sids):
        """Load raw numpy arrays for pipeline computation.

        Args:
            fields: List of column names to load.
            start_dt: Start date (inclusive).
            end_dt: End date (inclusive).
            sids: List of security identifiers.

        Returns:
            List of numpy arrays, one per field, each of shape
            (num_sessions, num_sids).
        """
        start_day = self._fmt_date(start_dt)
        end_day = self._fmt_date(end_dt)
        sessions = self.trading_calendar.sessions_in_range(start_dt, end_dt)
        log.debug("Loading raw arrays for %d assets (%s)." % (len(sids), type(sids)))

        if any(not isinstance(x, (int, np.integer)) for x in sids):
            sids = [x.sid for x in sids]

        raw_arrays = []
        with closing(sqlite3.connect(self._filename)) as conn:
            for field in fields:
                query = "SELECT date, sid, %s FROM prices WHERE sid in (%s) and date >= '%s' AND date <= '%s';" \
                        % (field, ",".join(map(str, sids)), str(start_day), str(end_day))
                df = pd.read_sql_query(query, conn)
                result = df.pivot(index='date', columns='sid', values=field)
                result = result.reindex(index=list(map(self._fmt_date, sessions)), columns=sids)
                raw_arrays.append(result.values)

        return raw_arrays

    def load_values_at(self, field, sids, dates):
        """Load one field for individual (sid, date) pairs.

        Args:
            field: Column name to load.
            sids: Security identifiers.
            dates: Dates, aligned with ``sids``.

        Returns:
            np.ndarray: float64 values aligned with the inputs (NaN if missing).
        """
        sids = np.asarray(sids, dtype='int64')
        if len(sids) == 0:
            return np.array([], dtype='float64')
        date_strs = pd.DatetimeIndex(dates).strftime('%Y-%m-%d 00:00:00')
        with closing(sqlite3.connect(self._filename)) as conn:
            conn.execute("CREATE TEMP TABLE wanted (ix INTEGER PRIMARY KEY, date TEXT, sid INTEGER)")
            conn.executemany("INSERT INTO wanted VALUES (?, ?, ?)",
                             zip(range(len(sids)), date_strs, sids.tolist()))
            rows = conn.execute(
                'SELECT w.ix, p."%s" FROM wanted w JOIN prices p ON p.date = w.date AND p.sid = w.sid' % field
            ).fetchall()
        result = np.full(len(sids), np.nan)
        if rows:
            ix, values = zip(*rows)
            result[list(ix)] = np.asarray(values, dtype='float64')
        return result
    def get_last_traded_dt(self, sid, dt):
        """Get the last traded datetime for a sid on or before dt.

        Args:
            sid: Security identifier.
            dt: Date to query.

        Returns:
            pd.Timestamp or pd.NaT if no data found.

        Raises:
            KeyError: If the sid does not exist.
        """
        day = self._fmt_date(dt)
        sql = "SELECT date FROM prices WHERE sid = %d and date = '%s'" % (sid, day)
        res = self._query(sql)
        if len(res) == 0:
            if self._exist_sid(sid):
                return pd.NaT
            else:
                raise KeyError(sid)

        return pd.Timestamp(res[0][0]).tz_localize(None)

    @property
    def last_available_dt(self):
        sql = "SELECT MAX(date) FROM prices"
        res = self._query(sql)
        if len(res) == 0:
            return pd.NaT
        return pd.Timestamp(res[0][0]).tz_localize(None)

    @property
    def trading_calendar(self):
        sql = 'SELECT "0" FROM properties WHERE key="calendar_name"'
        res = self._query(sql)
        if len(res) == 0:
            raise ValueError("No trading calendar defined.")
        return get_calendar(res[0][0], start=pd.Timestamp('2000-01-01 00:00:00'))

    @property
    def first_trading_day(self):
        trading_calendar_first_session = self.trading_calendar.first_session
        sql = "SELECT MIN(date) FROM prices"
        res = self._query(sql)
        if len(res) == 0:
            return pd.NaT
        first_trading_day = pd.Timestamp(res[0][0]).tz_localize(None)
        return max(first_trading_day, trading_calendar_first_session)

    @property
    def sessions(self):
        cal = self.trading_calendar
        return cal.sessions_in_range(self.first_trading_day, self.last_available_dt)


class SQLiteDailyAdjustmentWriter(SQLiteAdjustmentWriter):

    """Writes adjustment data (splits, dividends, mergers) to SQLite.

    Extends zipline's SQLiteAdjustmentWriter with custom schema creation
    and dividend ratio calculation logic.

    Attributes:
        _filename: Path to the adjustments SQLite database.
        _equity_daily_bar_reader: Reader for price data used in ratio calculation.
        _calendar: Trading calendar.
        _asset_finder: Asset finder for looking up asset metadata.
    """
    def __init__(self, adjustment_dbpath, equity_daily_bar_reader, asset_finder, calendar):
        self._filename = adjustment_dbpath
        self._equity_daily_bar_reader = equity_daily_bar_reader
        self._calendar = calendar
        self._asset_finder = asset_finder

        # Create schema, if not exists
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='dividends'")
            if c.fetchone()[0] == 0:
                c.executescript(SCHEMA_ADJUST)

    def _write(self, tablename, expected_dtypes, frame):
        if frame is None or frame.empty:
            # keeping the dtypes correct for empty frames is not easy
            frame = pd.DataFrame(
                np.array([], dtype=list(expected_dtypes.items())),
            )
        else:
            if frozenset(frame.columns) != frozenset(expected_dtypes):
                raise ValueError(
                    "Unexpected frame columns:\n"
                    "Expected Columns: %s\n"
                    "Received Columns: %s" % (
                        set(expected_dtypes),
                        frame.columns.tolist(),
                    )
                )

            actual_dtypes = frame.dtypes
            for colname, expected in iteritems(expected_dtypes):
                actual = actual_dtypes[colname]
                if actual != expected:
                    raise TypeError(
                        "Expected data of type {expected} for column"
                        " '{colname}', but got '{actual}'.".format(
                            expected=expected,
                            colname=colname,
                            actual=actual,
                        ),
                    )

        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            # Insert by column name: the frame column order (e.g. sid, effective_date, ratio
            # from calc_dividend_ratios) may differ from the table column order.
            table_cols = [r[1] for r in c.execute('PRAGMA table_info("%s")' % tablename)]
            col_names = ', '.join('"%s"' % col for col in [table_cols[0]] + list(frame.columns))
            params = ', '.join(['?'] * (len(frame.columns) + 1))
            sql = 'INSERT OR REPLACE INTO "%s" (%s) VALUES (%s)' % (tablename, col_names, params)
            with click.progressbar(length=len(frame), label="Inserting %s..." % tablename) as pbar:
                for row in frame.itertuples(index=True, name=None):
                    index = str(row[0]) if isinstance(row[0], pd.Timestamp) else row[0]
                    values = [v.item() if isinstance(v, np.generic) else v for v in (index,) + row[1:]]
                    try:
                        c.execute(sql, values)
                    except sqlite3.Error as e:
                        log.error("%s: %s %s" % (e, sql, values))
                    pbar.update(1)

    def write(self, splits=None, mergers=None, dividends=None, stock_dividends=None):
        """Write splits, mergers, and dividend data to the database.

        Args:
            splits: DataFrame of split records.
            mergers: DataFrame of merger records.
            dividends: DataFrame of dividend payout records.
            stock_dividends: DataFrame of stock dividend records.
        """
        self.write_frame('splits', splits)
        self.write_frame('mergers', mergers)
        self.write_dividend_data(dividends, stock_dividends)

    def calc_dividend_ratios(self, dividends):
        """
        Calculate the ratios to apply to equities when looking back at pricing
        history so that the price is smoothed over the ex_date, when the market
        adjusts to the change in equity value due to upcoming dividend.

        Returns
        -------
        DataFrame
            A frame in the same format as splits and mergers, with keys
            - sid, the id of the equity
            - effective_date, the date in seconds on which to apply the ratio.
            - ratio, the ratio to apply to backwards looking pricing data.
        """
        if dividends is None or dividends.empty:
            return pd.DataFrame(np.array(
                [],
                dtype=[
                    ('sid', uint64_dtype),
                    ('effective_date', uint32_dtype),
                    ('ratio', float64_dtype),
                ],
            ))

        pricing_reader = self._equity_daily_bar_reader
        input_sids = dividends.sid.values
        dates = pricing_reader.sessions.values

        date_ix = np.searchsorted(dates, dividends.ex_date.values)
        mask = date_ix > 0

        date_ix = date_ix[mask]
        input_dates = dividends.ex_date.values[mask]
        input_sids = input_sids[mask]

        # Raw (unadjusted) closes, like the dividend amounts. An adjusted history would
        # also include later splits/dividends and give wrong ratios for older ex dates.
        # subtract one day to get the close on the day prior to the ex date
        previous_dates = dates[date_ix - 1]
        if hasattr(pricing_reader, 'load_values_at'):
            # Only read the needed (sid, date) closes instead of the full price history.
            previous_close = pricing_reader.load_values_at('close', input_sids, previous_dates)
        else:
            unique_sids, sids_ix = np.unique(input_sids, return_inverse=True)
            start = pd.Timestamp(dates[0]).tz_localize(None)
            end = pd.Timestamp(dates[-1]).tz_localize(None)
            close = np.asarray(
                pricing_reader.load_raw_arrays(['close'], start, end, list(unique_sids))[0], dtype='float64'
            )
            previous_close = close[date_ix - 1, sids_ix]
        previous_close = np.asarray(previous_close, dtype='float64').copy()
        previous_close[previous_close <= 0] = np.nan

        amount = dividends.amount.values[mask]
        ratio = 1.0 - amount / previous_close

        non_nan_ratio_mask = ~np.isnan(ratio)
        for ix in np.flatnonzero(~non_nan_ratio_mask):
            ex_date = pd.Timestamp(input_dates[ix]).tz_localize(None)
            start_date = self._asset_finder.retrieve_asset(input_sids[ix]).start_date
            if ex_date != start_date:
                log.warn(
                    "Couldn't compute ratio for dividend"
                    " sid={sid}, ex_date={ex_date:%Y-%m-%d}, start_date={start_date:%Y-%m-%d}, amount={amount:.3f}",
                    sid=input_sids[ix],
                    ex_date=ex_date,
                    amount=amount[ix],
                    start_date=start_date
                )

        valid_ratio_mask = non_nan_ratio_mask & (ratio > 0)
        for ix in np.flatnonzero(non_nan_ratio_mask & ~valid_ratio_mask):
            log.warn(
                "Dividend ratio <= 0 for dividend"
                " sid={sid}, ex_date={ex_date:%Y-%m-%d}, amount={amount:.3f}",
                sid=input_sids[ix],
                ex_date=pd.Timestamp(input_dates[ix]).tz_localize(None),
                amount=amount[ix],
            )

        return pd.DataFrame({
            'sid': input_sids[valid_ratio_mask],
            'effective_date': input_dates[valid_ratio_mask],
            'ratio': ratio[valid_ratio_mask],
        })
