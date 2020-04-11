import pandas as pd
import numpy as np
import sqlite3
import click
from contextlib import closing
from zipline.utils.calendars import get_calendar
from zipline.data.session_bars import SessionBarReader
from sharadar.util.logger import log

from zipline.data.bar_reader import (
    NoDataBeforeDate,
)

from singleton_decorator import singleton
from memoization import cached

SCHEMA = """
CREATE TABLE IF NOT EXISTS "properties" (
"key" TEXT,
  "0" TEXT
);
CREATE INDEX "ix_properties_key" ON "properties" ("key");

CREATE TABLE IF NOT EXISTS "prices" (
  "date" TIMESTAMP,
  "sid" INTEGER,
  "open" REAL,
  "high" REAL,
  "low" REAL,
  "close" REAL,
  "volume" REAL
);
CREATE INDEX "ix_prices_date_sid" ON "prices" ("date","sid");
"""
# Sqlite Maximum Number Of Columns in a table or query
SQLITE_MAX_COLUMN = 2000

@singleton
class SQLiteDailyBarWriter(object):
    """
    Class capable of writing daily OHLCV data to disk in a format that can
    be read efficiently by BcolzDailyOHLCVReader.

    Parameters
    ----------
    filename : str
        The location at which we should write our output.
    calendar : zipline.utils.calendar.trading_calendar
        Calendar to use to compute asset calendar offsets.
    start_session: pd.Timestamp
        Midnight UTC session label.
    end_session: pd.Timestamp
        Midnight UTC session label.

    See Also
    --------
    zipline.data.us_equity_pricing.BcolzDailyBarReader
    """


    def __init__(self, filename, calendar, start_session, end_session):
        self._filename = filename

        if start_session != end_session:
            if not calendar.is_session(start_session):
                raise ValueError(
                    "Start session %s is invalid!" % start_session
                )
            if not calendar.is_session(end_session):
                raise ValueError(
                    "End session %s is invalid!" % end_session
                )

        self._start_session = start_session
        self._end_session = end_session

        self._calendar = calendar

        # Create schema, if not exists
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='prices'")
            if c.fetchone()[0] == 0:
                c.executescript(SCHEMA)


    def _validate(self, data):
        if not isinstance(data, pd.DataFrame):
            raise ValueError("data must be an instance of DataFrame.")
        if data.index.names != ['date', 'sid']:
            raise ValueError("data indexes must be ['date', 'sid'].")

    def write(self, data, with_progress=True, drop_table=False):
        self._validate(data)

        df = data[['open', 'high', 'low', 'close', 'volume']]
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            properties = pd.Series({'calendar_name' : self._calendar.name})
            properties.to_sql('properties', con, index_label='key', if_exists="replace")

            with click.progressbar(length=len(df), label="Inserting price data...") as pbar:     
                count = 0
                for index, row in df.iterrows():
                    sql = "INSERT OR REPLACE INTO prices (date, sid, open, high, low, close, volume) VALUES ('%s',%f,%f,%f,%f,%f,%f)"
                    values = index + tuple(row.values)
                    try:
                        c.execute(sql % values)
                    except sqlite3.OperationalError as e:
                        log.error(str(e) + "; values: " + str(values))
                    count += 1
                    pbar.update(count)

@singleton
class SQLiteDailyBarReader(SessionBarReader):
    """
    Reader for pricing data written by SQLiteDailyBarWriter.


    See Also
    --------
    zipline.data.us_equity_pricing.BcolzDailyBarReader
    """
    def __init__(self, filename):
        self._filename = filename

    def _query(self, sql):
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            c.execute(sql)
            return c.fetchall()
        
    def _exist_sids(self, sids):
        sql = "SELECT COUNT(DISTINCT(sid)) FROM prices WHERE sid IN (%s)" % ",".join(map(str, sids))
        res = self._query(sql)
        return res[0][0] == len(sids)

    def _fmt_date(self, dt):
        return pd.to_datetime(dt).strftime('%Y-%m-%d') + " 00:00:00"
    
    @cached
    def get_value(self, sid, dt, field):
        day = self._fmt_date(dt)
        sql = "SELECT %s FROM prices WHERE sid = %d and date = '%s'" % (field, sid, day)
        res = self._query(sql)
        if len(res) == 0:
            if self._exist_sids([sid]):
                raise NoDataBeforeDate("No data on or before day={0} for sid={1}".format(dt, sid))
            else:
                raise KeyError(sid)
        return res[0][0]

    def _create_pivot_query(self, field, start_dt, end_dt, sids):
        select = "SELECT "
        select_case = "MAX(CASE WHEN sid = %d THEN " + field +" END) '%d',"
        for sid in sids:
            select += (select_case % (sid, sid))

        return select[:-1] + " FROM prices WHERE date >= '" + str(start_dt) + "' and date <= '" + str(end_dt) + "' GROUP BY date"


    def _chunker(self, seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    @cached
    def load_raw_arrays(self, fields, start_dt, end_dt, sids):
        start_day = self._fmt_date(start_dt)
        end_day = self._fmt_date(end_dt)

        log.info("Loading raw arrays for %d assets." % (len(sids)))

        raw_arrays = []
        for field in fields:
            result_chunks = []
            for sids_chunk in self._chunker(sids, SQLITE_MAX_COLUMN):
                sid_from = 1 + len(result_chunks)*SQLITE_MAX_COLUMN
                sid_to = min(sid_from + SQLITE_MAX_COLUMN-1, len(sids))
                log.info("Loading raw array for field '%s' from the assets no. %d to the assets no. %d." \
                         % (field, sid_from, sid_to))
                pivot_query = self._create_pivot_query(field, start_day, end_day, sids_chunk)
                result_chunks.append(self._query(pivot_query))
            #result = np.hstack(result_chunks)
            result = np.concatenate(result_chunks, axis=1)
            raw_arrays.append(np.array(result,  dtype=float))
        return raw_arrays

    def get_last_traded_dt(self, sid, dt):
        day = self._fmt_date(dt)
        sql = "SELECT date FROM prices WHERE sid = %d and date = '%s'" % (sid, day)
        res = self._query(sql)
        if len(res) == 0:
            if self._exist_sids([sid]):
                return pd.NaT
            else:
                raise KeyError(sid)

        return pd.Timestamp(res[0][0], tz='UTC')

    @property
    def last_available_dt(self):
        sql = "SELECT MAX(date) FROM prices"
        res = self._query(sql)
        if len(res) == 0:
            return pd.NaT
        return pd.Timestamp(res[0][0], tz='UTC')

    @property
    def trading_calendar(self):
        sql = 'SELECT "0" FROM properties WHERE key="calendar_name"'
        res = self._query(sql)
        if len(res) == 0:
            raise ValueError("No trading calendar defined.")
        return get_calendar(res[0][0])

    @property
    def first_trading_day(self):
        sql = "SELECT MIN(date) FROM prices"
        res = self._query(sql)
        if len(res) == 0:
            return pd.NaT
        return pd.Timestamp(res[0][0], tz='UTC')

    @property
    def sessions(self):
        cal = self.trading_calendar
        return cal.sessions_in_range(self.first_trading_day, self.last_available_dt)
