"""Equity supplementary data utilities for the Sharadar bundle.

Provides functions to insert and query supplementary equity data
(company info, fundamentals, daily metrics) in the asset database's
equity_supplementary_mappings table.
"""
import pandas as pd
import numpy as np
from zipline.utils.cli import maybe_show_progress


UNIQUE_INDEX_NAME = 'ux_equity_supp_sid_field_start'


def ensure_unique_key(cursor):
    """Guarantee uniqueness of (sid, field, start_date) in equity_supplementary_mappings.

    The INSERT OR REPLACE statements below rely on that key. If the table was
    rebuilt without its PRIMARY KEY (e.g. via "CREATE TABLE ... AS SELECT"),
    duplicate rows accumulate. Removes existing duplicates (keeping the most
    recently inserted row) and creates a UNIQUE index.

    Returns:
        int: Number of duplicate rows removed.
    """
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='equity_supplementary_mappings'")
    table = cursor.fetchone()
    if table is None:
        return 0
    if 'PRIMARY KEY' in (table[0] or '').upper():
        return 0
    cursor.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (UNIQUE_INDEX_NAME,))
    if cursor.fetchone() is not None:
        return 0

    cursor.execute(
        "DELETE FROM equity_supplementary_mappings WHERE rowid NOT IN "
        "(SELECT MAX(rowid) FROM equity_supplementary_mappings GROUP BY sid, field, start_date)"
    )
    removed = cursor.rowcount
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS %s ON equity_supplementary_mappings (sid, field, start_date)"
        % UNIQUE_INDEX_NAME
    )
    return removed


def value_changed(cursor, sid, field, value):
    """
    Returns True, if the entry existed and its value changed
    """
    sql = "SELECT value from equity_supplementary_mappings WHERE sid = ? AND field = ? ORDER BY start_date DESC LIMIT 1"
    cursor.execute(sql, (sid, field))
    record = cursor.fetchone()
    if record is None:
        # if the entry doesn't exist, return False, otherwise it's used Timestamp.now
        return False
    return record[0] != value


def insert_asset_info(sharadar_metadata_df, cursor):
    """
    Basic extra data like company name, category (ARD, Domestic), industry sector, etc...
    These are the information from the table SHARADAR/TICKERS
    """

    exclude_fields = ['table', 'permaticker', 'ticker', 'firstpricedate', 'lastpricedate']
    for index, row in sharadar_metadata_df.iterrows():
        for field in row.index:
            if field not in exclude_fields:
                sid = row['permaticker']
                value = row[field]
                if value is None:
                    continue
                date = row['firstpricedate']

                start_date = date.value if not value_changed(cursor, sid, field, value) else pd.Timestamp("now").value

                # end_date not used (set -1)
                sql = "INSERT OR REPLACE INTO equity_supplementary_mappings (sid, field, start_date, end_date, value) VALUES(?, ?, ?, -1, ?)"
                cursor.execute(sql, (sid, field, start_date, str(value)))


def lookup_related_tickers(sharadar_metadata_df, related, ticker):
    """Look up a SID by searching related ticker mappings.

    Used as a fallback when a ticker is not found directly in metadata.

    Args:
        sharadar_metadata_df: Sharadar ticker metadata DataFrame.
        related: Series of related ticker strings (space-delimited).
        ticker: Ticker symbol to search for.

    Returns:
        int: The permaticker (SID) if found, -1 otherwise.
    """
    related_index = related[related.str.contains(' ' + str(ticker) + ' ')].index
    related_metadata = sharadar_metadata_df.loc[related_index]
    # only in 'Domestic', 'Domestic Primary'
    result = related_metadata[related_metadata['category'].isin(['Domestic', 'Domestic Primary'])]['permaticker']
    return int(result.iloc[0]) if len(result) > 0 else -1


def lookup_sid(sharadar_metadata_df, related, ticker):
    """Map a ticker symbol to its permanent security identifier (SID).

    First attempts direct lookup, then falls back to related ticker search.

    Args:
        sharadar_metadata_df: Sharadar ticker metadata DataFrame.
        related: Series of related ticker strings.
        ticker: Ticker symbol to resolve.

    Returns:
        int: The permaticker (SID), or -1 if not found.
    """
    try:
        permaticker = sharadar_metadata_df.loc[ticker, 'permaticker']
    except KeyError:
        return lookup_related_tickers(sharadar_metadata_df, related, ticker)
    if isinstance(permaticker, pd.Series):
        # Duplicate ticker rows in the metadata: use the first one.
        permaticker = permaticker.iloc[0]
    return int(permaticker)


def map_tickers_to_sids(sharadar_metadata_df, related, tickers):
    """Resolve each ticker once and map it to its SID.

    The related-tickers fallback scans the whole metadata, so it must not be
    repeated for every row of large tables such as SEP or DAILY.

    Args:
        sharadar_metadata_df: Sharadar ticker metadata DataFrame.
        related: Series of related ticker strings.
        tickers: Series of ticker symbols.

    Returns:
        pd.Series: int64 SIDs aligned with ``tickers`` (-1 if not found).
    """
    sid_by_ticker = {ticker: lookup_sid(sharadar_metadata_df, related, ticker) for ticker in pd.unique(tickers)}
    return tickers.map(sid_by_ticker).astype('int64')


INSERT_MAPPING_SQL = ("INSERT OR REPLACE INTO equity_supplementary_mappings "
                      "(sid, field, start_date, end_date, value) VALUES(?, ?, ?, -1, ?)")
INSERT_CHUNK_ROWS = 50000


def _related_tickers(sharadar_metadata_df):
    related_tickers = sharadar_metadata_df['relatedtickers'].dropna()
    # Add a space at the begin and end of relatedtickers, search for ' TICKER '
    return ' ' + related_tickers.astype(str) + ' '


def _value_strings(values):
    """Format values exactly like ``str(value)`` did in the former row-by-row insert."""
    if pd.api.types.is_float_dtype(values.dtype):
        return [str(v) for v in values.to_numpy(dtype=float).tolist()]
    return [str(v) for v in values.tolist()]


def _insert_mappings(df, date_column, columns, field_suffix, cursor, show_progress, label):
    """Insert all non-null ``columns`` of ``df`` into equity_supplementary_mappings.

    ``df`` must contain 'sid' and ``date_column`` (start_date). ``field_suffix``
    is None or a Series of suffixes appended as ``<column>_<suffix>``.
    """
    all_sids = df['sid'].to_numpy()
    all_start_dates = df[date_column].to_numpy()
    valid_by_column = {}
    for column in columns:
        values = df[column]
        valid = values.notna().to_numpy()
        if values.dtype == object:
            valid &= (values != 'None').to_numpy()
        valid_by_column[column] = valid

    # Rows of the same field keep their order, so INSERT OR REPLACE resolves duplicates as before.
    chunk_starts = range(0, len(df), INSERT_CHUNK_ROWS)
    with maybe_show_progress(chunk_starts, show_progress, label=label) as it:
        for start in it:
            end = start + INSERT_CHUNK_ROWS
            for column in columns:
                ix = start + np.flatnonzero(valid_by_column[column][start:end])
                if len(ix) == 0:
                    continue
                if field_suffix is None:
                    fields = [column] * len(ix)
                else:
                    fields = (column + '_' + field_suffix.iloc[ix]).tolist()
                cursor.executemany(INSERT_MAPPING_SQL, zip(all_sids[ix].tolist(), fields,
                                                           all_start_dates[ix].tolist(),
                                                           _value_strings(df[column].iloc[ix])))


def _to_ns(dates):
    return pd.to_datetime(dates).astype('datetime64[ns]').astype('int64')


def _prepare_by_ticker(sharadar_metadata_df, df, date_column, extra_drop):
    """Add sids and order rows like the former per-ticker, newest-first insert.

    Rows of tickers without a known sid are dropped.
    """
    df = df.drop(columns=[c for c in extra_drop if c in df.columns])
    df = df.assign(
        sid=map_tickers_to_sids(sharadar_metadata_df, _related_tickers(sharadar_metadata_df), df['ticker']),
        _ticker_order=pd.factorize(df['ticker'])[0],
    )
    df = df[df['sid'] != -1]
    df = df.sort_values(['_ticker_order', date_column], ascending=[True, False], kind='stable')
    return df.drop(columns=['_ticker_order', 'ticker'])


FUNDAMENTALS_EXCLUDED = ['fiscalperiod', 'siccode', 'dimension', 'ev', 'evebit', 'evebitda', 'marketcap', 'pb',
                         'pe', 'ps']


def insert_fundamentals(sharadar_metadata_df, sf1_df, cursor, show_progress=True):
    """Insert quarterly fundamental data into supplementary mappings.

    Processes SF1 data and writes each field/quarter combination as a
    separate row in equity_supplementary_mappings.

    Args:
        sharadar_metadata_df: Sharadar ticker metadata DataFrame.
        sf1_df: SF1 fundamentals DataFrame from NASDAQ Data Link.
        cursor: SQLite cursor for writing.
        show_progress: Whether to show a progress bar. Defaults to True.
    """
    if sf1_df.empty:
        return
    df = _prepare_by_ticker(sharadar_metadata_df, sf1_df, 'datekey', ['lastupdated', 'calendardate'])
    df['_start_date'] = _to_ns(pd.to_datetime(df['datekey']) + pd.Timedelta(days=1))
    suffix = df['dimension'].str.lower()
    columns = [c for c in df.columns
               if c not in FUNDAMENTALS_EXCLUDED and c not in ('sid', 'datekey', '_start_date')]
    _insert_mappings(df, '_start_date', columns, suffix, cursor, show_progress, 'Inserting fundamental data: ')


def insert_daily_metrics(sharadar_metadata_df, daily_df, cursor, show_progress=True):
    """Insert daily metric data into supplementary mappings.

    Processes SHARADAR/DAILY data and writes market cap, P/E, and other
    daily metrics for each ticker/date combination.

    Args:
        sharadar_metadata_df: Sharadar ticker metadata DataFrame.
        daily_df: Daily metrics DataFrame from NASDAQ Data Link.
        cursor: SQLite cursor for writing.
        show_progress: Whether to show a progress bar. Defaults to True.
    """
    if daily_df.empty:
        return
    df = _prepare_by_ticker(sharadar_metadata_df, daily_df, 'date', ['lastupdated'])
    df['_start_date'] = _to_ns(df['date'])
    columns = [c for c in df.columns if c not in ('sid', 'date', '_start_date')]
    _insert_mappings(df, '_start_date', columns, None, cursor, show_progress, 'Inserting daily metrics: ')
