"""Equity supplementary data utilities for the Sharadar bundle.

Provides functions to insert and query supplementary equity data
(company info, fundamentals, daily metrics) in the asset database's
equity_supplementary_mappings table.
"""
import pandas as pd
import numpy as np
from zipline.utils.cli import maybe_show_progress


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
        return int(sharadar_metadata_df.loc[ticker]['permaticker'])
    except KeyError:
        return lookup_related_tickers(sharadar_metadata_df, related, ticker)


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
    tickers = sf1_df['ticker'].unique()
    related_tickers = sharadar_metadata_df['relatedtickers'].dropna()
    # Add a space at the begin and end of relatedtickers, search for ' TICKER '
    related_tickers = ' ' + related_tickers.astype(str) + ' '

    with maybe_show_progress(tickers, show_progress, label='Parsing fundamental data: ') as it:
        for ticker in it:
            df_ticker = sf1_df[sf1_df['ticker'] == ticker]
            df_ticker.set_index('datekey', inplace=True)
            df_ticker = df_ticker.sort_index(ascending=False)
            df_ticker = df_ticker.drop(['ticker', 'lastupdated', 'calendardate'], axis=1)

            sid = lookup_sid(sharadar_metadata_df, related_tickers, ticker)

            for datekey, row in df_ticker.iterrows():
                for column in row.index:
                    if column in ['fiscalperiod', 'siccode', 'dimension', 'ev', 'evebit', 'evebitda', 'marketcap', 'pb', 'pe', 'ps']:
                        continue
                    value = row[column]
                    if type(value) == float and np.isnan(value):
                        continue
                    if value is None or value == 'None':
                        continue
                    field = column + '_' + row['dimension'].lower()
                    date = datekey + pd.Timedelta(days=1)

                    # end_date not used (set -1)
                    sql = "INSERT OR REPLACE INTO equity_supplementary_mappings (sid, field, start_date, end_date, value) VALUES(?, ?, ?, -1, ?)"
                    cursor.execute(sql, (sid, field, date.value, str(value)))


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
    tickers = daily_df['ticker'].unique()
    related_tickers = sharadar_metadata_df['relatedtickers'].dropna()
    # Add a space at the begin and end of relatedtickers, search for ' TICKER '
    related_tickers = ' ' + related_tickers.astype(str) + ' '

    with maybe_show_progress(tickers, show_progress, label='Parsing fundamental data: ') as it:
        for ticker in it:
            df_ticker = daily_df[daily_df['ticker'] == ticker]
            df_ticker.set_index('date', inplace=True)
            df_ticker = df_ticker.sort_index(ascending=False)
            df_ticker = df_ticker.drop(['ticker', 'lastupdated'], axis=1)

            sid = lookup_sid(sharadar_metadata_df, related_tickers, ticker)

            for date, row in df_ticker.iterrows():
                for field in row.index:
                    value = row[field]
                    if np.isnan(value):
                        continue

                    # end_date not used (set -1)
                    sql = "INSERT OR REPLACE INTO equity_supplementary_mappings (sid, field, start_date, end_date, value) VALUES(?, ?, ?, -1, ?)"
                    cursor.execute(sql, (sid, field, date.value, str(value)))
