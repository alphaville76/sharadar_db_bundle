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
    related_index = related[related.str.contains(' ' + ticker + ' ')].index
    related_metadata = sharadar_metadata_df.loc[related_index]
    # only in 'Domestic', 'Domestic Primary'
    result = related_metadata[related_metadata['category'].isin(['Domestic', 'Domestic Primary'])]['permaticker']
    return int(result[0]) if len(result) > 0 else -1

def lookup_sid(sharadar_metadata_df, related, ticker):
    try:
      return int(sharadar_metadata_df.loc[ticker]['permaticker'])
    except KeyError:
      return lookup_related_tickers(sharadar_metadata_df, related, ticker)

def insert_fundamentals(sharadar_metadata_df, sf1_df, cursor, show_progress=True):
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
                    if column in ['dimension', 'ev', 'evebit', 'evebitda', 'marketcap', 'pb', 'pe', 'ps']:
                        continue
                    value = row[column]
                    if type(value) == float and np.isnan(value):
                        continue
                    field = column + '_' + row['dimension'].lower()
                    date = datekey + pd.Timedelta(days=1)
                    
                    # end_date not used (set -1)
                    sql = "INSERT OR REPLACE INTO equity_supplementary_mappings (sid, field, start_date, end_date, value) VALUES(?, ?, ?, -1, ?)"
                    cursor.execute(sql, (sid, field, date.value, str(value)))

def insert_daily_metrics(sharadar_metadata_df, daily_df, cursor, show_progress=True):
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

