from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarWriter, SQLiteDailyBarReader

import quandl
from os import environ as env
import pandas as pd
from sharadar.loaders.ingest_sharadar import process_data_table
from zipline.utils.calendars import get_calendar
from sharadar.util.equity_supplementary_util import lookup_sid
from zipline.data.bar_reader import (
    NoDataBeforeDate,
)

def create_data_list(df, sharadar_metadata_df):
    data_list = []  # list to send to daily_bar_writer

    # iterate over all the securities and pack data and metadata for writing
    tickers = df['ticker'].unique()

    for ticker in tickers:
        df_ticker = df[df['ticker'] == ticker]
        df_ticker = df_ticker.sort_index()

        sharadar_metadata = sharadar_metadata_df.loc[ticker]

        sid = sharadar_metadata.loc['permaticker']


        # drop metadata columns
        df_ticker = df_ticker.drop(['ticker', 'dividends'], axis=1)

        # pack data to be written by daily_bar_writer
        data_list.append((sid, df_ticker))
    return data_list


quandl.ApiConfig.api_key=env["QUANDL_API_KEY"]

sharadar_metadata_df = quandl.get_table('SHARADAR/TICKERS', table='SEP', paginate=True)
sharadar_metadata_df.set_index('ticker', inplace=True)

related_tickers = sharadar_metadata_df['relatedtickers'].dropna()
# Add a space at the begin and end of relatedtickers, search for ' TICKER '
related_tickers = ' ' + related_tickers.astype(str) + ' '

def dt(s):
    return pd.to_datetime(s)

start = dt('2019-04-16')
end = dt('2019-04-22')

# dataframe with sid insted of ticker
#data = quandl.get_table('SHARADAR/SEP', date={'gte':start,'lte':end}, ticker=['AAPL', 'IBM', 'PINS'], paginate=True)
data = quandl.get_table('SHARADAR/SEP', date={'gte':start,'lte':end}, paginate=True)
#data['sid'] = data['ticker'].apply(lambda x: sharadar_metadata_df.loc[x]['permaticker'])
data['sid'] = data['ticker'].apply(lambda x: lookup_sid(sharadar_metadata_df, related_tickers, x))
data = process_data_table(data)
data = data.drop(['ticker'], axis=1)

data.set_index(['date', 'sid'], inplace=True)

print(data)

#data_list = create_data_list(data, sharadar_metadata_df)
calendar = get_calendar("NYSE")
#path = '/tmp/test.bcolz'
#writer = BcolzDailyBarWriter(path,calendar,start,end)
#ctable = writer.write(data_list)
#print(ctable.attrs)
#equity_daily_reader = BcolzDailyBarReader(path)

dbpath = '/tmp/prices_new.db'
sql_writer = SQLiteDailyBarWriter(dbpath, calendar)
sql_writer.write(data)

sql_reader = SQLiteDailyBarReader(dbpath)

print(sql_reader.last_available_dt)
print(sql_reader.trading_calendar)
print(sql_reader.first_trading_day)
print(sql_reader.sessions)

print(sql_reader.get_last_traded_dt(199059, end))

print(sql_reader.get_value(110959, end, 'close'))
print(sql_reader.get_value(199623, end, 'close'))
try:
    sql_reader.get_value(110959, start, 'close')
except NoDataBeforeDate as e:
    print(e)

print(sql_reader.load_raw_arrays(['open','close'], start, end, [199623,110959]))
print(sql_reader.load_raw_arrays(['close'], start, end, [199623,110959,199059]))