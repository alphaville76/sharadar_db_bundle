import nasdaqdatalink
import os
from os import environ as env
from pandas.tseries.offsets import DateOffset
import pandas as pd
import requests
from sharadar.util.output_dir import get_output_dir
from sharadar.loaders.constant import METADATA_HEADERS
from exchange_calendars import get_calendar
from sharadar.util.nasdaqdatalink_util import last_available_date
import pandas_datareader.data as pdr
import sharadar.loaders.constant as k
import random

# To clean the database if necessary use:
# delete from prices where sid in (10003,   10006,  10012,  10024,  10036,  10060,  10084,  10120,  10240, 10400, 10410, 10420, 10430, 10440, 10450)


nasdaqdatalink.ApiConfig.api_key = env["NASDAQ_API_KEY"]


def _add_macro_def(df, sid, start_date, end_date, ticker, asset_name):
    # The date on which to close any positions in this asset.
    auto_close_date = end_date + pd.Timedelta(days=1)

    # The canonical name of the exchange
    exchange = 'MACRO'

    # Remove timezone, otherwise "TypeError: data type not understood"
    df.loc[sid] = (ticker, asset_name,
                   start_date,
                   end_date,
                   start_date,
                   auto_close_date,
                   exchange)

def _to_prices_df(df, sid):
    df.index = df.index.tz_localize('UTC')
    df['sid'] = sid
    df.set_index('sid', append=True, inplace=True)
    df = _append_ohlc(df)
    return df


def _append_ohlc(df):
    df.index.names = ['date', 'sid']
    df.columns = ['open']
    df['high'] = df['low'] = df['close'] = df['open']
    df['volume'] = 100.0
    return df


def utc(s):
    return pd.to_datetime(s, utc=True)

def create_macro_equities_df():
    # TR1M, TR2M and TR30Y excluded because of too many missing data
    end_date = utc(last_available_date())
    df = pd.DataFrame(columns=METADATA_HEADERS)
    #_add_macro_def(df, 10001, utc('1997-12-31'), end_date, 'TR1M', 'US Treasury Bill 1 MO')
    #_add_macro_def(df, 10002, utc('1997-12-31'), end_date, 'TR2M', 'US Treasury Bill 2 MO')
    _add_macro_def(df, 10003, utc('1990-01-02'), end_date, 'TR3M', 'US Treasury Bill 3 MO')
    _add_macro_def(df, 10006, utc('1990-01-02'), end_date, 'TR6M', 'US Treasury Bill 6 MO')
    _add_macro_def(df, 10012, utc('1990-01-02'), end_date, 'TR1Y', 'US Treasury Bond 1 YR')
    _add_macro_def(df, 10024, utc('1990-01-02'), end_date, 'TR2Y', 'US Treasury Bond 2 YR')
    _add_macro_def(df, 10036, utc('1990-01-02'), end_date, 'TR3Y', 'US Treasury Bond 3 YR')
    _add_macro_def(df, 10060, utc('1990-01-02'), end_date, 'TR5Y', 'US Treasury Bond 5 YR')
    _add_macro_def(df, 10084, utc('1990-01-02'), end_date, 'TR7Y', 'US Treasury Bond 7 YR')
    _add_macro_def(df, 10120, utc('1990-01-02'), end_date, 'TR10Y', 'US Treasury Bond 10 YR')
    _add_macro_def(df, 10240, utc('1990-01-02'), end_date, 'TR20Y', 'US Treasury Bond 20 YR')
    #_add_macro_def(df, 10360, utc('1990-01-02'), end_date, 'TR30Y', 'US Treasury Bond 30 YR')
    _add_macro_def(df, 10400, utc('1996-12-31'), end_date, 'CBOND', 'US Corporate Bond Yield')
    _add_macro_def(df, 10410, utc('1990-01-02'), end_date, 'INDPRO', 'Industrial Production Index')
    _add_macro_def(df, 10420, utc('1990-01-02'), end_date, 'INDPROPCT', 'Industrial Production Montly % Change')
    _add_macro_def(df, 10430, utc('1990-01-02'), end_date, 'PMICMP', 'Purchasing Managers Index')
    _add_macro_def(df, 10440, utc('1990-01-02'), end_date, 'UNRATE', 'Civilian Unemployment Rate')
    _add_macro_def(df, 10450, utc('1990-01-02'), end_date, 'RATEINF', 'US Inflation Rates YoY')
    return df

def create_macro_prices_df(start_str: str, calendar=get_calendar('XNYS')):
    start = pd.to_datetime(start_str)
    end = pd.to_datetime(last_available_date())

    if start is not None and start > end:
        start = end

    #TODO optimize only CPIAUCNS need 13, the rest 6, maybe even 3
    m_start = start - DateOffset(months=13)

    # Interest Rates (T-bills and T-bonds)
    # Frequency: daily
    tres_df = pdr.DataReader(['DTB3', 'DTB6', 'DGS1', 'DGS1', 'DGS2', 'DGS3', 'DGS5', 'DGS7', 'DGS10'], 'fred', start, end)

    # sids
    tres_df.columns =        [10003,   10006,  10012,  10024,  10036,  10060,  10084,  10120,  10240]

    tres_df.index = tres_df.index.tz_localize('UTC')
    sessions = calendar.sessions_in_range(start, end)
    tres_df = tres_df.reindex(sessions).fillna(method='ffill').dropna()

    prices = tres_df.unstack().to_frame()
    prices = prices.swaplevel()
    prices = _append_ohlc(prices)

    # USEY-US-Corporate-Bond-Index-Yield (BAMLC0A0CMEY)
    # Frequency: daily
    corp_bond_df = _to_prices_df(pdr.DataReader(['BAMLC0A0CMEY'], 'fred', start, end).fillna(method="ffill"), 10400)
    prices = pd.concat([prices, corp_bond_df])


    # Industrial Production Change
    # Frequency: monthly
    indpro_df = pdr.DataReader(['INDPRO'], 'fred', m_start, end)\
        .reindex(pd.date_range(start=m_start, end=end), method='ffill').loc[pd.date_range(start, end)]
    prices = pd.concat([prices, _to_prices_df(indpro_df, 10410)])

    # rdiff: row-on-row % change
    # Frequency: monthly
    indpro_p_df = pdr.DataReader(['INDPRO'], 'fred', m_start, end).pct_change()\
        .reindex(pd.date_range(start=m_start, end=end), method='ffill').loc[pd.date_range(start, end)]
    prices = pd.concat([prices, _to_prices_df(indpro_p_df, 10420)])

    # Civilian Unemployment Rate
    # https://data.nasdaq.com/data/FRED/UNRATE-Civilian-Unemployment-Rate
    # Frequency: monthly
    unrate_df = pdr.DataReader(['UNRATE'], 'fred', m_start, end)\
        .reindex(pd.date_range(start=m_start, end=end), method='ffill').loc[pd.date_range(start, end)]
    prices = pd.concat([prices, _to_prices_df(unrate_df, 10440)])

    # https://data.nasdaq.com/data/RATEINF/INFLATION_USA-Inflation-YOY-USA
    # Frequency: monthly
    inf_df = (pdr.DataReader(['CPIAUCNS'], 'fred', m_start, end).pct_change(periods=12) * 100.00).round(2)\
        .reindex(pd.date_range(start=m_start, end=end), method='ffill').loc[pd.date_range(start, end)]
    prices = pd.concat([prices, _to_prices_df(inf_df, 10450)])

    # ISM Purchasing Managers Index
    # https://data.nasdaq.com/data/ISM/MAN_PMI-PMI-Composite-Index
    # https://www.economy.com/united-states/ism-purchasing-managers-index ?
    # Frequency: monthly
    pmi_df = retrieve_ism_pmi()\
        .reindex(pd.date_range(start=m_start, end=end), method='ffill').loc[pd.date_range(start, end)]
    prices = pd.concat([prices, _to_prices_df(pmi_df, 10430)])

    return prices.sort_index()


# Retrive the entire set from investing.com
# U.S. ISM Manufacturing Purchasing Managers Index (PMI)
# https://www.investing.com/economic-calendar/ism-manufacturing-pmi-173
def retrieve_ism_pmi():
    head = {
        "User-Agent": random.choice(k.USER_AGENTS),
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    # U.S. ISM Manufacturing Purchasing Managers Index (PMI)
    # https://www.investing.com/economic-calendar/ism-manufacturing-pmi-173
    url = 'https://sbcharts.investing.com/events_charts/us/173.json'
    response = requests.get(url, headers=head)
    if response.status_code != 200:
        raise ConnectionError(
            f"ERR#0015: error {response.status_code}, try again later."
        )

    data = response.json()["data"]
    df = pd.DataFrame(data, columns=['timestamp', 'value', 'forecast'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df.drop(columns=['forecast'], inplace=True)

    return df

def ingest(start):
    from sharadar.pipeline.engine import load_sharadar_bundle
    from zipline.assets import ASSET_DB_VERSION
    from sharadar.data.sql_lite_assets import SQLiteAssetDBWriter
    from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarWriter
    from exchange_calendars import get_calendar
    from sharadar.loaders.constant import EXCHANGE_DF


    calendar = get_calendar('XNYS')
    macro_equities_df = create_macro_equities_df()
    macro_prices_df = create_macro_prices_df(start)
    output_dir = get_output_dir()
    asset_dbpath = os.path.join(output_dir, ("assets-%d.sqlite" % ASSET_DB_VERSION))
    asset_db_writer = SQLiteAssetDBWriter(asset_dbpath)
    asset_db_writer.write(equities=macro_equities_df, exchanges=EXCHANGE_DF)
    prices_dbpath = os.path.join(output_dir, "prices.sqlite")
    sql_daily_bar_writer = SQLiteDailyBarWriter(prices_dbpath, calendar)
    sql_daily_bar_writer.write(macro_prices_df)
    return macro_prices_df.shape[0]

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 1:
        print("Usage: ingest_macro [start_date]")
        sys.exit(os.EX_USAGE)


    start = '1990-01-02' if len(sys.argv) == 1 else sys.argv[1]

    print("Adding macro data from %s..." % (start))
    n = ingest(start)
    print("inserted/updated %d entries" % n)
