import quandl
import os
from os import environ as env
from pandas.tseries.offsets import DateOffset
import pandas as pd
from sharadar.util.equity_supplementary_util import METADATA_HEADERS
from sharadar.util.output_dir import get_output_dir

quandl.ApiConfig.api_key = env["QUANDL_API_KEY"]


def _add_macro_def(df, sid, end_date, ticker, asset_name):
    start_date = pd.to_datetime("1998-01-01")

    # The first date we have trade data for this asset.
    first_traded = start_date

    # The date on which to close any positions in this asset.
    auto_close_date = end_date + pd.Timedelta(days=1)

    exchange = 'MACRO'
    df.loc[sid] = (ticker, asset_name, start_date, end_date, first_traded, auto_close_date, exchange)

def _quandl_get_monthly_to_daily(name, start_date, end_date, transform=None):
    m_start = start_date - DateOffset(months=3)
    df = quandl.get(name, start_date=m_start, end_date=end_date, transform=transform)
    new_index = pd.date_range(start=m_start, end=end_date)
    return df.reindex(new_index, method='ffill').loc[pd.date_range(start=start_date, end=end_date)]


def _to_prices_df(df, sid):
    df['sid'] = sid
    df.set_index('sid', append=True, inplace=True)
    df = _append_ohlc(df)
    return df


def _append_ohlc(df):
    df.index.names = ['date', 'sid']
    df.columns = ['open']
    df['high'] = df['low'] = df['close'] = df['open']
    df['volume'] = 100.0
    df['dividends'] = 0.0
    return df


def create_macro_equities_df(end_date):
    df = pd.DataFrame(columns=METADATA_HEADERS)
    #_add_macro_def(df, 10001, end_date, 'TR1M', 'US Treasury Bill 1 MO')
    #_add_macro_def(df, 10002, end_date, 'TR2M', 'US Treasury Bill 2 MO')
    _add_macro_def(df, 10003, end_date, 'TR3M', 'US Treasury Bill 3 MO')
    _add_macro_def(df, 10006, end_date, 'TR6M', 'US Treasury Bill 6 MO')
    _add_macro_def(df, 10012, end_date, 'TR1Y', 'US Treasury Bond 1 YR')
    _add_macro_def(df, 10024, end_date, 'TR2Y', 'US Treasury Bond 2 YR')
    _add_macro_def(df, 10036, end_date, 'TR3Y', 'US Treasury Bond 3 YR')
    _add_macro_def(df, 10060, end_date, 'TR5Y', 'US Treasury Bond 5 YR')
    _add_macro_def(df, 10084, end_date, 'TR7Y', 'US Treasury Bond 7 YR')
    _add_macro_def(df, 10120, end_date, 'TR10Y', 'US Treasury Bond 10 YR')
    _add_macro_def(df, 10240, end_date, 'TR20Y', 'US Treasury Bond 20 YR')
    #_add_macro_def(df, 10360, end_date, 'TR30Y', 'US Treasury Bond 30 YR')
    _add_macro_def(df, 10400, end_date, 'CBOND', 'US Corporate Bond Yield')
    _add_macro_def(df, 10410, end_date, 'INDPRO', 'Industrial Production Index')
    _add_macro_def(df, 10420, end_date, 'INDPROPCT', 'Industrial Production Montly % Change')
    _add_macro_def(df, 10430, end_date, 'PMICMP', 'Purchasing Managers Index')
    _add_macro_def(df, 10440, end_date, 'UNRATE', 'Civilian Unemployment Rate')
    _add_macro_def(df, 10450, end_date, 'RATEINF', 'US Inflation Rates YoY')
    return df


def create_macro_prices_df(start, end, calendar=None):
    # https://www.quandl.com/data/USTREASURY/YIELD-Treasury-Yield-Curve-Rates
    tres_df = quandl.get("USTREASURY/YIELD", start_date=start, end_date=end)
    # sids
    tres_df.columns = [10001, 10002, 10003, 10006, 10012, 10024, 10036, 10060, 10084, 10120, 10240, 10360]
    # TR1M, TR2M and TR30Y excluded because of too many missing data
    tres_df.drop(columns=[10001, 10002, 10360], inplace=True)

    if calendar is not None:
        sessions = calendar.sessions_in_range(start, end)
        tres_df = tres_df.reindex(sessions.tz_localize(None))
        tres_df = tres_df.fillna(method='ffill')
        tres_df = tres_df.dropna()

    prices = tres_df.unstack().to_frame()
    prices = prices.swaplevel()
    prices = _append_ohlc(prices)

    # https://www.quandl.com/data/ML/USEY-US-Corporate-Bond-Index-Yield
    corp_bond_df = _to_prices_df(quandl.get("ML/USEY", start_date=start, end_date=end), 10400)
    prices = prices.append(corp_bond_df)

    # Industrial Production Change
    # Frequency: monthly
    indpro_df = _to_prices_df(_quandl_get_monthly_to_daily("FRED/INDPRO", start_date=start, end_date=end), 10410)
    prices = prices.append(indpro_df)

    # rdiff: row-on-row % change
    indpro_p_df = _to_prices_df(
        _quandl_get_monthly_to_daily("FRED/INDPRO", start_date=start, end_date=end, transform="rdiff"), 10420)
    prices = prices.append(indpro_p_df)

    # ISM Purchasing Managers Index
    # https://www.quandl.com/data/ISM/MAN_PMI-PMI-Composite-Index
    # Frequency: monthly
    pmi_df = _to_prices_df(_quandl_get_monthly_to_daily("ISM/MAN_PMI", start_date=start, end_date=end), 10430)
    prices = prices.append(pmi_df)

    # Civilian Unemployment Rate
    # https://www.quandl.com/data/FRED/UNRATE-Civilian-Unemployment-Rate
    # Frequency: monthly
    unrate_df = _to_prices_df(_quandl_get_monthly_to_daily("FRED/UNRATE", start_date=start, end_date=end), 10440)
    prices = prices.append(unrate_df)

    # https://www.quandl.com/data/RATEINF/INFLATION_USA-Inflation-YOY-USA
    # Frequency: monthly
    inf_df = _to_prices_df(_quandl_get_monthly_to_daily("RATEINF/INFLATION_USA", start_date=start, end_date=end), 10450)
    prices = prices.append(inf_df)

    prices.sort_index(inplace=True)
    return prices


if __name__ == "__main__":
    #TODO ask the dates
    prices_start = pd.to_datetime('1998-01-01')
    prices_end = pd.to_datetime('2020-07-07')

    from sharadar.pipeline.engine import load_sharadar_bundle
    from zipline.assets import ASSET_DB_VERSION
    from sharadar.data.sql_lite_assets import SQLiteAssetDBWriter
    from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarWriter
    from trading_calendars import get_calendar

    bundle = load_sharadar_bundle()
    calendar = get_calendar('NYSE')

    macro_equities_df = create_macro_equities_df(prices_end)

    macro_prices_df = create_macro_prices_df(prices_start, prices_end, calendar)

    output_dir = get_output_dir()
    asset_dbpath = os.path.join(output_dir, ("assets-%d.sqlite" % ASSET_DB_VERSION))
    asset_db_writer = SQLiteAssetDBWriter(asset_dbpath)
    asset_db_writer.write(equities=macro_equities_df)

    prices_dbpath = os.path.join(output_dir, "prices.sqlite")
    sql_daily_bar_writer = SQLiteDailyBarWriter(prices_dbpath, calendar)
    sql_daily_bar_writer.write(macro_prices_df)

