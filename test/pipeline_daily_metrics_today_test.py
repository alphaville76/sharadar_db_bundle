import pandas as pd
import numpy as np
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.engine import load_sharadar_bundle, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime
from sharadar.pipeline.factors import *
from zipline.pipeline.factors import AverageDollarVolume
from sharadar.pipeline.universes import TradableStocksUS
from trading_calendars import get_calendar

from os import environ as env
import quandl
quandl.ApiConfig.api_key = env["QUANDL_API_KEY"]

pd.set_option('display.float_format', lambda x: '%.2f' % x)

bundle = load_sharadar_bundle()
last_available_daily_metrics_dt = bundle.asset_finder.last_available_daily_metrics_dt


spe = make_pipeline_engine()
pipe= Pipeline(columns={
    'close': USEquityPricing.close.latest,
    'mkt_cap': MarketCap(),
    'ev': EV(),
    'debt': Fundamentals(field='debtusd_arq'),
    'cash': Fundamentals(field='cashnequsd_arq')
},
screen = StaticAssets(symbols(['AAPL']))
)

pipe_start = pd.to_datetime('Today', utc=True)
calendar = get_calendar('XNYS')
if not calendar.is_session(pipe_start):
    pipe_start = calendar.previous_open(pipe_start).normalize()


df = quandl.get_table('SHARADAR/SEP', date={'gte':pipe_start}, ticker='AAPL')
print(df)

df = quandl.get_table("SHARADAR/DAILY", date={'gte':pipe_start}, ticker='AAPL')
print(df)
print("last_available_daily_metrics_dt", last_available_daily_metrics_dt)
print("pipe_start", pipe_start)

stocks = spe.run_pipeline(pipe, pipe_start)
print(stocks.iloc[0])
assert not pd.isna(stocks.iloc[0]['ev'])
assert not pd.isna(stocks.iloc[0]['mkt_cap'])

td_delta = 1
sessions = calendar.all_sessions
dates= pd.Series(data=sessions)\
    .groupby([sessions.year, sessions.month])\
    .nth(td_delta)\
    #.astype(np.int64)
print(dates)
