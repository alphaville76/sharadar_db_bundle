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

from os import environ as env
import quandl
quandl.ApiConfig.api_key = env["QUANDL_API_KEY"]

pd.set_option('display.float_format', lambda x: '%.2f' % x)

bundle = load_sharadar_bundle()
daily_df = quandl.get_table('SHARADAR/DAILY', date='2021-03-15', ticker='AAPL')
bundle_array = bundle.asset_finder.get_daily_metrics([110096,110097,199059], 'pb', pd.to_datetime('2021-03-15', utc=True), n=1)
print(daily_df)
print(bundle_array)

assert bundle_array[0][2] == 31.40
assert bundle_array[0][2] == daily_df['pb'].values[0]


bundle_array = bundle.asset_finder.get_daily_metrics([110096,110097,199059], 'pe', pd.to_datetime('2021-03-15', utc=True), n=3)
print(bundle_array)

#assert (bundle_array[0] == [np.nan, -4.9, 32.6]).all()
#assert (bundle_array[1] == [np.nan, -4.9, 33.0]).all()
#assert (bundle_array[2] == [np.nan, -4.6, 32.8]).all()


spe = make_pipeline_engine()
pipe= Pipeline(columns={
    'close': USEquityPricing.close.latest,
    'adv': AverageDollarVolume(window_length=3),
    'sharesbas_arq': Fundamentals(field='sharesbas_arq'),
    'sharefactor_arq': Fundamentals(field='sharefactor_arq'),
    'mkt_cap': MarketCap(),
    'ev': EV(),
    'debt': Fundamentals(field='debtusd_arq'),
    'cash': Fundamentals(field='cashnequsd_arq'),
    'EvEbit' : EvEbit(),
    'EvEbitda' : EvEbitda(),
    'PriceBook' : PriceBook(),
    'PriceEarnings' : PriceEarnings(),
    'PriceSales' : PriceSales()
},
screen = StaticAssets(symbols(['AAPL']))
)

pipe_start = pd.to_datetime('2021-03-15', utc=True)
stocks = spe.run_pipeline(pipe, pipe_start)
print(stocks.iloc[0])
assert stocks.iloc[0]['EvEbit']           ==             28.90
assert stocks.iloc[0]['EvEbitda']         ==             25.20
assert stocks.iloc[0]['PriceBook']        ==             31.40
assert stocks.iloc[0]['PriceEarnings']    ==             32.60
assert stocks.iloc[0]['PriceSales']       ==              7.10
assert stocks.iloc[0]['cash']             ==    36010000000.00
assert stocks.iloc[0]['close']            ==            121.03
assert stocks.iloc[0]['debt']             ==   112043000000.00
assert stocks.iloc[0]['ev']               ==  2157589000000.00
assert stocks.iloc[0]['mkt_cap']          ==  2081556000000.00
assert stocks.iloc[0]['sharefactor_arq']  ==              1.00
assert stocks.iloc[0]['sharesbas_arq']    ==    16788096000.00
assert abs(stocks.iloc[0]['adv'] - 12219809367.47) <= 1e-2

pipe_start = pd.to_datetime('2020-02-05', utc=True)
stocks = spe.run_pipeline(pipe, pipe_start)
print(stocks.iloc[0])
assert stocks.iloc[0]['EvEbit']           ==            21.80
assert stocks.iloc[0]['EvEbitda']          ==            18.50
assert stocks.iloc[0]['PriceBook']         ==            15.70
assert stocks.iloc[0]['PriceEarnings']     ==            24.40
assert stocks.iloc[0]['PriceSales']        ==             5.30
assert stocks.iloc[0]['cash']              ==   39771000000.00
assert stocks.iloc[0]['close']             ==           318.85
assert stocks.iloc[0]['debt']              ==  108292000000.00
assert stocks.iloc[0]['ev']                == 1475010300000.00
assert stocks.iloc[0]['mkt_cap']           == 1406489300000.00
assert stocks.iloc[0]['sharefactor_arq']   ==             1.00
assert stocks.iloc[0]['sharesbas_arq']     ==   17501920000.00
assert abs(stocks.iloc[0]['adv'] - 13253087818.53) <= 1e-2
