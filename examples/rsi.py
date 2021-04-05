from zipline.api import order_target, record, symbol
import matplotlib.pyplot as plt
from sharadar.pipeline.arbitrage_pricing import PurchaseManagerIndexBeta, InflationRateBeta
#from sharadar.pipeline.arbitrage_pricing import TBillBeta, TBillBondSpreadBeta, CorpGvrnBondsSpreadBeta
from sharadar.pipeline.arbitrage_pricing import TBillBeta, TBillBondSpreadBeta
from sharadar.stat.ann import NeuralNetwork
from sharadar.stat.preprocess import winsorize_iqr, normalize
from sharadar.pipeline.indicators_ttm import *
from sharadar.pipeline.universes import TRADABLE_STOCKS_US, NamedUniverse, TradableStocksUS
from zipline.api import (
    attach_pipeline,
    order_target,
    order_target_percent,
    pipeline_output,
    schedule_function,
    set_commission,
    set_slippage,
    set_long_only,
    sid
)
from zipline.pipeline import Pipeline
from sharadar.util.run_algo import run_algorithm
from sharadar.util import performance
from sharadar.pipeline.factors import (
    EV,
    Fundamentals,
    LogFundamentalsTrend,
    LogTimeTrend,
    LogLatest,
    Sector,
    Beta,
    StdDev,
    TradingVolume,
    ExcessReturn,
    InvestmentToAssets,
    InvestmentToAssetsTrend
)

import numpy as np
import pandas as pd
import time
from sharadar.util.events import date_rules
from zipline.algorithm import time_rules
from zipline.pipeline.data import USEquityPricing
from zipline.finance import commission, slippage
from zipline.pipeline.factors import DailyReturns, Returns, Latest
from sharadar.util.stop_loss_util import stop_loss_portfolio
from lightgbm import LGBMRegressor
from sharadar.util.logger import BacktestLogger
log = BacktestLogger(__file__)

import talib


def initialize(context):
    context.begin = time.time()
    context.security = sid(118267)  # ISHARES RUSSELL 3000 ETF
    context.small = 30
    context.large = 70

    context.stocks_etf = sid(118267)
    context.momentum = -1
    context.rsi = -1

    schedule_function(rebalance, date_rules.week_start(), time_rules.market_open())
    #schedule_function(record_vars, date_rules.every_day(), time_rules.market_close())
    schedule_function(record_vars, date_rules.month_start(), time_rules.market_close(hours=1))


def rebalance(context, data):
    prices = data.history(context.security, 'price', 100, '1d')
    rsi = talib.RSI(prices.values)[-1]
    log.info("RSI: %.2f" % rsi)


    if rsi < context.small:
        order_target_percent(context.security, 1)
        record(type=1)
    elif rsi > context.large:
        order_target_percent(context.security, -1)
        record(type=-1)


def analyze(context, perf):
    duration = (time.time() - context.begin)
    log.info('Backtest executed in %s' % (time.strftime("%H:%M:%S", time.gmtime(duration))))
    performance.analyze(perf, __file__, __doc__, duration, context=context)

def compute_vars(context, data, long_ema=120, short_ema=20):
    market_history = data.history(symbol('SPY'), 'price', long_ema * 2, '1d')

    lt_mean = np.nanmean(market_history[-long_ema:])
    st_mean = np.nanmean(market_history[-short_ema:])
    momentum = st_mean/lt_mean - 1.0

    rsi = talib.RSI(data.history(symbol('SPY'), 'price', 100, '1d').values)[-1]
    vix = data.history(symbol('^VIX'), 'price', 1, '1d')[0]

    return momentum, rsi, vix

def record_vars(context, data):
    momentum, rsi, vix = compute_vars(context, data)
    record(momentum=momentum)
    record(rsi=rsi)
    record(vix=vix)


def run_this_algorithm():
    run_algorithm(initialize=initialize,
                  start=pd.Timestamp('2001-02-01', tz='utc'),
                  analyze=analyze
                  )


if __name__ == "__main__":
    # execute only if run as a script
    run_this_algorithm()
