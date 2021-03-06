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

    schedule_function(rebalance,
                      date_rules.week_start(),
                      time_rules.market_open())


def rebalance(context, data):
    prices = data.history(context.security, 'price', 100, '1d')
    macd, signal, hist = talib.MACD(prices)

    if macd[-1] > signal[-1]:
        order_target_percent(context.security, 1)
    else:
        order_target_percent(context.security, -1)


def analyze(context, perf):
    duration = (time.time() - context.begin)
    log.info('Backtest executed in %s' % (time.strftime("%H:%M:%S", time.gmtime(duration))))
    performance.analyze(perf, __file__, __doc__, duration)



def run_this_algorithm():
    run_algorithm(initialize=initialize,
                  start=pd.Timestamp('2001-02-01', tz='utc'),
                  analyze=analyze
                  )


if __name__ == "__main__":
    # execute only if run as a script
    run_this_algorithm()

if __name__ == "__main__":
    # execute only if run as a script
    run_this_algorithm()