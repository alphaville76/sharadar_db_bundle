from zipline.api import order_target, record, symbol
import matplotlib.pyplot as plt
import pandas as pd
from sharadar.util.logger import BacktestLogger
from sharadar.util.run_algo import run_algorithm
from sharadar.util import performance
from sharadar.util.performance import change_extension
from logbook import Logger, FileHandler, DEBUG, INFO, StreamHandler
import sys
import os
log = BacktestLogger(__file__)

import tracemalloc

tracemalloc.start()

# silence warnings
import warnings
warnings.filterwarnings('ignore')

def initialize(context):
    context.i = 0
    context.asset = symbol('AAPL')
    log.info("*  *  *  I N I T  *  *  *")


def handle_data(context, data):
    # Skip first 300 days to get full windows
    context.i += 1
    if context.i < 300:
        return

    # Compute averages
    # data.history() has to be called with the same params
    # from above and returns a pandas dataframe.
    short_mavg = data.history(context.asset, 'price', bar_count=100, frequency="1d").mean()
    long_mavg = data.history(context.asset, 'price', bar_count=300, frequency="1d").mean()

    # Trading logic
    log.info("short_mavg > long_mavg: %s" % (short_mavg > long_mavg))
    if short_mavg > long_mavg:
        # order_target orders as many shares as needed to
        # achieve the desired number of shares.
        order_target(context.asset, 100)
    elif short_mavg < long_mavg:
        order_target(context.asset, 0)

    # Save values for later inspection
    record(AAPL=data.current(context.asset, 'price'),
           short_mavg=short_mavg,
           long_mavg=long_mavg)


def analyze_old(context, perf):
    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    perf.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel('portfolio value in $')

    ax2 = fig.add_subplot(212)
    perf['AAPL'].plot(ax=ax2)
    perf[['short_mavg', 'long_mavg']].plot(ax=ax2)

    perf_trans = perf.ix[[t != [] for t in perf.transactions]]
    buys = perf_trans.ix[[t[0]['amount'] > 0 for t in perf_trans.transactions]]
    sells = perf_trans.ix[
        [t[0]['amount'] < 0 for t in perf_trans.transactions]]
    ax2.plot(buys.index, perf.short_mavg.ix[buys.index],
             '^', markersize=10, color='m')
    ax2.plot(sells.index, perf.short_mavg.ix[sells.index],
             'v', markersize=10, color='k')
    ax2.set_ylabel('price in $')
    plt.legend(loc=0)
    plt.show()

def analyze(context, perf):

    from sharadar.util.cache import wrappers
    for wrapper in wrappers:
        print(wrapper, wrapper.cache_info())

    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')

    print("[ Top 10 ]")
    for stat in top_stats[:10]:
        print(stat)

    performance.analyze(perf, __file__, __doc__, show_image=True)

def run_this_algorithm():
    #start = pd.Timestamp('2011-01-01', tz='utc'),
    #end = pd.Timestamp('2020-01-01', tz='utc'),
    # runs the zipline ALGO function
    run_algorithm(initialize=initialize,
                  start=pd.Timestamp('2019-01-02', tz='utc'),
                  handle_data=handle_data,
                  analyze=analyze,
                  state_filename=change_extension(__file__, '_context.pickle')
                  )

if __name__ == "__main__":
    # execute only if run as a script
    run_this_algorithm()