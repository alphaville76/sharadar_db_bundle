"""
A simple Pipeline algorithm that longs the top 3 stocks by RSI and shorts
the bottom 3 each day.
"""
from sharadar.pipeline.filters import NamedUniverse
from six import viewkeys
from zipline.api import (
    attach_pipeline,
    order_target_percent,
    pipeline_output,
    schedule_function,
)
from zipline.pipeline.factors import RSI
import zipline.algorithm as algo
from zipline.pipeline import Pipeline
from zipline.api import record, get_datetime
from zipline.utils.pandas_utils import normalize_date
from zipline.algorithm import log
from sharadar.pipeline.factors import (
    Fundamentals,
    EV
)

def initialize(context):
    attach_pipeline(make_pipeline(), 'my_pipeline')

    """
    Called once at the start of the algorithm.
    """
    # Rebalance every day, 1 hour after market open.
    schedule_function(
        rebalance,
        algo.date_rules.every_day(),
        algo.time_rules.market_open(hours=1),
    )

    # Record tracking variables at the end of each day.
    schedule_function(
        record_vars,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(),
    )


def make_pipeline():
    rsi = RSI()
    return Pipeline(
        columns = {
            'longs': rsi.top(3),
            'shorts': rsi.bottom(3),
            'revenue': Fundamentals(field='revenue_art'),
        },
        # no screen: 1 mo -> ca. 4 minutes;
        #            1 yr -> 20m

        # base_universe: ca. 1 mo. -> 3 minutes
        #screen = base_universe()

        # tradable_stocks(): 1 mo. -> ca 1h
        # 1yr -< 1h 40m
        #screen = TradableStocksUS()

        # NamedUniverse('tradable_stocks_us'): 1yr 7m
        screen = NamedUniverse('tradable_stocks_us')
    )


def base_universe():
    #return TradableStocksUS()
    return (
        (Fundamentals(field='revenue_art') > 0) &
        (Fundamentals(field='assets_arq') > 0) &
        (Fundamentals(field='equity_arq') > 0) &
        (EV() > 0)

    )

def rebalance(context, data):
    log.info("rebalance) " + str(normalize_date(get_datetime())))
    # Pipeline data will be a dataframe with boolean columns named 'longs' and
    # 'shorts'.
    pipeline_data = context.pipeline_data.dropna()
    log.info(pipeline_data.head())

    all_assets = pipeline_data.index

    longs = all_assets[pipeline_data.longs]
    shorts = all_assets[pipeline_data.shorts]

    record(universe_size=len(all_assets))

    # Build a 2x-leveraged, equal-weight, long-short portfolio.
    one_third = 1.0 / 3.0
    for asset in longs:
        order_target_percent(asset, one_third)

    for asset in shorts:
        order_target_percent(asset, -one_third)

    # Remove any assets that should no longer be in our portfolio.
    portfolio_assets = longs | shorts
    positions = context.portfolio.positions
    for asset in viewkeys(positions) - set(portfolio_assets):
        # This will fail if the asset was removed from our portfolio because it
        # was delisted.
        if data.can_trade(asset):
            order_target_percent(asset, 0)


def before_trading_start(context, data):
    context.pipeline_data = pipeline_output('my_pipeline')


def record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    log.info("record_vars) " + str(normalize_date(get_datetime())))
    pass


def handle_data(context, data):
    """
    Called every minute.
    """
    pass


def analyze(context=None, results=None):
    import matplotlib.pyplot as plt
    fig = plt.figure()
    ax = fig.add_subplot(111)
    results.portfolio_value.plot(ax=ax)
    ax.set_ylabel('Portfolio value (USD)')
    plt.show()
