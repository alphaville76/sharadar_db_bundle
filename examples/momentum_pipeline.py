"""
A simple Pipeline algorithm that longs the top 3 stocks by RSI and shorts
the bottom 3 each day.
"""
from six import viewkeys
from zipline.api import (
    attach_pipeline,
    order_target_percent,
    pipeline_output,
    record,
    schedule_function,
)
from zipline.algorithm import date_rules
from zipline.finance import commission, slippage
from zipline.pipeline.factors import RSI
import zipline.algorithm as algo
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.filters import TradableStocksUS


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
        columns={
            'longs': rsi.top(3),
            'shorts': rsi.bottom(3),
        },
    )


def rebalance(context, data):

    # Pipeline data will be a dataframe with boolean columns named 'longs' and
    # 'shorts'.
    pipeline_data = context.pipeline_data
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
    pass


def handle_data(context, data):
    """
    Called every minute.
    """
    pass


# Note: this function can be removed if running
# this algorithm on quantopian.com
def analyze(context=None, results=None):
    import matplotlib.pyplot as plt
    fig = plt.figure()
    ax = fig.add_subplot(111)
    results.portfolio_value.plot(ax=ax)
    ax.set_ylabel('Portfolio value (USD)')
    plt.show()
