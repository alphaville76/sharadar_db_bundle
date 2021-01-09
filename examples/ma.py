import pandas as pd
import zipline.api as algo
from zipline.pipeline import Pipeline
from zipline.pipeline.factors import AverageDollarVolume, Returns
from zipline.finance.execution import MarketOrder
from zipline.pipeline.filters import StaticAssets
from sharadar.pipeline.engine import symbols, sid, sids, symbol
from sharadar.util.run_algo import run_algorithm
from sharadar.util import performance

def initialize(context):
    algo.set_long_only()

    algo.attach_pipeline(make_pipeline(), 'pipeline')

    algo.schedule_function(
        rebalance,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(minutes=30),
    )


def make_pipeline():
    russell_universe = StaticAssets(symbols(['AAPL', 'AA', 'KKD', 'MON', 'SPY', 'XOM', 'JNJ', 'HD', 'MSFT']))

    filt = AverageDollarVolume(window_length=30, mask=russell_universe) > 10e6

    pipeline = Pipeline(
        columns={
            "1y_returns": Returns(window_length=252),
        },
        screen=filt
    )
    return pipeline


def before_trading_start(context, data):
    factors = algo.pipeline_output('pipeline')

    returns = factors["1y_returns"].sort_values(ascending=False)
    context.winners = returns.index[:3]


def rebalance(context, data):
    algo.record(aapl_price=data.current(symbol('AAPL'), "price"))

    # calculate intraday returns for our winners
    current_prices = data.current(context.winners, "price")
    prior_closes = data.history(context.winners, "close", 2, "1d").iloc[0]
    intraday_returns = (current_prices - prior_closes) / prior_closes

    positions = context.portfolio.positions

    # Exit positions we no longer want to hold
    for asset, position in positions.items():
        if asset not in context.winners:
            algo.order_target_value(asset, 0, style=MarketOrder())

    # Enter long positions
    for asset in context.winners:

        # if already long, nothing to do
        if asset in positions:
            continue

        # if the stock is up for the day, don't enter
        if intraday_returns[asset] > 0:
            continue

        # otherwise, buy a fixed $100K position per asset
        algo.order_target_value(asset, 100e3, style=MarketOrder())

def handle_data(context, data):
    pdf = performance.describe_portfolio(context.portfolio.positions)
    print(pdf)

result = run_algorithm(
    start=pd.Timestamp("2014-01-01", tz='utc'),
    end=pd.Timestamp("2019-12-15", tz='utc'),
    initialize=initialize,  # Define startup function
    handle_data=handle_data,
    before_trading_start=before_trading_start,
    capital_base=1000000,  # Set initial capital
    data_frequency='daily',  # Set data frequency
)
