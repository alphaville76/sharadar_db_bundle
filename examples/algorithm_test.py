"""
This is a template algorithm on Quantopian for you to adapt and fill in.
"""
import zipline.algorithm as algo
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.filters import TradableStocksUS


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Rebalance every day, 1 hour after market open.
    algo.schedule_function(
        rebalance,
        algo.date_rules.every_day(),
        algo.time_rules.market_open(hours=1),
    )

    # Record tracking variables at the end of each day.
    algo.schedule_function(
        record_vars,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(),
    )

    # Create our dynamic stock selector.
    algo.attach_pipeline(make_pipeline(), 'pipeline')




def make_pipeline():
    """
    A function to create our dynamic stock selector (pipeline). Documentation
    on pipeline can be found here:
    https://www.quantopian.com/help#pipeline-title
    """

    # Base universe set to the QTradableStocksUS
    base_universe = TradableStocksUS()

    # Factor of yesterday's close price.
    yesterday_close = USEquityPricing.close.latest

    pipe = Pipeline(
        columns={
            'close': yesterday_close,
        },
        screen=base_universe
    )
    return pipe


def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    context.output = algo.pipeline_output('pipeline')

    # These are the securities that we are interested in trading each day.
    context.security_list = context.output.index


def rebalance(context, data):
    """
    Execute orders according to our schedule_function() timing.
    """
    pass


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