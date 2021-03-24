"""
This is a template algorithm on Quantopian for you to adapt and fill in.
"""
import pandas as pd
import zipline.algorithm as algo
from sharadar.pipeline.engine import symbols
from sharadar.util.run_algo import run_algorithm
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.filters import StaticAssets
from zipline.api import (
    attach_pipeline,
    order_target,
    order_target_percent,
    pipeline_output,
    schedule_function,
    set_commission,
    set_slippage,
    set_long_only,
    sid,
    get_datetime
)
from sharadar.util.events import date_rules, time_rules
from zipline.pipeline.factors import (
    Returns)
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
    InvestmentToAssetsTrend,
    DaysSinceFiling,
    Exchange,
    IsDomesticCommonStock,
    MarketCap,
    Previous
)

def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Rebalance every day, 1 hour after market open.
    schedule_function(
        rebalance,
        date_rules.every_day(),
        time_rules.market_open(hours=1),
    )

    # Create our dynamic stock selector.
    attach_pipeline(make_pipeline(), 'pipeline')


def make_pipeline():
    base_universe = StaticAssets(symbols(['AAPL', 'SPY']))

    pipe = Pipeline(
        columns={
            'close': USEquityPricing.close.latest,
            'prev': Previous([USEquityPricing.close], window_length=2, mask=base_universe),
            'ret': Returns(window_length=2, mask=base_universe),
            'excess_return': ExcessReturn(window_length=2, mask=base_universe),
        },
        screen=base_universe
    )
    return pipe



def rebalance(context, data):
    output = pipeline_output('pipeline')
    print(get_datetime())
    print(output)


def handle_data(context, data):
    """
    Called every minute.
    """
    pass


def run_this_algorithm():
    run_algorithm(initialize=initialize,
                  start=pd.Timestamp('2020-08-26', tz='utc'),
                  end=pd.Timestamp('2020-09-02', tz='utc')
                  )


if __name__ == "__main__":
    # execute only if run as a script
    run_this_algorithm()
