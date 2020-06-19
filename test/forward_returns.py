import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.factors import MarketCap
from sharadar.pipeline.factors import EV
from sharadar.pipeline.factors import Fundamentals
from sharadar.pipeline.engine import load_sharadar_bundle, symbol, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime
from zipline.pipeline.factors import CustomFactor, DailyReturns, Returns
from sharadar.pipeline.engine import BundleLoader
from sharadar.pipeline.factors import ForwardsReturns


universe = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
    'monthly_ret': Returns(window_length=2),
    'monthly_fret': ForwardsReturns(window_length=3, mask=universe)
},
screen = universe
)

engine = make_pipeline_engine()
pipe_start = pd.to_datetime('2020-04-06', utc=True)
stocks = engine.run_pipeline(pipe, pipe_start)
print(stocks)