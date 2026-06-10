import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing

from sharadar.pipeline.engine import load_sharadar_bundle, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime
from sharadar.pipeline.factors import DaysSinceFiling

bundle = load_sharadar_bundle()

bundle.asset_finder.retrieve_equities([199059, 199623])

spe = make_pipeline_engine()

pipe_start = pd.to_datetime('2020-02-03', utc=True)
pipe_end = pd.to_datetime('2020-02-07', utc=True)

universe = StaticAssets(symbols(['IBM', 'F', 'AAPL']))

pipe_mkt_cap = Pipeline(columns={
    'days_since_filing': DaysSinceFiling(mask=universe),
},
screen = universe
)

start_time = time.time()
stocks = spe.run_pipeline(pipe_mkt_cap, pipe_start, pipe_end)
print("stocks.shape [mkt cap]", stocks)
