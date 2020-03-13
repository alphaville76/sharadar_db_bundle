import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.loaders.load import (
    load_sep_bundle,
    make_pipeline_engine,
    MarketCap,
    symbols
)
from zipline.pipeline.filters import StaticAssets
import time
import datetime

bundle = load_sep_bundle()

engine_start = pd.to_datetime('2018-12-01', utc=True)
engine_end = pd.to_datetime('2020-02-07', utc=True)
spe = make_pipeline_engine(engine_start, engine_end)

pipe_start = pd.to_datetime('2020-02-03', utc=True)
pipe_end = pd.to_datetime('2020-02-07', utc=True)

pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)

stocks = spe.run_pipeline(pipe, pipe_start, pipe_end)
print("stocks.shape [close]", stocks.shape)


pipe_mkt_cap = Pipeline(columns={
    'mkt_cap': MarketCap()
},
#screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)

start_time = time.time()
stocks = spe.run_pipeline(pipe_mkt_cap, pipe_start, pipe_end)
print("stocks.shape [mkt cap]", stocks.shape)
print("--- %s ---" % datetime.timedelta(seconds=(time.time() - start_time)))


