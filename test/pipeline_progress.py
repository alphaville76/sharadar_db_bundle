import pandas as pd
from zipline.pipeline import Pipeline
from sharadar.pipeline.factors import (
    MarketCap,
    EV,
    Fundamentals
)
from sharadar.pipeline.engine import load_sep_bundle, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime

bundle = load_sep_bundle()

spe = make_pipeline_engine()

pipe_start = pd.to_datetime('2019-02-04', utc=True)
pipe_end = pd.to_datetime('2020-02-07', utc=True)

pipe_mkt_cap_ev = Pipeline(columns={
    'mkt_cap': MarketCap(),
    'ev': EV(),
    'debt': Fundamentals(field='debtusd_arq'),
    'cash': Fundamentals(field='cashnequsd_arq')
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)


start_time = time.time()
#stocks = spe.run_pipeline(pipe_mkt_cap_ev, pipe_start, pipe_end)
stocks = spe.run_pipeline(pipe_mkt_cap_ev, pipe_start, pipe_end, chunksize=125)
print(stocks)
print("Elapsed time: %s" % datetime.timedelta(seconds=(time.time() - start_time)))

# mkt cap, ev, debt and cash: 0:00:47.085863
# ev, debt and cash: 0:00:47.506076
# debt and cash:  0:00:02.075351
# cash: 0:00:01.553650

#[2020-04-10 21:44:36.628128] INFO: PipelineEngine: Computing term 1 of 9 [AssetExists()]
#[2020-04-10 21:44:36.628236] INFO: PipelineEngine: Term already in workspace: no computation needed
#[2020-04-10 21:44:36.628313] INFO: PipelineEngine: Computing term 2 of 9 [USEquityPricing.close::float64]
#[2020-04-10 21:45:18.692352] INFO: PipelineEngine: Elapsed time: 0:00:42.064021
#[2020-04-10 21:45:18.692474] INFO: PipelineEngine: Computing term 3 of 9 [Fundamentals([], 1)]
#[2020-04-10 21:45:19.107182] INFO: PipelineEngine: Elapsed time: 0:00:00.414686
#[2020-04-10 21:45:19.107315] INFO: PipelineEngine: Computing term 4 of 9 [Fundamentals([], 1)]
#[2020-04-10 21:45:19.548932] INFO: PipelineEngine: Elapsed time: 0:00:00.441602
#[2020-04-10 21:45:19.549062] INFO: PipelineEngine: Computing term 5 of 9 [MarketCap([USEquityPricing.close, Fundamentals(...), Fundamentals(...)], 1)]
#[2020-04-10 21:45:19.550731] INFO: PipelineEngine: Elapsed time: 0:00:00.001691
#[2020-04-10 21:45:19.550806] INFO: PipelineEngine: Computing term 6 of 9 [Fundamentals([], 1)]
#[2020-04-10 21:45:19.990796] INFO: PipelineEngine: Elapsed time: 0:00:00.439909
#[2020-04-10 21:45:19.990993] INFO: PipelineEngine: Computing term 7 of 9 [Fundamentals([], 1)]
#[2020-04-10 21:45:20.420044] INFO: PipelineEngine: Elapsed time: 0:00:00.429059
#[2020-04-10 21:45:20.420162] INFO: PipelineEngine: Computing term 8 of 9 [EV([MarketCap(...), Fundamentals(...), Fundamentals(...)], 1)]
#[2020-04-10 21:45:20.421706] INFO: PipelineEngine: Elapsed time: 0:00:00.001560
#[2020-04-10 21:45:20.421775] INFO: PipelineEngine: Computing term 9 of 9 [StaticAssets([], 0)]
#[2020-04-10 21:45:20.422163] INFO: PipelineEngine: Elapsed time: 0:00:00.000390
#(15, 4)
#Elapsed time: 0:00:45.002658

#new method: Elapsed time: 0:00:11.731463 - 4x faster!

#2nd new method chunck 21: Elapsed time: 0:01:52.484754
#2nd new method no chunks:Elapsed time: 0:01:33.944235
