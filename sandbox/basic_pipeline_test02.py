import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.factors import MarketCap, EV, Fundamentals
from sharadar.pipeline.engine import load_sharadar_bundle, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime
from zipline.pipeline.factors import AverageDollarVolume
from sharadar.pipeline.factors import Exchange, Sector, IsDomesticCommonStock, MarketCap, Fundamentals, EV

bundle = load_sharadar_bundle()

sectors = bundle.asset_finder.get_info([199059, 199623], 'sector')
print("sectors", sectors)
assert (sectors == [['Technology', 'Technology']]).all()

equities = bundle.asset_finder.retrieve_equities([199059, 199623])
print("equities", equities)

syms = symbols(['SPY'])
print("symbols", syms)
assert len(syms) == 1
assert syms[0].exchange == 'NYSEARCA'
assert syms[0].sid == 118691

spe = make_pipeline_engine()

pipe_start = pd.to_datetime('2020-01-02', utc=True)
pipe_end = pd.to_datetime('2020-01-06', utc=True)


pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)
stocks = spe.run_pipeline(pipe, pipe_start, pipe_end)
print(stocks)
print("stocks.shape [close]", stocks.shape)
assert stocks.shape == (9, 1)


universe = AverageDollarVolume(window_length = 5).top(10)
pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest
},
screen = universe
)
stocks = spe.run_pipeline(pipe, pipe_end)
print(stocks)
print("stocks.shape [close]", stocks.shape)
assert stocks.shape == (10, 1)