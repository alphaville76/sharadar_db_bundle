import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing

from sharadar.pipeline.engine import load_sharadar_bundle, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime
from sharadar.pipeline.factors import Exchange, Sector, IsDomestic, MarketCap, Fundamentals, EV
from zipline.pipeline.factors import AverageDollarVolume
from sharadar.pipeline.universes import TradableStocksUS

bundle = load_sharadar_bundle()

sectors = bundle.asset_finder.get_info([199059, 199623], 'sector')
print("sectors", sectors)

equities = bundle.asset_finder.retrieve_equities([199059, 199623])
print("equities", equities)

syms = symbols(['SPY'])
print("symbols", syms)
assert len(syms) == 1
assert syms[0].exchange == 'NYSEARCA'
assert syms[0].sid == 118691

spe = make_pipeline_engine()

pipe_start = pd.to_datetime('2020-02-03', utc=True)
pipe_end = pd.to_datetime('2020-02-07', utc=True)

pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)

stocks = spe.run_pipeline(pipe, pipe_start, pipe_end)
print("stocks.shape [close]", stocks.shape)
assert stocks.shape == (15, 1)

from sharadar.pipeline.factors import MarketCap, EV, Fundamentals
pipe_mkt_cap = Pipeline(columns={
    'mkt_cap': MarketCap()
},
)

start_time = time.time()
stocks = spe.run_pipeline(pipe_mkt_cap, pipe_start, pipe_end)
print("stocks.shape [mkt cap]", stocks.shape)
assert stocks.shape == (49867, 1)

pipe_mkt_cap_ev = Pipeline(columns={
    'mkt_cap': MarketCap(),
    'ev': EV(),
    'debt': Fundamentals(field='debtusd_arq'),
    'cash': Fundamentals(field='cashnequsd_arq')
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)

stocks = spe.run_pipeline(pipe_mkt_cap_ev, pipe_start, pipe_end)
print(stocks.shape)
assert stocks.shape == (15, 4)

print(stocks.iloc[0])
assert stocks.iloc[0]['cash'] == 39771000000.0
assert stocks.iloc[0]['debt'] == 108292000000.0
assert stocks.iloc[0]['ev'] == 1422775814800.0
assert stocks.iloc[0]['mkt_cap'] == 1354254814800.0


pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
    'Exchange': Exchange(),
    'Sector': Sector()
},
screen = (
    (TradableStocksUS()) &
    (AverageDollarVolume(window_length = 200, mask=TradableStocksUS()).top(10))
)
)
stocks = spe.run_pipeline(pipe, pipe_end)
print(stocks)
print("stocks.shape", stocks.shape)
assert stocks.shape == (10, 3)