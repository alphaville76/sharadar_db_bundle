import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.factors import MarketCap, EV, Fundamentals, Previous
from sharadar.pipeline.engine import load_sharadar_bundle, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime
from zipline.pipeline.factors import AverageDollarVolume, Latest
from sharadar.pipeline.factors import Exchange, Sector, IsDomesticCommonStock, MarketCap, Fundamentals, EV

pd.set_option('display.float_format', lambda x: '%.2f' % x)

spe = make_pipeline_engine()

pipe_start = pd.to_datetime('2020-01-02', utc=True)

pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
    'Close 1 YA': Previous([USEquityPricing.close], window_length=260),
    'mkt cap': MarketCap(),
    'mkt cap 1 YA': Previous([MarketCap()], window_length=260),
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)
stocks = spe.run_pipeline(pipe, pipe_start)
print(stocks)
print("stocks.shape [close]", stocks.shape)
assert stocks.iloc[0]['Close']        == 293.65
assert stocks.iloc[0]['Close 1 YA']   == 158.51
assert stocks.iloc[0]['mkt_cap']      == 1334543500000.00
assert stocks.iloc[0]['mkt cap 1 YA'] == 744230300000.00

