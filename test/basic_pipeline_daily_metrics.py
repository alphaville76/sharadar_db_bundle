import pandas as pd

from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.factors import MarketCap, EV, Fundamentals, Previous
from sharadar.pipeline.engine import load_sharadar_bundle, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime
from zipline.pipeline.factors import AverageDollarVolume, Latest, Returns
from sharadar.pipeline.factors import Exchange, Sector, IsDomesticCommonStock, MarketCap, Fundamentals, EV
import numpy as np

pd.set_option('display.float_format', lambda x: '%.2f' % x)

spe = make_pipeline_engine()

pipe_start = pd.to_datetime('2020-01-02', utc=True)

class Rmax5_21d(CustomFactor):
    """
    Highest 5 days return in the last 21 day
    """
    inputs = [Returns(window_length=5)]
    window_length = 21
    window_safe = False

    def compute(self, today, assets, out, ret5d):
        out[:] = np.nanmax(ret5d, axis=0)

pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
    'Close 1 YA': Previous([USEquityPricing.close], window_length=260),
    'mkt cap': MarketCap(),
    'mkt cap 1 YA': Previous([MarketCap()], window_length=260),
    'max 5d return': Rmax5_21d(mask=StaticAssets(symbols(['IBM', 'F', 'AAPL'])))
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

