import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.factors import (
    MarketCap,
    EV,
    Fundamentals,
    LogFundamentalsTrend,
    LogTimeTrend,
    InvestmentToAssets,
    InvestmentToAssetsTrend
)
from sharadar.pipeline.engine import load_sharadar_bundle, symbol, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
from zipline.pipeline.factors import CustomFactor
import numpy as np

#universe = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
universe = StaticAssets(symbols(['IBM']))
pipe = Pipeline(columns={
    'ia': InvestmentToAssets(),
    'ia_trend': InvestmentToAssetsTrend(mask=universe).trend
},
screen = universe
)

engine = make_pipeline_engine()
pipe_date = pd.to_datetime('2020-06-01', utc=True)
stocks = engine.run_pipeline(pipe, pipe_date)
print(stocks)

#NO PERIODIC
#                                                      ey  ey_trend  \
#2017-09-07 00:00:00+00:00 Equity(199059 [AAPL])  0.055782  0.000066
#                          Equity(199623 [IBM])   0.085316 -0.000013
#                          Equity(199713 [F])     0.083262  0.000290