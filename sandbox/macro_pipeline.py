import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.engine import symbol, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets

tickers = symbols(['TR1M', 'TR1Y', 'RATEINF'])
print(tickers)

pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
},
screen = StaticAssets(tickers)
)

engine = make_pipeline_engine()
pipe_start = pd.to_datetime('2020-02-03', utc=True)
pipe_end = pd.to_datetime('2020-02-07', utc=True)
stocks = engine.run_pipeline(pipe, pipe_start, pipe_end)
print("stocks.shape [close]", stocks)

print(symbol('TR1M').to_dict())
