import pandas as pd
from sharadar.pipeline.engine import symbols, make_pipeline_engine
from sharadar.pipeline.factors import Fundamentals
from sharadar.pipeline.factors import FundamentalsTTM
from zipline.pipeline import Pipeline
from zipline.pipeline.filters import StaticAssets

pd.set_option('display.float_format', lambda x: '%.2f' % x)

engine = make_pipeline_engine()

pipe_start = pd.to_datetime('2020-01-02', utc=True)

pipe = Pipeline(columns={
    'saleTTM0': FundamentalsTTM(field='revenue'),
    'saleTTM1': FundamentalsTTM(field='revenue', window_length=2),
    'sale0': Fundamentals(field='revenue', window_length=1),
    'sale1': Fundamentals(field='revenue', window_length=2),
    'sale2': Fundamentals(field='revenue', window_length=3),
    'sale3': Fundamentals(field='revenue', window_length=4),

    'sale4': Fundamentals(field='revenue', window_length=5),
    'sale5': Fundamentals(field='revenue', window_length=6),
    'sale6': Fundamentals(field='revenue', window_length=7),
    'sale7': Fundamentals(field='revenue', window_length=8)
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)
stocks = engine.run_pipeline(pipe, pipe_start, hooks=[])
print(stocks.T)
for i in range(3):
    assert (stocks.iloc[i]['sale0']+stocks.iloc[i]['sale1']+stocks.iloc[i]['sale2']+stocks.iloc[i]['sale3']) == stocks.iloc[i]['saleTTM0']


