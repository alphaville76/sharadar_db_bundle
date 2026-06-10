import pandas as pd
from sharadar.pipeline.engine import symbols, make_pipeline_engine
from sharadar.pipeline.factors import MarketCap, Fundamentals, EV
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.filters import StaticAssets

spe = make_pipeline_engine()

date = pd.Timestamp('2023-10-24')
pipeline = Pipeline(columns={
    'close': USEquityPricing.close.latest,
    'sharesbas_arq': Fundamentals(field='sharesbas_arq'),
    'sharefactor_arq': Fundamentals(field='sharefactor_arq'),
    'mkt_cap': MarketCap(),
    'ev': EV(),
    'debt': Fundamentals(field='debtusd_arq'),
    'cash': Fundamentals(field='cashnequsd_arq')
},
    screen=StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)

array = spe._finder.get_fundamentals([199623, 199713, 199059], 'marketcap', as_of_date=date, n=1)
print(array)

df2 = spe.run_pipeline(pipeline, date)
print(df2.to_markdown())