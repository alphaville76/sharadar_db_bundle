import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.factors import (
    MarketCap,
    EV,
    Fundamentals
)
from sharadar.pipeline.engine import load_sharadar_bundle, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime
from sharadar.pipeline.factors import IsBankruptcy, IsDelinquent
from sharadar.pipeline.factors import Exchange, Sector, IsDomesticCommonStock, MarketCap, Fundamentals, EV

def universe():
    return( (USEquityPricing.close.latest > 3) &
        #Exchange().element_of(['NYSE', 'NASDAQ', 'NYSEMKT']) &
        #(Sector().notnull()) &
        (IsBankruptcy().eq(1)))

spe = make_pipeline_engine()

date = pd.to_datetime('2020-09-28', utc=True)


pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
},
#screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
screen=universe()
)

stocks = spe.run_pipeline(pipe, date)
for e in stocks.index:
    print(e[1].symbol)


