import pandas as pd
from sharadar.loaders.ingest_macro import ingest
from sharadar.pipeline.engine import load_sharadar_bundle, symbols, make_pipeline_engine
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.filters import StaticAssets

bundle = load_sharadar_bundle()


def ingest_macro():
    start = bundle.equity_daily_bar_reader.first_trading_day
    print("Adding macro data from %s ..." % (start))
    print(ingest(start))

#ingest_macro()

spe = make_pipeline_engine()

pipe_start = pd.to_datetime('2021-01-04', utc=True)
pipe_end = bundle.equity_daily_bar_reader.last_available_dt
macro = symbols(['TR3M', 'TR6M', 'TR1Y', 'TR2Y', 'TR3Y', 'TR5Y', 'TR7Y', 'TR10Y','TR20Y','CBOND', 'INDPRO', 'INDPROPCT', 'PMICMP', 'UNRATE', 'RATEINF'])
pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
},
screen = StaticAssets(macro)
)
stocks = spe.run_pipeline(pipe, pipe_start, pipe_end)
print(stocks.tail(30))
