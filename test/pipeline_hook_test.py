from sharadar.pipeline.engine import prices, returns
from sharadar.pipeline.engine import symbols, make_pipeline_engine, load_sharadar_bundle
from sharadar.pipeline.factors import *
from zipline.pipeline import Pipeline
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.factors import Latest, Returns
from zipline.pipeline.filters import StaticAssets
from interface import implements
from zipline.pipeline.hooks import PipelineHooks

from zipline.utils.compat import contextmanager
from logbook import Logger, StreamHandler
import sys
StreamHandler(sys.stdout).push_application()
log = Logger('Logbook')


pd.set_option('display.float_format', lambda x: '%.2f' % x)

engine = make_pipeline_engine()

universe = StaticAssets(symbols(['AAPL', 'IBM', 'F']))
pipe = Pipeline(columns={
    'close': Latest([EquityPricing.close], mask=universe),
    'mkt_cap': MarketCap(mask=universe),
    'sector': Sector()
},
    screen=universe,
)
stocks = engine.run_pipeline(pipe, '2021-03-17', '2022-03-24', hooks=[])
print(stocks)