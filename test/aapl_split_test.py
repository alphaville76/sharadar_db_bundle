from sharadar.pipeline.engine import prices, returns
from sharadar.pipeline.engine import symbols, make_pipeline_engine, load_sharadar_bundle
from sharadar.pipeline.factors import *
from zipline.pipeline import Pipeline
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.factors import Latest, Returns
from zipline.pipeline.filters import StaticAssets

pd.set_option('display.float_format', lambda x: '%.2f' % x)

# Apple's stock has split five times since the company went public.
# The stock split on a 4-for-1 basis on August 28, 2020, a 7-for-1 basis on June 9, 2014,
# and split on a 2-for-1 basis on February 28, 2005, June 21, 2000, and June 16, 1987.

start = pd.to_datetime('2020-08-26', utc=True)
end = pd.to_datetime('2020-09-02', utc=True)

# AAPL sid 199059
print(prices(symbols(['AAPL']), start, end))

engine = make_pipeline_engine()
universe = StaticAssets(symbols(['AAPL']))
pipe = Pipeline(columns={
    'close': Latest([EquityPricing.close], mask=universe),
    'mkt_cap': MarketCap(mask=universe),
    'prev': Previous([USEquityPricing.close], window_length=2, mask=universe),
    'ret': Returns(window_length=2, mask=universe),
},
    screen=universe,
)

stocks = engine.run_pipeline(pipe, start, end, hooks=[])
print(stocks)

expected = [[499.30, 2163847100000.00, 503.43, -0.01],
            [506.09, 2137988000000.00, 499.30, 0.01],
            [500.04, 2134533300000.00, 506.09, -0.01],
            [124.81, 2206911200000.00, 125.01, -0.00],
            [129.04, 2294818300000.00, 124.81, 0.03],
            [134.18, 2247273200000.00, 129.04, 0.04]]
assert np.sum(abs(stocks.values - expected)) <= 1e-1

bundle = load_sharadar_bundle()
adjustments = bundle.adjustment_reader.get_adjustments_for_sid('splits', 199059)
print(adjustments)

print(returns(symbols(['AAPL', 'IBM', 'F']), start, end))

universe = StaticAssets(symbols(['AAPL', 'IBM', 'F']))
pipe = Pipeline(columns={
    'close': Latest([EquityPricing.close], mask=universe),
    'mkt_cap': MarketCap(mask=universe)
},
    screen=universe,
)
stocks = engine.run_pipeline(pipe, '2021-03-17', '2021-03-24', hooks=[])
print(stocks)