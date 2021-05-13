from sharadar.pipeline.engine import prices, symbols, load_sharadar_bundle
import pandas as pd

pd.set_option('display.float_format', lambda x: '%.2f' % x)


# Apple's stock has split five times since the company went public.
# The stock split on a 4-for-1 basis on August 28, 2020, a 7-for-1 basis on June 9, 2014,
# and split on a 2-for-1 basis on February 28, 2005, June 21, 2000, and June 16, 1987.

start = pd.to_datetime('2020-08-26', utc=True)
end = pd.to_datetime('2020-09-02', utc=True)

# AAPL sid 199059
sids = symbols(['AAPL', 'F'])

print(prices(sids, start, end))
print("---")

bundle = load_sharadar_bundle()
pricing_reader=bundle.equity_daily_bar_reader
close, = pricing_reader.load_raw_arrays(['close'], start, end, sids,)
print(close)
print("---")


