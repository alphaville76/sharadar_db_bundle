from zipline.pipeline.loaders.equity_pricing_loader import USEquityPricingLoader
from zipline.pipeline.domain import EquityCalendarDomain
from zipline.pipeline.data import USEquityPricing
import pandas as pd
import numpy as np
from sharadar.pipeline.engine import symbols, make_pipeline_engine, load_sharadar_bundle

bundle = load_sharadar_bundle()
loader = USEquityPricingLoader.without_fx(bundle.equity_daily_bar_reader, bundle.adjustment_reader)

domain = EquityCalendarDomain('US', 'XNYS')
columns = [USEquityPricing.close]
# AAPL sid 199059
sids = pd.Int64Index([199059])
mask = None
start = pd.to_datetime('2020-08-26', utc=True)
end = pd.to_datetime('2020-09-02', utc=True)
trading_calendar = bundle.equity_daily_bar_reader.trading_calendar
dates = trading_calendar.sessions_in_range(start, end)

array = loader.load_adjusted_array(domain, columns, dates, sids, mask)
adjusted_array = list(array.values())[0]
print(adjusted_array.data)
print(adjusted_array.adjustments)
print(adjusted_array.data[3])