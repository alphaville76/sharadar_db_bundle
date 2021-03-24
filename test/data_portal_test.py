from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.utils.calendars import get_calendar
from zipline.pipeline.data import USEquityPricing
from zipline.data.data_portal import DataPortal
from sharadar.pipeline.engine import symbols, make_pipeline_engine, load_sharadar_bundle
import pandas as pd

bundle = load_sharadar_bundle()
# Set the dataloader
pricing_loader = USEquityPricingLoader.without_fx(bundle.equity_daily_bar_reader, bundle.adjustment_reader)


# Define the function for the get_loader parameter
def choose_loader(column):
    if column not in USEquityPricing.columns:
        raise Exception('Column not in USEquityPricing')
    return pricing_loader

# Set the trading calendar
trading_calendar = get_calendar('NYSE')

start_date = pd.to_datetime('2020-08-26', utc=True)
end_date = pd.to_datetime('2020-09-02', utc=True)
bar_count = trading_calendar.session_distance(start_date, end_date)

# Create a data portal
data_portal = DataPortal(bundle.asset_finder,
                         trading_calendar = trading_calendar,
                         first_trading_day = start_date,
                         equity_daily_reader = bundle.equity_daily_bar_reader,
                         adjustment_reader = bundle.adjustment_reader)

equity = bundle.asset_finder.lookup_symbol("AAPL", end_date)
history = data_portal.get_history_window(assets=[equity], end_dt=end_date, bar_count=bar_count,
                               frequency='1d',
                               field='close',
                               data_frequency='daily')

print(history)