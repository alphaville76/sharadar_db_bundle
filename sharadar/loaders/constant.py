import pandas as pd

# 'sid' will be used as index
METADATA_HEADERS = ['symbol', 'asset_name', 'start_date', 'end_date', 'first_traded', 'auto_close_date', 'exchange']

OLDEST_DATE_SEP = pd.to_datetime('1997-12-31', utc=True)

EXCHANGE_DF = pd.DataFrame([
    ['NYSE', 'US'],
    ['NASDAQ', 'US'],
    ['OTC', 'US'],
    ['NYSEMKT', 'US'],
    ['NYSEARCA', 'US'],
    ['BATS', 'US'],
    ['INDEX', 'US'],
    ['MACRO', 'US'],
],
    columns=['exchange', 'country_code'])