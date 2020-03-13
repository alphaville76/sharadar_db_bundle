import pandas as pd
import numpy as np

from sharadar.util.quandl_util import fetch_data_table

from sharadar.util.equity_supplementary_util import equity_extra_data_sf1

import quandl
API_KEY="env["QUANDL_API_KEY"]"
quandl.ApiConfig.api_key=API_KEY

sharadar_metadata_df = quandl.get_table('SHARADAR/TICKERS', table='SEP', paginate=True)
sharadar_metadata_df.set_index('ticker', inplace=True)

sf1_df = fetch_data_table(API_KEY, "SHARADAR/SF1", parse_dates=['datekey', 'reportperiod'])
extra_sf1 = equity_extra_data_sf1(sharadar_metadata_df, sf1_df)

extra_sf1.to_csv('extra_sf1.csv')