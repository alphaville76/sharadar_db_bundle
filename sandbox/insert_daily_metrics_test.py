import pandas as pd
import numpy as np
from sharadar.util.nasdaqdatalink_util import fetch_entire_table, fetch_table_by_date
from sharadar.util.equity_supplementary_util import insert_asset_info
from sharadar.util.equity_supplementary_util import insert_fundamentals
from sharadar.util.equity_supplementary_util import insert_daily_metrics
from contextlib import closing
import sqlite3
from os import environ as env
import os.path

import nasdaqdatalink
API_KEY=env["NASDAQ_API_KEY"]
nasdaqdatalink.ApiConfig.api_key=API_KEY

asset_dbpath = '/tmp/assets-7.sqlite'
if not os.path.exists(asset_dbpath):
    print ("DB file does not exist")
    exit(-1)

sharadar_metadata_df = nasdaqdatalink.get_table('SHARADAR/TICKERS', table='SEP', paginate=True)
sharadar_metadata_df.set_index('ticker', inplace=True)

start = pd.to_datetime('2021-03-08', utc=True)
daily_df = fetch_table_by_date(env["NASDAQ_API_KEY"], 'SHARADAR/DAILY', start)
with closing(sqlite3.connect(asset_dbpath)) as conn, conn, closing(conn.cursor()) as cursor:
    insert_daily_metrics(sharadar_metadata_df, daily_df, cursor, show_progress=True)