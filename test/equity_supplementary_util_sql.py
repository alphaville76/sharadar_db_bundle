import pandas as pd
import numpy as np
from sharadar.util.quandl_util import fetch_entire_table
from sharadar.util.equity_supplementary_util import insert_asset_info
from sharadar.util.equity_supplementary_util import insert_fundamentals
from sharadar.util.equity_supplementary_util import insert_daily_metrics
from contextlib import closing
import sqlite3
from os import environ as env
import os.path

import quandl
API_KEY=env["QUANDL_API_KEY"]
quandl.ApiConfig.api_key=API_KEY

db_file = '/tmp/assets-7.sqlite'
if not os.path.exists(db_file):
    print ("DB file does not exist")
    exit(-1)

with closing(sqlite3.connect(db_file)) as conn, conn, closing(conn.cursor()) as cursor:
    cursor.execute("""
CREATE TABLE IF NOT EXISTS equity_supplementary_mappings (
        sid INTEGER NOT NULL,
        field TEXT NOT NULL,
        start_date INTEGER NOT NULL,
        end_date INTEGER NOT NULL,
        value TEXT NOT NULL,
        PRIMARY KEY (sid, field, start_date),
        FOREIGN KEY(sid) REFERENCES equities (sid)
);
    """)

sharadar_metadata_df = quandl.get_table('SHARADAR/TICKERS', table='SEP', paginate=True)
sharadar_metadata_df.set_index('ticker', inplace=True)
with closing(sqlite3.connect(db_file)) as conn, conn, closing(conn.cursor()) as cursor:
    insert_asset_info(sharadar_metadata_df, cursor)


daily_df = fetch_entire_table(env["QUANDL_API_KEY"], "SHARADAR/DAILY", parse_dates=['date'])
with closing(sqlite3.connect(db_file)) as conn, conn, closing(conn.cursor()) as cursor:
    insert_daily_metrics(sharadar_metadata_df, daily_df, cursor, show_progress=True)


sf1_df = fetch_entire_table(API_KEY, "SHARADAR/SF1", parse_dates=['datekey', 'reportperiod'])
with closing(sqlite3.connect(db_file)) as conn, conn, closing(conn.cursor()) as cursor:
    insert_fundamentals(sharadar_metadata_df, sf1_df, cursor, show_progress=True)
