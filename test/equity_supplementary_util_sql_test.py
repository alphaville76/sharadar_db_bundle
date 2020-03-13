import pandas as pd
import numpy as np
from sharadar.util.quandl_util import fetch_data_table
from sharadar.util.equity_supplementary_util import insert_equity_extra_data_basic, insert_equity_extra_data_sf1
from contextlib import closing
import sqlite3

import quandl
API_KEY="env["QUANDL_API_KEY"]"
quandl.ApiConfig.api_key=API_KEY

db_file = '/tmp/assets-7.sqlite'

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

sf1_df = fetch_data_table(API_KEY, "SHARADAR/SF1", parse_dates=['datekey', 'reportperiod'])

with closing(sqlite3.connect(db_file)) as conn, conn, closing(conn.cursor()) as cursor:
    insert_equity_extra_data_basic(sharadar_metadata_df.head(5), cursor)

with closing(sqlite3.connect(db_file)) as conn, conn, closing(conn.cursor()) as cursor:
    insert_equity_extra_data_sf1(sharadar_metadata_df, sf1_df.head(50), cursor, show_progress=True)