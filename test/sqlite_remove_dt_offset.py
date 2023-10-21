import sqlite3
import sys
from sharadar.util.output_dir import get_data_dir
import os

prices_dbpath = os.path.join(get_data_dir(), "prices.sqlite")
prices_db = sqlite3.connect(prices_dbpath + 'prices.sqlite', isolation_level=None)

prices_db_cursor = prices_db.cursor()

prices_db_cursor.execute("begin")

try:
    prices_db_cursor.execute("SELECT * from prices ORDER BY date")
    for row in prices_db_cursor.fetchall():
        date = row[0]
        no_offset_date = date.replace("+00:00", "")
        sid = row[1]
        prices_db_cursor.execute(
            "UPDATE prices set date = '%s' WHERE sid = '%s' and date = '%s'" % (no_offset_date, sid, date))
        print(date)

    prices_db_cursor.execute("commit")

    prices_db_cursor.execute("VACUUM")
except:
    print("failed: " + sys.exc_info()[0])
    prices_db_cursor.execute("rollback")



prices_db.close()
