import sqlite3
import sys

FOLDER = '/home/ccerbo/.zipline/data/sharadar/latest/'

prices_db = sqlite3.connect(FOLDER + 'prices.sqlite', isolation_level=None)

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
except:
    print("failed: " + sys.exc_info()[0])
    prices_db_cursor.execute("rollback")

#prices_db_cursor.execute("VACUUM")

prices_db.close()
