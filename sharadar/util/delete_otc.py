import sqlite3
import sys

FOLDER = '/home/c.cerbo/.zipline/data/sharadar_no_otc/latest/'

assets_db = sqlite3.connect(FOLDER + 'assets-7.sqlite', isolation_level=None)
prices_db = sqlite3.connect(FOLDER + 'prices.sqlite', isolation_level=None)
adjust_db = sqlite3.connect(FOLDER + 'adjustments.sqlite', isolation_level=None)

assets_db_cursor = assets_db.cursor()
prices_db_cursor = prices_db.cursor()
adjust_db_cursor = adjust_db.cursor()

assets_db_cursor.execute("begin")
prices_db_cursor.execute("begin")
adjust_db_cursor.execute("begin")

try:
    assets_db_cursor.execute("SELECT sid from equities WHERE exchange = 'OTC'")
    for row in assets_db_cursor.fetchall():
        sid = row[0]

        assets_db_cursor.execute("DELETE FROM asset_router WHERE sid = %d" % sid)
        assets_db_cursor.execute("DELETE FROM equities WHERE sid = %d" % sid)
        assets_db_cursor.execute("DELETE FROM equity_supplementary_mappings WHERE sid = %d" % sid)
        assets_db_cursor.execute("DELETE FROM equity_symbol_mappings WHERE sid = %d" % sid)

        prices_db_cursor.execute("DELETE FROM prices WHERE sid = %d" % sid)

        adjust_db_cursor.execute("DELETE FROM dividend_payouts WHERE sid = %d" % sid)
        adjust_db_cursor.execute("DELETE FROM dividends WHERE sid = %d" % sid)
        adjust_db_cursor.execute("DELETE FROM mergers WHERE sid = %d" % sid)
        adjust_db_cursor.execute("DELETE FROM splits WHERE sid = %d" % sid)
        adjust_db_cursor.execute("DELETE FROM stock_dividend_payouts WHERE sid = %d" % sid)

    assets_db_cursor.execute("commit")
    prices_db_cursor.execute("commit")
    adjust_db_cursor.execute("commit")
except:
    print("failed: " + sys.exc_info()[0])
    assets_db_cursor.execute("rollback")
    prices_db_cursor.execute("rollback")
    adjust_db_cursor.execute("rollback")

assets_db_cursor.execute("VACUUM")
prices_db_cursor.execute("VACUUM")
adjust_db_cursor.execute("VACUUM")

assets_db.close()
prices_db.close()
adjust_db.close()