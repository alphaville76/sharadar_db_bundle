"""Repair the dividends table of the adjustments database.

1. Older versions of SQLiteDailyAdjustmentWriter inserted rows by position, while
   calc_dividend_ratios returns the columns as (sid, effective_date, ratio) and the
   table is ("index", effective_date, ratio, sid). Rows were therefore stored as

       effective_date <- sid, ratio <- effective_date, sid <- ratio

   so zipline never found them (it selects dividends by effective_date) and dividend
   price adjustments were never applied. These rows are moved back into place.

2. Because of the shifted effective_date, reduce_db.sh deleted every dividend ratio.
   Ratios missing for rows of dividend_payouts are recomputed from the raw close of
   the previous session (same computation as the ingest).

Finally the pipeline cache is cleared, since its results were computed without
dividend adjustments.

Usage (run it with no algo or ingest using the bundle):
    python -m sharadar.util.fix_dividends [BUNDLE_DIR] [--dry-run]

BUNDLE_DIR is the folder with adjustments.sqlite, prices.sqlite and assets-7.sqlite
(default: ~/.zipline/data/sharadar/latest).
"""
import os
import sqlite3
import sys
from contextlib import closing

import pandas as pd

# A shifted row has a date in seconds in "ratio" (> 1e8, i.e. after 1973) and a
# ratio in "sid" (dividend ratios are in (0, 1]).
SHIFTED = "ratio > 100000000 AND sid > 0 AND sid <= 1"

MISSING_RATIOS = (
    "SELECT sid, ex_date, amount FROM dividend_payouts p WHERE amount > 0 AND NOT EXISTS "
    "(SELECT 1 FROM dividends d WHERE d.sid = p.sid AND d.effective_date = p.ex_date)"
)


def fix_shifted_rows(adjustments_path, dry_run=False):
    """Move shifted dividends values back into the right columns.

    Returns:
        int: Number of shifted rows (repaired unless dry_run).
    """
    with closing(sqlite3.connect(adjustments_path)) as con, con:
        n_total = con.execute("SELECT COUNT(*) FROM dividends").fetchone()[0]
        n_shifted = con.execute("SELECT COUNT(*) FROM dividends WHERE " + SHIFTED).fetchone()[0]
        print("dividends: %d rows, %d shifted" % (n_total, n_shifted))
        if dry_run or n_shifted == 0:
            return n_shifted
        # all right-hand side expressions use the values from before the update;
        # OR REPLACE drops a correct row with the same (effective_date, sid), if any
        con.execute(
            "UPDATE OR REPLACE dividends SET "
            "effective_date = CAST(ratio AS INTEGER), ratio = sid, sid = CAST(effective_date AS INTEGER) "
            "WHERE " + SHIFTED
        )
        print("repaired %d shifted rows" % n_shifted)
        return n_shifted


def rebuild_missing_ratios(bundle_dir, dry_run=False, chunk_size=200):
    """Compute and write the ratios missing for rows of dividend_payouts.

    Returns:
        int: Number of payouts without ratio (written ratios unless dry_run).
    """
    adjustments_path = os.path.join(bundle_dir, "adjustments.sqlite")
    with closing(sqlite3.connect(adjustments_path)) as con:
        missing = pd.read_sql_query(MISSING_RATIOS, con)
    print("dividend_payouts without ratio: %d" % len(missing))
    if dry_run or missing.empty:
        return len(missing)

    from sharadar.data.sql_lite_assets import SQLiteAssetFinder
    from sharadar.data.sql_lite_daily_pricing import SQLiteDailyAdjustmentWriter, SQLiteDailyBarReader

    prices_reader = SQLiteDailyBarReader(os.path.join(bundle_dir, "prices.sqlite"))
    asset_finder = SQLiteAssetFinder(os.path.join(bundle_dir, "assets-7.sqlite"))
    writer = SQLiteDailyAdjustmentWriter(adjustments_path, prices_reader, asset_finder,
                                         prices_reader.trading_calendar)

    missing['sid'] = missing['sid'].astype('int64')
    missing['ex_date'] = pd.to_datetime(missing['ex_date'], unit='s')
    sids = missing['sid'].unique()
    written = 0
    for i in range(0, len(sids), chunk_size):
        chunk = missing[missing['sid'].isin(sids[i:i + chunk_size])].reset_index(drop=True)
        ratios = writer.calc_dividend_ratios(chunk)
        if not ratios.empty:
            ratios['sid'] = ratios['sid'].astype('int64')
            writer.write_frame('dividends', ratios.reset_index(drop=True))
            written += len(ratios)
        print("sids %d/%d, ratios written: %d" % (min(i + chunk_size, len(sids)), len(sids), written))
    print("written %d ratios, %d payouts skipped (no previous close or ratio <= 0)"
          % (written, len(missing) - written))
    return written


def main(argv):
    dry_run = '--dry-run' in argv
    args = [a for a in argv if a != '--dry-run']
    if args:
        bundle_dir = args[0]
    else:
        from sharadar.util.output_dir import get_data_dir
        bundle_dir = get_data_dir()
    adjustments_path = os.path.join(bundle_dir, "adjustments.sqlite")
    if not os.path.isfile(adjustments_path):
        sys.exit("File not found: %s" % adjustments_path)
    print("Bundle folder: %s%s" % (bundle_dir, " (dry run)" if dry_run else ""))

    changed = fix_shifted_rows(adjustments_path, dry_run)
    changed += rebuild_missing_ratios(bundle_dir, dry_run)

    if changed and not dry_run:
        from sharadar.loaders.ingest_sharadar import clear_cache_dir
        clear_cache_dir()
        print("Pipeline cache cleared.")


if __name__ == '__main__':
    main(sys.argv[1:])