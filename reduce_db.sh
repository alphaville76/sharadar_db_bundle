#!/bin/bash
# Removes historical data before START_DATE to reduce the size of the bundle databases.
# Every value valid on or after START_DATE is preserved (prices, adjustments and the
# as-of value of each supplementary field). History needed by lookback windows is NOT:
# choose START_DATE before your first backtest/live date with a margin covering the
# longest pipeline window (e.g. 1 year of prices, ~2 years for fundamentals n=4/TTM/YoY).
set -euo pipefail

[ $# -eq 1 ] || { echo -e "You must provide the START_DATE argument\nUsage: $0 <START_DATE>"; exit 1; }
[ "$(basename "$PWD")" = "latest" ] || { echo "Error: you must be in the 'latest' folder"; exit 1; }
command -v sqlite3 >/dev/null || { echo "Error: sqlite3 command not found"; exit 1; }
[ ! -e ../latest_all ] || { echo "Error: backup folder ../latest_all already exists, remove or rename it first"; exit 1; }

start_date=$(date --date="$1 00:00:00 +0000" +"%Y-%m-%d") || { echo "Invalid date"; exit 1; }
start_date_s=$(date --date="$start_date 00:00:00 +0000" +"%s")
start_date_ns=$(date --date="$start_date 00:00:00 +0000" +"%s%9N")

# NOTE: Ensure you have enough disk space before creating this full backup copy!
cp -r ../latest ../latest_all

# NOTE: Use DELETE instead of "CREATE TABLE ... AS SELECT": CTAS drops PRIMARY KEYs
# and indexes, so the INSERT OR REPLACE statements used by the daily ingest would
# append duplicate rows (e.g. "Index contains duplicate entries, cannot reshape").

# ==========================================
# 1. PRICES.SQLITE (Table: prices)
# ==========================================
echo "Optimizing prices.sqlite..."
# dates are stored as 'YYYY-MM-DD 00:00:00' strings
sqlite3 prices.sqlite "DELETE FROM prices WHERE date < '$start_date';"
sqlite3 prices.sqlite "VACUUM;"

# ==========================================
# 2. ADJUSTMENTS.SQLITE (Tables: dividend_payouts, dividends, splits)
# ==========================================
# Adjustments before START_DATE only affect prices before START_DATE, which are deleted.
echo "Optimizing adjustments.sqlite..."
sqlite3 adjustments.sqlite "DELETE FROM dividend_payouts WHERE record_date < $start_date_s;"
sqlite3 adjustments.sqlite "DELETE FROM dividends WHERE effective_date < $start_date_s;"
sqlite3 adjustments.sqlite "DELETE FROM splits WHERE effective_date < $start_date_s;"
sqlite3 adjustments.sqlite "VACUUM;"

# ==========================================
# 3. ASSETS-7.SQLITE (Table: equity_supplementary_mappings)
# ==========================================
# Values are valid from start_date until superseded. Keep, for each (sid, field), the
# last value before START_DATE: static fields (name, sector, category, ...) are stored
# with start_date = first price date, and the as-of value is needed on START_DATE.
echo "Optimizing assets-7.sqlite..."
sqlite3 assets-7.sqlite "CREATE INDEX IF NOT EXISTS idx_sid_field_start ON equity_supplementary_mappings (sid, field, start_date);"
sqlite3 assets-7.sqlite "DELETE FROM equity_supplementary_mappings WHERE start_date < $start_date_ns AND EXISTS (SELECT 1 FROM equity_supplementary_mappings b WHERE b.sid = equity_supplementary_mappings.sid AND b.field = equity_supplementary_mappings.field AND b.start_date > equity_supplementary_mappings.start_date AND b.start_date < $start_date_ns);"
sqlite3 assets-7.sqlite "VACUUM;"

echo "Procedure completed successfully!"