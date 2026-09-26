#!/bin/bash

[ $# -eq 1 ] || { echo -e "You must provide the START_DATE argument\nUsage: $0 <START_DATE>"; exit 1; }
[ "$(basename "$PWD")" = "latest" ] || { echo "Error: you must be in the 'latest' folder"; exit 1; }


start_date="$1"
start_date_s=$(date --date="$start_date 00:00:00 +0000" +"%s") || { echo "Invalid date"; exit 1; }
start_date_ns=$(date --date="$start_date 00:00:00 +0000" +"%s%9N") || { echo "Invalid date"; exit 1; }

cd ..
cp -rv latest latest_all
cd latest

sqlite3 prices.sqlite "DELETE FROM prices WHERE date < $start_date"
sqlite3 prices.sqlite "VACUUM"

sqlite3 adjustments.sqlite "DELETE FROM dividend_payouts WHERE record_date < $start_date_s"
sqlite3 adjustments.sqlite "DELETE FROM dividends WHERE effective_date < $start_date_s"
sqlite3 adjustments.sqlite "DELETE FROM splits WHERE effective_date < $start_date_s"
sqlite3 adjustments.sqlite "VACUUM"

sqlite3 assets-7.sqlite "DELETE FROM equity_supplementary_mappings WHERE start_date < $start_date_ns"
sqlite3 assets-7.sqlite "VACUUM"
