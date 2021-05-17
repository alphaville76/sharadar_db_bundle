#!/bin/bash

if [ ! $# -eq 1 ]
  then
    echo "You must provide the START_DATE argument"
    echo "Usage $0 <START_DATE>"
    exit 1
fi

export start_date="$1"
export start_date_s=`date --date="$start_date 00:00:00 +0000" +"%s"`
export start_date_ns=`date --date="$start_date 00:00:00 +0000" +"%s%9N"`

cd ~/.zipline/data/sharadar
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
