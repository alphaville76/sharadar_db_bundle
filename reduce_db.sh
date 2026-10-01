#!/bin/bash

[ $# -eq 1 ] || { echo -e "You must provide the START_DATE argument\nUsage: $0 <START_DATE>"; exit 1; }
[ "$(basename "$PWD")" = "latest" ] || { echo "Error: you must be in the 'latest' folder"; exit 1; }

start_date="$1"
start_date_s=$(date --date="$start_date 00:00:00 +0000" +"%s") || { echo "Invalid date"; exit 1; }
start_date_ns=$(date --date="$start_date 00:00:00 +0000" +"%s%9N") || { echo "Invalid date"; exit 1; }

cd ..
# NOTE: Ensure you have enough disk space before creating this full backup copy!
cp -rv latest latest_all
cd latest

# ==========================================
# 1. PRICES.SQLITE (Table: prices)
# ==========================================
echo "Optimizing prices.sqlite..."
sqlite3 prices.sqlite "CREATE TABLE prices_new AS SELECT * FROM prices WHERE date >= $start_date;"
sqlite3 prices.sqlite "DROP TABLE prices;"
sqlite3 prices.sqlite "ALTER TABLE prices_new RENAME TO prices;"

echo "Recreating indexes on prices.sqlite..."
sqlite3 prices.sqlite "CREATE INDEX IF NOT EXISTS ix_prices_sid ON prices (sid);"
sqlite3 prices.sqlite "CREATE INDEX IF NOT EXISTS ix_prices_date ON prices (date);"
sqlite3 prices.sqlite "CREATE INDEX IF NOT EXISTS ix_properties_key ON properties (key);"
sqlite3 prices.sqlite "VACUUM;"

# ==========================================
# 2. ADJUSTMENTS.SQLITE (Tables: dividend_payouts, dividends, splits)
# ==========================================
echo "Optimizing adjustments.sqlite..."

# Table: dividend_payouts
sqlite3 adjustments.sqlite "CREATE TABLE dividend_payouts_new AS SELECT * FROM dividend_payouts WHERE record_date >= $start_date_s;"
sqlite3 adjustments.sqlite "DROP TABLE dividend_payouts;"
sqlite3 adjustments.sqlite "ALTER TABLE dividend_payouts_new RENAME TO dividend_payouts;"

# Table: dividends
sqlite3 adjustments.sqlite "CREATE TABLE dividends_new AS SELECT * FROM dividends WHERE effective_date >= $start_date_s;"
sqlite3 adjustments.sqlite "DROP TABLE dividends;"
sqlite3 adjustments.sqlite "ALTER TABLE dividends_new RENAME TO dividends;"

# Table: splits
sqlite3 adjustments.sqlite "CREATE TABLE splits_new AS SELECT * FROM splits WHERE effective_date >= $start_date_s;"
sqlite3 adjustments.sqlite "DROP TABLE splits;"
sqlite3 adjustments.sqlite "ALTER TABLE splits_new RENAME TO splits;"

echo "Recreating indexes on adjustments.sqlite..."
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS ix_splits_index ON splits(index);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS ix_mergers_index ON mergers(index);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS ix_dividend_payouts_date ON dividend_payouts(date);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS ix_stock_dividend_payouts_index ON stock_dividend_payouts(index);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS ix_dividends_indexON dividends(index);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS splits_sids ON splits(sid);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS splits_effective_date ON splits(effective_date);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS mergers_sids ON mergers(sid);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS mergers_effective_date ON mergers(effective_date);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS dividends_sid ON dividends(sid);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS dividends_effective_date ON dividends(effective_date);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS dividend_payouts_sid ON dividend_payouts(sid);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS dividends_payouts_ex_date ON dividend_payouts(ex_date);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS stock_dividend_payouts_sid ON stock_dividend_payouts(sid);"
sqlite3 adjustments.sqlite "CREATE INDEX IF NOT EXISTS stock_dividends_payouts_ex_date ON stock_dividend_payouts(ex_date);"
sqlite3 adjustments.sqlite "VACUUM;"

# ==========================================
# 3. ASSETS-7.SQLITE (Table: equity_supplementary_mappings)
# ==========================================
echo "Optimizing assets-7.sqlite..."
sqlite3 assets-7.sqlite "CREATE TABLE equity_supplementary_mappings_new AS SELECT * FROM equity_supplementary_mappings WHERE start_date >= $start_date_ns;"
sqlite3 assets-7.sqlite "DROP TABLE equity_supplementary_mappings;"
sqlite3 assets-7.sqlite "ALTER TABLE equity_supplementary_mappings_new RENAME TO equity_supplementary_mappings;"

echo "Recreating indexes on assets-7.sqlite..."
sqlite3 assets-7.sqlite "CREATE INDEX ix_equity_symbol_mappings_sid ON equity_symbol_mappings(sid);"
sqlite3 assets-7.sqlite "CREATE INDEX ix_equity_symbol_mappings_company_symbol ON equity_symbol_mappings(company_symbol);"
sqlite3 assets-7.sqlite "CREATE UNIQUE INDEX ix_futures_contracts_symbol ON futures_contracts(symbol);"
sqlite3 assets-7.sqlite "CREATE INDEX ix_futures_contracts_root_symbol ON futures_contracts(root_symbol);"
sqlite3 assets-7.sqlite "CREATE INDEX idx_start_date_field  ON equity_supplementary_mappings(start_date, field);"
sqlite3 assets-7.sqlite "CREATE INDEX idx_equity_mappings_field_value ON equity_supplementary_mappings(field, value);"
sqlite3 assets-7.sqlite "CREATE INDEX idx_equity_supp_start ON equity_supplementary_mappings(start_date);"

sqlite3 assets-7.sqlite "VACUUM;"

echo "Procedure completed successfully!"
