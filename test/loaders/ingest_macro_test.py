from sharadar.loaders.ingest_macro import create_macro_equities_df, create_macro_prices_df

start_fetch_date = "2020-04-01"
end_fetch_date = "2020-04-14"

print(start_fetch_date, end_fetch_date)

macro_equities_df = create_macro_equities_df()
print(macro_equities_df)
assert macro_equities_df.shape == (15, 7)

macro_prices_df = create_macro_prices_df(start_fetch_date)
print(macro_prices_df)
assert macro_prices_df.shape == (160, 5)

macro_prices_df = create_macro_prices_df(start_fetch_date)
print(macro_prices_df)
assert macro_prices_df.shape[0] >= 160
assert macro_prices_df.shape[1] == 5


