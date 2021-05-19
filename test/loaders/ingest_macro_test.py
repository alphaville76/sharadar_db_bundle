from sharadar.loaders.ingest_macro import create_macro_equities_df, create_macro_prices_df

start_fetch_date = "2021-05-17"

macro_equities_df = create_macro_equities_df()
print(macro_equities_df)
assert macro_equities_df.shape == (15, 7)

macro_prices_df = create_macro_prices_df(start_fetch_date)
print(macro_prices_df)
assert macro_prices_df.shape[0] >= 160
assert macro_prices_df.shape[1] == 5


