from sharadar.loaders.ingest_macro import create_macro_equities_df, create_macro_prices_df
import pandas as pd

start = pd.to_datetime("2020-04-01")
end = pd.to_datetime("2020-04-14")

macro_prices_df = create_macro_prices_df(start, end)
print(macro_prices_df)
assert macro_prices_df.shape == (187, 6)

macro_equities_df = create_macro_equities_df(start, end)
print(macro_equities_df)
assert macro_equities_df.shape == (18, 7)

#Adding macro data from 2010-01-04 00:00:00 to 2020-04-22 00:00:00 ...


