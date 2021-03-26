from sharadar.pipeline.engine import (
    load_sharadar_bundle,
    symbol,
    symbols,
    prices,
    prices_old,
    returns,
    trading_date)
import pandas as pd


start = pd.to_datetime('2021-03-08', utc=True)
end = pd.to_datetime('2021-03-12', utc=True)

assets = symbols(['IBM', 'F', 'AAPL'])
p = prices(assets, start, end)
print(p)
assert p.shape == (5, 3)
assert type(p)  == pd.DataFrame

assets = symbols(['SPY'])
p = prices(assets, start, end)
assert p.shape == (5,)
assert type(p)  == pd.Series

assets = [symbol('SPY')]
p = prices(assets, start, end)
print(p)
assert p.shape == (5,)
assert type(p)  == pd.Series

assets = symbols(['IBM', 'F', 'AAPL'])
p = returns(assets, start, end)
print(p)
assert p.shape == (5, 3)
assert type(p)  == pd.DataFrame

assets = symbols(['SPY'])
p = returns(assets, start, end)
print(p)
assert p.shape == (5,)
assert type(p)  == pd.Series

assets = [symbol('SPY')]
p = returns(assets, start, end)
assert p.shape == (5,)
assert type(p)  == pd.Series




