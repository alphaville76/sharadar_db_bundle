from sharadar.pipeline.engine import load_sharadar_bundle, symbol, symbols, prices, returns, trading_date
import pandas as pd

df_rets = returns(
    assets=symbol('AAPL'),
    start='2013-01-01',
    end='2014-01-01'
)
print(df_rets)

assets = symbols(['IBM', 'F', 'AAPL'])

start = pd.to_datetime('2021-03-08', utc=True)
end = pd.to_datetime('2021-03-12', utc=True)

print(prices(assets, start, end))

print(prices(assets, '2020-02-03', '2020-02-05'))

print(returns(assets, start, end))

print(prices(symbols(['AAPL']), '2020-08-26', '2020-09-02'))



