import pyfolio as pf
import pandas as pd
import numpy as np
from sharadar.pipeline.engine import symbol, returns
import empyrical as ep

results = pd.read_pickle('../../../algo/haugen20/haugen20_perf.dump')
rets, pos, trax = pf.utils.extract_rets_pos_txn_from_zipline(results)

start_date = rets.index[0]
end_date = rets.index[-1]
print(start_date, end_date)

benchmark_rets = returns(symbol('SPY'), start_date, end_date)

cum_rets = ep.cum_returns(rets, 1.0)
cum_benchmark_rets = ep.cum_returns(benchmark_rets, 1.0)

cum_log_returns = np.log1p(rets).cumsum()
cum_log_benchmark_rets = np.log1p(benchmark_rets).cumsum()

fig1 = pf.create_returns_tear_sheet(rets, pos, trax, benchmark_rets=benchmark_rets, return_fig=True)