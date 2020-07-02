import pyfolio as pf
import pandas as pd
from sharadar.pipeline.engine import symbol, returns
import matplotlib.pylab as plt
import numpy as np


def create_log_rets(rets, benchmark_rets):
    cum_log_returns = np.log1p(rets).cumsum()
    cum_log_benchmark_rets = np.log1p(benchmark_rets).cumsum()

    fig, ax = plt.subplots()
    cum_log_returns.plot(ax=ax, figsize=(20, 10))
    cum_log_benchmark_rets.plot(ax=ax)
    ax.grid(True)
    ax.axhline(y=0, linestyle='--', color='black')
    ax.legend(['Backtest', 'Benchmark'])
    plt.title("Log returns")

    return plt

if __name__ == "__main__":
    results = pd.read_pickle('../algo/haugen20/haugen20_202006300552_perf.dump')
    rets, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(results)

    benchmark_rets = returns(symbol('SPY'), rets.index[0], rets.index[0])
    plt = create_log_rets(rets, benchmark_rets)
    plt.show()
