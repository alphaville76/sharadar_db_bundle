"""Daily metric computation for zipline trading simulations.

Provides a modified metrics set that replaces the default benchmark
returns calculation with a daily-compatible version suitable for
live trading with minute emission mode.
"""
from zipline.finance.metrics import BenchmarkReturnsAndVolatility, default_metrics

from zipline.finance import metrics

metrics.register

def default_daily():
    """Create a metrics set with daily benchmark computation.

    Replaces the standard BenchmarkReturnsAndVolatility metric with
    DailyBenchmarkReturnsAndVolatility which skips end_of_bar processing.

    Returns:
        set: Modified metrics set for daily trading simulations.
    """
    daily_metrics = default_metrics()
    daily_metrics = set(filter(lambda x: not isinstance(x, BenchmarkReturnsAndVolatility), daily_metrics))
    daily_metrics.add(DailyBenchmarkReturnsAndVolatility())
    return daily_metrics


class DailyBenchmarkReturnsAndVolatility(BenchmarkReturnsAndVolatility):
    """Benchmark returns metric that skips intraday bar processing.

    In live trading with minute emission mode, end_of_bar is called
    frequently but benchmark data is only available daily. This class
    overrides end_of_bar to be a no-op.
    """
    def end_of_bar(self, packet, ledger, dt, session_ix, data_portal):
        # to avoid end_of_bar. In live trading we only use emission_mode minute.
        pass
