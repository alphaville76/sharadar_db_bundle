from zipline.finance.metrics import BenchmarkReturnsAndVolatility, default_metrics

from zipline.finance import metrics

metrics.register

def default_daily():
    daily_metrics = default_metrics()
    daily_metrics = set(filter(lambda x: not isinstance(x, BenchmarkReturnsAndVolatility), daily_metrics))
    daily_metrics.add(DailyBenchmarkReturnsAndVolatility())
    return daily_metrics


class DailyBenchmarkReturnsAndVolatility(BenchmarkReturnsAndVolatility):
    def end_of_bar(self, packet, ledger, dt, session_ix, data_portal):
        # to avoid end_of_bar. In live trading we only use emission_mode minute.
        pass
