from sharadar.util.metric_daily import default_daily, DailyBenchmarkReturnsAndVolatility
from zipline.finance.metrics import BenchmarkReturnsAndVolatility

class TestDefaultDaily:
    def test_returns_set(self):
        result = default_daily()
        assert isinstance(result, set)

    def test_no_original_benchmark_metric(self):
        result = default_daily()
        for metric in result:
            assert not (type(metric) is BenchmarkReturnsAndVolatility)

    def test_contains_daily_benchmark(self):
        result = default_daily()
        has_daily = any(isinstance(m, DailyBenchmarkReturnsAndVolatility) for m in result)
        assert has_daily

class TestDailyBenchmarkReturnsAndVolatility:
    def test_end_of_bar_does_nothing(self):
        metric = DailyBenchmarkReturnsAndVolatility()
        # Should not raise
        metric.end_of_bar(None, None, None, None, None)

    def test_is_subclass(self):
        assert issubclass(DailyBenchmarkReturnsAndVolatility, BenchmarkReturnsAndVolatility)
