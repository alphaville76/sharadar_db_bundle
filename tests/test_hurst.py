import numpy as np
import pytest
from sharadar.statistic.hurst import (
    _get_RS, get_RS, get_window_sizes, compute_hurst
)


class TestGetRS:
    def test_basic_rs(self):
        series = np.random.randn(100)
        rs = _get_RS(series)
        assert rs > 0
        assert np.isfinite(rs)

    def test_constant_series(self):
        series = np.ones(100)
        rs = _get_RS(series)
        assert rs == 0.0

    def test_short_series(self):
        series = np.array([1.0, 2.0, 3.0])
        rs = _get_RS(series)
        assert np.isfinite(rs)


class TestGetRSMultiple:
    def test_multiple_windows(self):
        series = np.random.randn(256)
        window_sizes = [8, 16, 32, 64]
        rs_values = get_RS(series, window_sizes)
        assert len(rs_values) == len(window_sizes)
        for rs in rs_values:
            assert rs >= 0

    def test_increasing_windows(self):
        np.random.seed(42)
        series = np.cumsum(np.random.randn(256))
        window_sizes = [8, 16, 32, 64, 128]
        rs_values = get_RS(series, window_sizes)
        assert len(rs_values) == 5


class TestGetWindowSizes:
    def test_returns_array(self):
        sizes = get_window_sizes()
        assert len(sizes) > 0
        assert all(s > 0 for s in sizes)

    def test_sizes_increasing(self):
        sizes = get_window_sizes()
        for i in range(1, len(sizes)):
            assert sizes[i] > sizes[i-1]


class TestComputeHurst:
    def test_random_walk_hurst_near_half(self):
        np.random.seed(42)
        series = np.cumsum(np.random.randn(1024))
        h = compute_hurst(series)
        assert 0.3 < h < 0.7

    def test_trending_series_hurst_above_half(self):
        series = np.cumsum(np.ones(1024) + 0.1 * np.random.randn(1024))
        h = compute_hurst(series)
        assert h > 0.5

    def test_hurst_in_valid_range(self):
        np.random.seed(123)
        series = np.cumsum(np.random.randn(512))
        h = compute_hurst(series)
        assert 0.0 <= h <= 1.0
