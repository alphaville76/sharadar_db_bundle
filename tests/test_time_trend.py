"""Tests for sharadar.pipeline.factors.time_trend NaN-filling behavior.

Regression test for the fix that replaced the deprecated
``fillna(method='ffill')`` pandas API with ``ffill()``.
"""
import numpy as np
import pytest

from sharadar.pipeline.factors import time_trend


class TestTimeTrendFillsMissingValues:
    def test_forward_fills_interior_nans_when_allowed_missing_zero(self):
        # column 0 has a NaN in the middle that should be forward filled
        Y = np.array([
            [1.0, 10.0],
            [np.nan, 20.0],
            [3.0, np.nan],
        ])
        slope, resid = time_trend(Y.copy(), allowed_missing=0)
        # after ffill/fillna(0) there should be no NaNs propagated into the
        # slope computation itself (slope is finite for every column)
        assert np.all(np.isfinite(slope))

    def test_all_nan_column_filled_with_zero(self):
        Y = np.array([
            [np.nan, 1.0],
            [np.nan, 2.0],
            [np.nan, 3.0],
        ])
        slope, resid = time_trend(Y.copy(), allowed_missing=0)
        # an all-NaN column has nothing to ffill so it is filled with 0;
        # the resulting slope for that column should be exactly 0
        assert slope[0] == pytest.approx(0.0)

    def test_no_warnings_raised(self, recwarn):
        Y = np.array([
            [1.0, np.nan],
            [2.0, 2.0],
            [np.nan, 3.0],
        ])
        time_trend(Y.copy(), allowed_missing=0)
        assert not any(
            issubclass(w.category, (FutureWarning, DeprecationWarning))
            for w in recwarn.list
        )
