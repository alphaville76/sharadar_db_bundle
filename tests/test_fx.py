"""Tests for sharadar.pipeline.fx.SimpleFXRateReader.

Regression test for the fix that replaced the (Python 3.13 incompatible)
``python-interface`` based ``implements(FXRateReader)`` declaration with a
plain ``FXRateReader`` ABC subclass.
"""
import numpy as np
import pandas as pd
import pytest

from sharadar.pipeline.fx import SimpleFXRateReader
from zipline.data.fx.base import FXRateReader


class TestSimpleFXRateReader:
    def test_is_instance_of_fx_rate_reader(self):
        reader = SimpleFXRateReader()
        assert isinstance(reader, FXRateReader)

    def test_get_rates_returns_ones_with_expected_shape(self):
        reader = SimpleFXRateReader()
        dts = pd.DatetimeIndex(['2020-01-01', '2020-01-02', '2020-01-03'], tz='UTC')
        bases = ['EUR', 'JPY']
        rates = reader.get_rates('mid', 'USD', bases, dts)
        assert rates.shape == (len(dts), len(bases))
        assert np.all(rates == 1.0)

    def test_get_rate_scalar_returns_one(self):
        reader = SimpleFXRateReader()
        rate = reader.get_rate_scalar('mid', 'USD', 'EUR', pd.Timestamp('2020-01-01', tz='UTC'))
        assert rate == 1.0

    def test_get_rates_columnar_returns_ones(self):
        reader = SimpleFXRateReader()
        dts = pd.DatetimeIndex(['2020-01-01', '2020-01-02'], tz='UTC')
        bases = np.array(['EUR', 'JPY'], dtype=object)
        rates = reader.get_rates_columnar('mid', 'USD', bases, dts)
        assert len(rates) == 2
        assert np.all(rates == 1.0)

    def test_cannot_instantiate_without_get_rates(self):
        # sanity check that FXRateReader is still a real ABC requiring
        # get_rates to be implemented
        with pytest.raises(TypeError):
            FXRateReader()
