import numpy as np
import pandas as pd
from sharadar.statistic.preprocess import normalize, winsorize_iqr


class TestNormalize:
    def test_basic_normalization(self):
        data = np.array([0.0, 5.0, 10.0])
        result = normalize(data)
        np.testing.assert_array_almost_equal(result, [0.0, 0.5, 1.0])

    def test_already_normalized(self):
        data = np.array([0.0, 0.5, 1.0])
        result = normalize(data)
        np.testing.assert_array_almost_equal(result, [0.0, 0.5, 1.0])

    def test_negative_values(self):
        data = np.array([-10.0, 0.0, 10.0])
        result = normalize(data)
        np.testing.assert_array_almost_equal(result, [0.0, 0.5, 1.0])

    def test_constant_values(self):
        data = np.array([5.0, 5.0, 5.0])
        result = normalize(data)
        assert not np.any(np.isinf(result))

    def test_2d_array(self):
        data = np.array([[0.0, 10.0], [5.0, 20.0]])
        result = normalize(data)
        assert result.shape == data.shape


class TestWinsorizeIQR:
    def test_no_outliers(self):
        df = pd.DataFrame({'a': [1, 2, 3, 4, 5]})
        result = winsorize_iqr(df)
        assert len(result) == 5

    def test_with_outliers(self):
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 100]
        df = pd.DataFrame({'a': data})
        result = winsorize_iqr(df)
        assert result['a'].max() < 100

    def test_custom_multiplier(self):
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 50]
        df = pd.DataFrame({'a': data})
        result_strict = winsorize_iqr(df, mult=1.0)
        result_loose = winsorize_iqr(df, mult=3.0)
        assert result_strict['a'].max() <= result_loose['a'].max()

    def test_preserves_shape(self):
        df = pd.DataFrame({'a': range(100), 'b': range(100)})
        result = winsorize_iqr(df)
        assert result.shape == df.shape
