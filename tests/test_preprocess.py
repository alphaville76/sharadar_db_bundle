import numpy as np
import pandas as pd
import pytest
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

    def test_dataframe_preserves_structure(self):
        df = pd.DataFrame({'a': [0.0, 5.0, 10.0], 'b': [10.0, 20.0, 30.0]}, 
                          index=['x', 'y', 'z'])
        result = normalize(df)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ['a', 'b']
        assert list(result.index) == ['x', 'y', 'z']

    def test_with_nan_values(self):
        data = np.array([0.0, np.nan, 10.0])
        result = normalize(data)
        assert not np.any(np.isinf(result))
        # NaN handling should not cause infinite values

    def test_dataframe_with_nan(self):
        df = pd.DataFrame({'a': [1.0, np.nan, 3.0], 'b': [np.nan, 2.0, 3.0]})
        result = normalize(df)
        assert isinstance(result, pd.DataFrame)
        assert result.shape == df.shape


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

    def test_with_nan_values(self):
        df = pd.DataFrame({'a': [1.0, 2.0, np.nan, 4.0, 100.0]})
        result = winsorize_iqr(df)
        assert result.shape == df.shape
        # Should not fail with NaN

    def test_empty_dataframe(self):
        df = pd.DataFrame({'a': []})
        result = winsorize_iqr(df)
        assert result.empty

    def test_rejects_non_dataframe(self):
        with pytest.raises(TypeError):
            winsorize_iqr([1, 2, 3, 4, 5])

    def test_rejects_numpy_array(self):
        with pytest.raises(TypeError):
            winsorize_iqr(np.array([1, 2, 3, 4, 5]))

    def test_multiple_columns_with_different_outliers(self):
        df = pd.DataFrame({
            'a': [1, 2, 3, 4, 5, 6, 7, 8, 9, 100],
            'b': [-50, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        })
        result = winsorize_iqr(df)
        assert result['a'].max() < 100
        assert result['b'].min() > -50
