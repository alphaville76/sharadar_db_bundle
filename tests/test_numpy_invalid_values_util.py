import numpy as np
from sharadar.util.numpy_invalid_values_util import (
    nansubtract,
    nandivide,
    nanlog,
    nanlog1p,
    nanmean,
    nanvar,
    nanstd,
)


class TestNansubtract:
    def test_normal_subtraction_same_shape(self):
        a = np.array([5.0, 10.0, 15.0])
        b = np.array([1.0, 2.0, 3.0])
        result = nansubtract(a, b)
        np.testing.assert_array_equal(result, np.array([4.0, 8.0, 12.0]))

    def test_inf_in_b_gives_nan(self):
        a = np.array([5.0, 10.0, 15.0])
        b = np.array([1.0, np.inf, 3.0])
        result = nansubtract(a, b)
        assert result[0] == 4.0
        assert np.isnan(result[1])
        assert result[2] == 12.0

    def test_negative_inf_in_b_gives_nan(self):
        a = np.array([5.0, 10.0])
        b = np.array([np.NINF, 2.0])
        result = nansubtract(a, b)
        assert np.isnan(result[0])
        assert result[1] == 8.0

    def test_different_shapes_broadcasts(self):
        a = np.array([[1.0, 2.0], [3.0, 4.0]])
        b = np.array([1.0, 1.0])
        result = nansubtract(a, b)
        expected = np.array([[0.0, 1.0], [2.0, 3.0]])
        np.testing.assert_array_equal(result, expected)

    def test_scalar_and_array(self):
        a = 10.0
        b = np.array([1.0, 2.0, 3.0])
        result = nansubtract(a, b)
        np.testing.assert_array_equal(result, np.array([9.0, 8.0, 7.0]))

    def test_both_arrays_same_shape_all_finite(self):
        a = np.array([100.0, 200.0, 300.0])
        b = np.array([50.0, 100.0, 150.0])
        result = nansubtract(a, b)
        np.testing.assert_array_equal(result, np.array([50.0, 100.0, 150.0]))

    def test_nan_in_b_gives_nan(self):
        a = np.array([5.0, 10.0])
        b = np.array([1.0, np.nan])
        result = nansubtract(a, b)
        assert result[0] == 4.0
        assert np.isnan(result[1])


class TestNandivide:
    def test_normal_division(self):
        a = np.array([10.0, 20.0, 30.0])
        b = np.array([2.0, 4.0, 5.0])
        result = nandivide(a, b)
        np.testing.assert_array_equal(result, np.array([5.0, 5.0, 6.0]))

    def test_division_by_zero_gives_nan(self):
        a = np.array([10.0, 20.0, 30.0])
        b = np.array([2.0, 0.0, 5.0])
        result = nandivide(a, b)
        assert result[0] == 5.0
        assert np.isnan(result[1])
        assert result[2] == 6.0

    def test_all_zeros_in_divisor(self):
        a = np.array([1.0, 2.0, 3.0])
        b = np.array([0.0, 0.0, 0.0])
        result = nandivide(a, b)
        assert all(np.isnan(result))

    def test_negative_divisor(self):
        a = np.array([10.0, 20.0])
        b = np.array([-2.0, -5.0])
        result = nandivide(a, b)
        np.testing.assert_array_equal(result, np.array([-5.0, -4.0]))

    def test_array_division_mixed(self):
        a = np.array([6.0, 0.0, -9.0, 12.0])
        b = np.array([3.0, 5.0, 0.0, -4.0])
        result = nandivide(a, b)
        assert result[0] == 2.0
        assert result[1] == 0.0
        assert np.isnan(result[2])
        assert result[3] == -3.0


class TestNanlog:
    def test_positive_values(self):
        a = np.array([1.0, np.e, np.e**2])
        result = nanlog(a)
        np.testing.assert_allclose(result, np.array([0.0, 1.0, 2.0]))

    def test_zero_gives_nan(self):
        a = np.array([0.0, 1.0])
        result = nanlog(a)
        assert np.isnan(result[0])
        assert result[1] == 0.0

    def test_negative_gives_nan(self):
        a = np.array([-1.0, -5.0])
        result = nanlog(a)
        assert all(np.isnan(result))

    def test_array_with_mixed_values(self):
        a = np.array([-1.0, 0.0, 1.0, np.e, 10.0])
        result = nanlog(a)
        assert np.isnan(result[0])
        assert np.isnan(result[1])
        np.testing.assert_allclose(result[2], 0.0)
        np.testing.assert_allclose(result[3], 1.0)
        np.testing.assert_allclose(result[4], np.log(10.0))


class TestNanlog1p:
    def test_positive_values(self):
        a = np.array([1.0, 2.0, 3.0])
        result = nanlog1p(a)
        np.testing.assert_allclose(result, np.log1p(np.array([1.0, 2.0, 3.0])))

    def test_zero_gives_nan(self):
        # a > 0 is the condition, so a=0 yields NaN
        a = np.array([0.0])
        result = nanlog1p(a)
        assert np.isnan(result[0])

    def test_negative_gives_nan(self):
        a = np.array([-1.0, -2.0])
        result = nanlog1p(a)
        assert all(np.isnan(result))

    def test_array_with_mixed_values(self):
        a = np.array([-2.0, -1.0, 0.0, 1.0, 5.0])
        result = nanlog1p(a)
        assert np.isnan(result[0])
        assert np.isnan(result[1])
        assert np.isnan(result[2])
        np.testing.assert_allclose(result[3], np.log1p(1.0))
        np.testing.assert_allclose(result[4], np.log1p(5.0))


class TestNanmean:
    def test_normal_mean(self):
        a = np.array([1.0, 2.0, 3.0, 4.0])
        result = nanmean(a)
        assert result == 2.5

    def test_all_nan_returns_nan(self):
        a = np.array([np.nan, np.nan, np.nan])
        result = nanmean(a)
        assert np.isnan(result)

    def test_partial_nan(self):
        a = np.array([1.0, np.nan, 3.0])
        result = nanmean(a)
        assert result == 2.0

    def test_2d_with_axis_0(self):
        a = np.array([[1.0, 2.0], [3.0, 4.0], [np.nan, 6.0]])
        result = nanmean(a, axis=0)
        np.testing.assert_allclose(result, np.array([2.0, 4.0]))

    def test_2d_with_axis_1(self):
        a = np.array([[1.0, np.nan], [3.0, 4.0]])
        result = nanmean(a, axis=1)
        np.testing.assert_allclose(result, np.array([1.0, 3.5]))

    def test_empty_slice_returns_nan(self):
        a = np.array([[np.nan, np.nan], [1.0, 2.0]])
        result = nanmean(a, axis=1)
        assert np.isnan(result[0])
        assert result[1] == 1.5


class TestNanvar:
    def test_normal_variance(self):
        a = np.array([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
        result = nanvar(a)
        np.testing.assert_allclose(result, np.var(a))

    def test_single_element(self):
        a = np.array([5.0])
        result = nanvar(a)
        assert result == 0.0

    def test_all_same_values_gives_zero(self):
        a = np.array([3.0, 3.0, 3.0, 3.0])
        result = nanvar(a)
        assert result == 0.0

    def test_with_nan_values(self):
        a = np.array([1.0, np.nan, 3.0, 5.0])
        result = nanvar(a)
        np.testing.assert_allclose(result, np.nanvar(np.array([1.0, np.nan, 3.0, 5.0])))

    def test_2d_with_axis(self):
        a = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
        result = nanvar(a, axis=0)
        np.testing.assert_allclose(result, np.array([2.25, 2.25, 2.25]))


class TestNanstd:
    def test_normal_std(self):
        a = np.array([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
        result = nanstd(a)
        np.testing.assert_allclose(result, np.std(a))

    def test_consistency_with_nanvar(self):
        a = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
        std_result = nanstd(a)
        var_result = nanvar(a)
        np.testing.assert_allclose(std_result, np.sqrt(var_result))

    def test_all_same_values_gives_zero(self):
        a = np.array([4.0, 4.0, 4.0])
        result = nanstd(a)
        assert result == 0.0

    def test_with_nan_values(self):
        a = np.array([1.0, np.nan, 2.0, np.nan, 3.0])
        result = nanstd(a)
        np.testing.assert_allclose(result, np.nanstd(np.array([1.0, np.nan, 2.0, np.nan, 3.0])))

    def test_2d_with_axis(self):
        a = np.array([[1.0, 2.0], [3.0, 4.0]])
        result = nanstd(a, axis=0)
        np.testing.assert_allclose(result, np.array([1.0, 1.0]))
