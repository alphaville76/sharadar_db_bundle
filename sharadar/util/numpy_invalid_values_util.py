"""NumPy arithmetic utilities with NaN/invalid value handling.

Provides safe wrappers around NumPy operations that suppress runtime
warnings and handle NaN, infinity, and zero-division cases gracefully
by returning NaN instead of raising errors.
"""
import warnings
import numpy as np

def nansubtract(a, b):
    """Subtract two arrays, returning NaN where b is non-finite.

    Args:
        a: Minuend array or scalar.
        b: Subtrahend array or scalar.

    Returns:
        numpy.ndarray: Result with NaN where b was non-finite.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        if np.shape(a) == np.shape(b):
            return np.subtract(a, b, out=np.full_like(b, fill_value=np.nan), where=np.isfinite(b))
        return np.subtract(a, b)

def nandivide(a, b):
    """Divide two arrays, returning NaN where divisor is zero.

    Args:
        a: Dividend array.
        b: Divisor array.

    Returns:
        numpy.ndarray: Result with NaN where b was zero.
    """
    return np.divide(a, b, out=np.full_like(a, fill_value=np.nan), where=b != 0)

def nanlog(a):
    """Compute natural logarithm, returning NaN for non-positive values.

    Args:
        a: Input array.

    Returns:
        numpy.ndarray: Result with NaN where a <= 0.
    """
    return np.log(a, out=np.full_like(a, fill_value=np.nan), where=a > 0)

def nanlog1p(a):
    """Compute log(1+x), returning NaN for non-positive values.

    Args:
        a: Input array.

    Returns:
        numpy.ndarray: Result with NaN where a <= 0.
    """
    return np.log1p(a, out=np.full_like(a, fill_value=np.nan), where=a > 0)

# Aggregations return NaN unless at least this fraction of the values is valid (not NaN).
MIN_VALID_FRACTION = 0.75


def _enough_valid(a, axis, min_valid_fraction):
    """True where at least ``min_valid_fraction`` of the values along ``axis`` are not NaN."""
    valid_count = np.sum(~np.isnan(a), axis=axis)
    total = a.size if axis is None else a.shape[axis]
    return (valid_count > 0) & (valid_count >= min_valid_fraction * total)


def _nan_aggregate(func, a, axis, min_valid_fraction):
    a = np.asarray(a, dtype=float)
    enough_valid = _enough_valid(a, axis, min_valid_fraction)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        result = func(a, axis)
    return np.where(enough_valid, result, np.nan)[()]


def nanmean(a, axis=0, min_valid_fraction=MIN_VALID_FRACTION):
    """Compute mean ignoring NaN values, suppressing warnings.

    Args:
        a: Input array.
        axis: Axis along which to compute. Defaults to 0.
        min_valid_fraction: Minimum fraction of non-NaN values, NaN otherwise.

    Returns:
        numpy.ndarray: Mean values with NaN handling.
    """
    return _nan_aggregate(np.nanmean, a, axis, min_valid_fraction)


def nanvar(a, axis=0, min_valid_fraction=MIN_VALID_FRACTION):
    """Compute variance ignoring NaN values, suppressing warnings.

    Args:
        a: Input array.
        axis: Axis along which to compute. Defaults to 0.
        min_valid_fraction: Minimum fraction of non-NaN values, NaN otherwise.

    Returns:
        numpy.ndarray: Variance values with NaN handling.
    """
    return _nan_aggregate(np.nanvar, a, axis, min_valid_fraction)


def nanmax(a, axis=0, min_valid_fraction=MIN_VALID_FRACTION):
    """Compute maximum ignoring NaN values, suppressing warnings.

    Args:
        a: Input array.
        axis: Axis along which to compute. Defaults to 0.
        min_valid_fraction: Minimum fraction of non-NaN values, NaN otherwise.

    Returns:
        numpy.ndarray: Maximum values with NaN handling.
    """
    return _nan_aggregate(np.nanmax, a, axis, min_valid_fraction)


def nanstd(a, axis=0, min_valid_fraction=MIN_VALID_FRACTION):
    """Compute standard deviation ignoring NaN values, suppressing warnings.

    Args:
        a: Input array.
        axis: Axis along which to compute. Defaults to 0.
        min_valid_fraction: Minimum fraction of non-NaN values, NaN otherwise.

    Returns:
        numpy.ndarray: Standard deviation values with NaN handling.
    """
    return _nan_aggregate(np.nanstd, a, axis, min_valid_fraction)
