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

def nanmean(a, axis=0):
    """Compute mean ignoring NaN values, suppressing warnings.

    Args:
        a: Input array.
        axis: Axis along which to compute. Defaults to 0.

    Returns:
        numpy.ndarray: Mean values with NaN handling.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanmean(a, axis)

def nanvar(a, axis=0):
    """Compute variance ignoring NaN values, suppressing warnings.

    Args:
        a: Input array.
        axis: Axis along which to compute. Defaults to 0.

    Returns:
        numpy.ndarray: Variance values with NaN handling.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanvar(a, axis)

def nanstd(a, axis=0):
    """Compute standard deviation ignoring NaN values, suppressing warnings.

    Args:
        a: Input array.
        axis: Axis along which to compute. Defaults to 0.

    Returns:
        numpy.ndarray: Standard deviation values with NaN handling.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanstd(a, axis)
