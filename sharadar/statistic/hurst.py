"""Hurst exponent estimation via rescaled range (R/S) analysis.

The Hurst exponent H characterizes the long-term memory of a time series:
- H ~ 0.5: random walk (no memory)
- H > 0.5: trending (persistent) behavior
- H < 0.5: mean-reverting (anti-persistent) behavior

Provides both standalone functions and a zipline CustomFactor for pipeline use.
"""
import numpy as np
from zipline.pipeline.factors import CustomFactor
from zipline.pipeline.data import USEquityPricing

def _get_RS(series):
    """Compute the rescaled range (R/S) statistic for a single window.

    Args:
        series: 1-D or 2-D numpy array of price/log-price values.

    Returns:
        R/S statistic as a scalar or array (one per column).
    """
    incs = np.diff(series, axis=0)
    mean_inc = np.sum(incs, axis=0) / len(incs)
    deviations = incs - mean_inc
    Z = np.cumsum(deviations, axis=0)
    R = np.max(Z, axis=0) - np.min(Z, axis=0)
    S = np.std(incs, ddof=1, axis=0)

    return np.divide(R, S, out=np.zeros_like(R), where=S != 0)


def get_RS(series, window_sizes):
    """Compute average R/S statistics across multiple window sizes.

    For each window size, splits the series into non-overlapping segments,
    computes R/S for each segment, and averages.

    Args:
        series: Input time series array.
        window_sizes: List of integer window sizes to evaluate.

    Returns:
        Numpy array of mean R/S values, one per window size.
    """
    RS = []

    for w in window_sizes:
        rs = []
        for start in range(0, len(series), w):
            if (start + w) > len(series):
                break
            rs.append(_get_RS(series[start:start + w]))
        RS.append(np.nanmean(rs, axis=0))

    return np.array(RS)


def get_window_sizes(series, min_window=10, max_window=None):
    """Generate logarithmically spaced window sizes for R/S analysis.

    Args:
        series: Input series (used to determine maximum window).
        min_window: Minimum window size. Default is 10.
        max_window: Maximum window size. Defaults to len(series) - 1.

    Returns:
        List of integer window sizes.
    """
    max_window = max_window or len(series) - 1
    window_sizes = list(map(
        lambda x: int(10 ** x),
        np.arange(np.log10(min_window), np.log10(max_window), 0.25)))
    window_sizes.append(len(series))
    return window_sizes


def compute_hurst(series, min_window=10, max_window=None):
    """Compute the Hurst exponent via rescaled range analysis.

    Estimates H by regressing log(R/S) against log(window_size).

    Args:
        series: Input time series (log-prices recommended), length >= 100.
        min_window: Minimum window size for R/S computation.
        max_window: Maximum window size. Defaults to len(series) - 1.

    Returns:
        Hurst exponent H (scalar or array if series is 2-D).

    Raises:
        ValueError: If series length is less than 100.
    """
    if len(series) < 100:
        raise ValueError("Series length must be greater or equal to 100")

    window_sizes = get_window_sizes(series, min_window, max_window)
    RS = get_RS(series, window_sizes)

    A = np.vstack([np.log10(window_sizes), np.ones(len(RS))]).T
    H, c = np.linalg.lstsq(A, np.log10(RS, out=np.zeros_like(RS), where=RS != 0), rcond=-1)[0]

    return H


class Hurst(CustomFactor):
    """Zipline CustomFactor that computes the Hurst exponent.

    Computes the Hurst exponent of log-prices over the trailing window
    for each asset in the pipeline.

    Attributes:
        inputs: Uses USEquityPricing.close.
        window_length: Default lookback of 252 trading days.
    """

    inputs = [USEquityPricing.close]
    window_length = 252
    window_safe = True

    def compute(self, today, assets, out, close):
        out[:] = compute_hurst(np.log(close))

