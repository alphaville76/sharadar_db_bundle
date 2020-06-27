import numpy as np
from zipline.pipeline.factors import CustomFactor
from zipline.pipeline.data import USEquityPricing

def _get_RS(series):
    incs = np.diff(series, axis=0)
    mean_inc = np.sum(incs, axis=0) / len(incs)
    deviations = incs - mean_inc
    Z = np.cumsum(deviations, axis=0)
    R = np.max(Z, axis=0) - np.min(Z, axis=0)
    S = np.std(incs, ddof=1, axis=0)

    return np.divide(R, S, out=np.zeros_like(R), where=S != 0)


def get_RS(series, window_sizes):
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
    max_window = max_window or len(series) - 1
    window_sizes = list(map(
        lambda x: int(10 ** x),
        np.arange(np.log10(min_window), np.log10(max_window), 0.25)))
    window_sizes.append(len(series))
    return window_sizes


def compute_hurst(series, min_window=10, max_window=None):
    if len(series) < 100:
        raise ValueError("Series length must be greater or equal to 100")

    window_sizes = get_window_sizes(series, min_window, max_window)
    RS = get_RS(series, window_sizes)

    A = np.vstack([np.log10(window_sizes), np.ones(len(RS))]).T
    H, c = np.linalg.lstsq(A, np.log10(RS, out=np.zeros_like(RS), where=RS != 0), rcond=-1)[0]

    return H


class Hurst(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 252
    window_safe = True

    def compute(self, today, assets, out, close):
        out[:] = compute_hurst(np.log(close))

