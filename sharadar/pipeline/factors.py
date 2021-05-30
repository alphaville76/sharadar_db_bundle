import warnings

import numpy as np
import pandas as pd
from sharadar.pipeline.engine import BundleLoader, symbol
from zipline.lib.labelarray import LabelArray
from zipline.pipeline.classifiers import CustomClassifier
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import CustomFactor, DailyReturns
from zipline.utils.numpy_utils import object_dtype
from zipline.pipeline.factors import AverageDollarVolume
from sharadar.pipeline.engine import history, returns
from sharadar.util.logger import log

def nanmean(a, axis=0):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanmean(a, axis)


def nanvar(a, axis=0):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanvar(a, axis)


def nanstd(a, axis=0):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanstd(a, axis)


class Fundamentals(CustomFactor, BundleLoader):
    inputs = []
    window_length = 1
    params = ('field',)
    window_safe = True

    def compute(self, today, assets, out, field):
        field_name = field
        if '_' not in field_name:
            field_name += '_arq'
        out[:] = self.asset_finder().get_fundamentals(assets, field_name, today, n=self.window_length)

    def __str__(self):
        return "Fundamentals" + str(self.params)


class FundamentalsTTM(Fundamentals):
    def compute(self, today, assets, out, field):
        out[:] = self.asset_finder().get_fundamentals_ttm(assets, field, today, k=self.window_length)


class DaysSinceFiling(CustomFactor, BundleLoader):
    inputs = []
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out):
        datekeys = self.asset_finder().get_datekey(assets, today, n=self.window_length)
        # timestamp value is in nanoseconds
        out[:] = (today.value - datekeys) / (24 * 60 * 60 * 1e9)

    def __str__(self):
        return "DaysSinceFiling"


class AbstractClassifier(CustomClassifier, BundleLoader):
    inputs = []
    window_length = 1
    dtype = object_dtype
    missing_value = 'NA'

    def __init__(self, categories, field):
        self.categories = categories
        self.field = field

    def _allocate_output(self, windows, shape):
        return LabelArray(np.full(shape, self.missing_value), self.missing_value, categories=self.categories)

    def compute(self, today, assets, out, *arrays):
        data = self.asset_finder().get_info(assets, self.field, today)
        out[:] = LabelArray(data, self.missing_value, categories=self.categories)


class Exchange(AbstractClassifier):
    def __init__(self):
        categories = ['NYSE', 'NASDAQ', 'OTC', 'NYSEMKT', 'NYSEARCA', 'BATS']
        field = 'exchange'
        super().__init__(categories, field)


class Sector(AbstractClassifier):
    def __init__(self):
        categories = ['Healthcare', 'Basic Materials', 'Financial Services', 'Consumer Cyclical', 'Technology',
                      'Consumer Defensive', 'Industrials', 'Real Estate', 'Energy', 'Communication Services',
                      'Utilities']
        field = 'sector'
        super().__init__(categories, field)


class IsDomesticCommonStock(CustomClassifier, BundleLoader):
    inputs = []
    window_length = 1
    dtype = np.int64
    missing_value = 0

    def compute(self, today, assets, out, *arrays):
        category = self.asset_finder().get_info(assets, 'category', today)
        out[:] = np.isin(category, ['Domestic Common Stock', 'Domestic Common Stock Primary Class',
                                    'Domestic Common Stock Secondary Class', 'Domestic Preferred Stock'])


class IsBankruptcy(CustomClassifier, BundleLoader):
    """
    The 5th letter "Q" stand for bankruptcy.
    The NASDAQ phased out the usage of Q as of 2016, but other markets may still use "Q" for this purpose.
    """
    inputs = []
    window_length = 1
    dtype = np.int64
    missing_value = -1

    def compute(self, today, assets, out, *arrays):
        equities = self.asset_finder().retrieve_all(assets)
        out[:] = [((len(e.symbol) == 5) & e.symbol.endswith('Q')) for e in equities]


class IsDelinquent(CustomClassifier, BundleLoader):
    """
    The 5th letter "E" stand for delinquent in regard to SEC filings.
    The NASDAQ phased out the usage of E as of 2016, but other markets may still use "E" for this purpose.
    """
    inputs = []
    window_length = 1
    dtype = np.int64
    missing_value = -1

    def compute(self, today, assets, out, *arrays):
        equities = self.asset_finder().retrieve_all(assets)
        out[:] = [((len(e.symbol) == 5) & e.symbol.endswith('E')) for e in equities]


def get_daily_metrics(asset_finder, assets, field, today, n, mult=1):
    metric = mult * asset_finder.get_daily_metrics(assets, field, today, n)
    if np.isnan(metric).all():
        # If all NaN (not ingested because delay in computation in SEP) then use the data of the previous day
        log.warn("No data for %s on %s. Use data from previous day." % (field, today.date()))
        metric = mult * asset_finder.get_daily_metrics(assets, field, today, n + 1)[0, :]
    return metric

class MarketCap(CustomFactor, BundleLoader):
    inputs = []
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out):
        out[:] = get_daily_metrics(self.asset_finder(), assets, 'marketcap', today, self.window_length, 1e6)

    def __str__(self):
        return "MarketCap(%d)" % self.window_length

class EV(CustomFactor, BundleLoader):
    inputs = []
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out):
        out[:] = out[:] = get_daily_metrics(self.asset_finder(), assets, 'ev', today, self.window_length, 1e6)

    def __str__(self):
        return "EV(%d)" % self.window_length

class EvEbit(CustomFactor, BundleLoader):
    inputs = []
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out):
        out[:] = get_daily_metrics(self.asset_finder(), assets, 'evebit', today, self.window_length)

    def __str__(self):
        return "EvEbit(%d)" % self.window_length


class EvEbitda(CustomFactor, BundleLoader):
    inputs = []
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out):
        out[:] = get_daily_metrics(self.asset_finder(), assets, 'evebitda', today, self.window_length)

    def __str__(self):
        return "EvEbitda(%d)" % self.window_length


class PriceBook(CustomFactor, BundleLoader):
    inputs = []
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out):
        out[:] = get_daily_metrics(self.asset_finder(), assets, 'pb', today, self.window_length)

    def __str__(self):
        return "PriceBook(%d)" % self.window_length


class PriceEarnings(CustomFactor, BundleLoader):
    inputs = []
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out):
        out[:] = get_daily_metrics(self.asset_finder(), assets, 'pe', today, self.window_length)

    def __str__(self):
        return "PriceEarnings(%d)" % self.window_length


class PriceSales(CustomFactor, BundleLoader):
    inputs = []
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out):
        out[:] = get_daily_metrics(self.asset_finder(), assets, 'ps', today, self.window_length)

    def __str__(self):
        return "PriceSales(%d)" % self.window_length


def time_trend(Y, allowed_missing=0):
    """
    If 'allowed_missing' is zero, interpolate to fill the NaN.
    If all values are NaN, replace them with zero
    """
    if allowed_missing == 0:
        # interpolate is too slow for the Algo Platform
        # Y = pd.DataFrame(Y).interpolate().fillna(method='bfill').fillna(0)
        Y = pd.DataFrame(Y).fillna(method='ffill', axis=0).fillna(0)
    n = Y.shape[0]
    m = Y.shape[1]
    # idx: n-1 to 0; chronological order: from the oldest to the most recent observation
    idx = np.arange(start=(n - 1), stop=-1, step=-1, dtype=float)
    X = np.full((m, n), idx).T
    # shape: (N, M)
    X = np.where(np.isnan(Y), np.nan, X)

    X_mean = nanmean(X, axis=0)
    Y_mean = nanmean(Y, axis=0)

    # shape: (M,)
    XY_cov = nanmean((X - X_mean) * (Y - Y_mean), axis=0)

    X_var = nanvar(X, axis=0)

    # shape: (M,)
    beta = np.divide(XY_cov, X_var)
    alpha = Y_mean - beta * X_mean
    Y_est = alpha + np.multiply(beta, X)
    residual = Y - Y_est
    s2 = np.nansum(residual ** 2, axis=0) / (n - 2.0)
    std_err2 = s2 / (n * X_var)
    std_err = np.sqrt(std_err2)

    # Write nans back to locations where we have more
    # then allowed number of missing entries.
    nanlocs = np.isnan(X).sum(axis=0) > allowed_missing
    beta[nanlocs] = np.nan
    # alpha[nanlocs] = nan
    std_err[nanlocs] = np.nan

    # return (alpha, beta)
    return (beta, std_err)


class FundamentalsTrend(CustomFactor, BundleLoader):
    inputs = []
    outputs = ['trend', 'std_err']
    # 20 quarters = 5 years
    # 12 quarters = 3 years
    window_length = 12
    params = ('field',)

    def retrieve_data(self, assets, field, today):
        field_name = field
        if '_' not in field_name:
            field_name += '_arq'
        y = self.asset_finder().get_fundamentals_df_window_length(assets, field_name, today, self.window_length)
        return y

    def compute(self, today, assets, out, field):
        y = self.retrieve_data(assets, field, today)

        (out.trend, out.std_err) = time_trend(y)


# to avoid divide by zero
def _robust(x, fn):
    if np.isscalar(x):
        return fn(x) if x != 0.0 else 0.0

    x1 = np.copy(x)
    idx = np.nonzero(x1)
    x1[idx] = fn(x[idx])
    return x1


def _logscale(x):
    # Given: y=log(1+x), y≈x when x is small (less than 1).
    return np.sign(x) * np.log(np.abs(x + np.sign(x)))


# to avoid divide by zero
def logscale(x):
    return _robust(x, _logscale)


class LogFundamentalsTrend(FundamentalsTrend):
    def compute(self, today, assets, out, field):
        data = self.retrieve_data(assets, field, today)
        y = logscale(data)

        (out.trend, out.std_err) = time_trend(y)

        # The arctan of a slope is the the angle θ with the origin between −π/2 and π/2
        # Then divide by π/2 to get a measure in [-1,1]
        # Then add π/2 and divide by π to get a measure in [0,1]
        # out.trend = 2.0 * np.arctan(out.trend) / np.pi
        out.trend = (np.arctan(out.trend) + np.pi / 2) / np.pi


class TimeTrend(CustomFactor):
    outputs = ['trend', 'std_err']
    window_length = 756
    params = ('periodic',)

    def compute(self, today, assets, out, data, periodic):
        (out.trend, out.std_err) = time_trend(data[list(periodic)])


class LogTimeTrend(TimeTrend):
    def compute(self, today, assets, out, data, periodic):
        y = logscale(data[list(periodic)])

        (out.trend, out.std_err) = time_trend(y)

        # The arctan of a slope is the the angle θ with the origin between −π/2 and π/2
        # Then divide by π/2 to get a measure in [-1,1]
        # Then add π/2 and divide by π to get a measure in [0,1]
        # out.trend = 2.0 * np.arctan(out.trend) / np.pi
        out.trend = (np.arctan(out.trend) + np.pi / 2) / np.pi


class LogLatest(CustomFactor):
    window_length = 1

    def compute(self, today, assets, out, data):
        out[:] = logscale(data[-1])


class StdDev(CustomFactor):
    window_length = 252

    def compute(self, today, assets, out, factor):
        out[:] = nanstd(factor)


def beta_residual(Y, X, allowed_missing=0, standardize=False):
    """
    Compute slopes of linear regressions between columns of ``Y`` and
    ``X``.
    Parameters
    ----------
    Y : np.array[N, M]
        Array with columns of data to be regressed against ``X``.
    X : np.array[N, 1]
        X variable of the regression
    allowed_missing : int
        Number of allowed missing (NaN) observations per column. Columns with
        more than this many non-nan observations in both ``Y`` and
        ``inY`` will output NaN as the regression coefficient.
    Returns
    -------
    slopes : np.array[M]
        Linear regression coefficients for each column of ``Y``.
    variance of residuals : np.array[M]
    """
    if standardize:
        Y = (Y - nanmean(Y, axis=0)) / nanstd(Y, axis=0)
        X = (X - nanmean(X, axis=0)) / nanstd(X, axis=0)

    # shape: (N, M)
    X = np.where(np.isnan(Y), np.nan, X)

    X_residual = X - nanmean(X, axis=0)

    # shape: (M,)
    covariances = nanmean(X_residual * Y, axis=0)

    X_variances = nanmean(X_residual ** 2, axis=0)

    # shape: (M,)
    beta = np.divide(covariances, X_variances)

    Y_est = np.multiply(beta, X)
    residual = Y - Y_est
    residual_var = nanvar(residual, axis=0)

    # Write nans back to locations where we have more
    # then allowed number of missing entries.
    nanlocs = np.isnan(X).sum(axis=0) > allowed_missing
    beta[nanlocs] = np.nan
    residual_var[nanlocs] = np.nan

    return (beta, residual_var)


class Beta(CustomFactor):
    outputs = ['beta', 'residual_var']
    inputs = [DailyReturns(), DailyReturns()[symbol('SPY')]]
    window_length = 252
    params = ('standardize',)

    def compute(self, today, assets, out, assets_returns, market_returns, standardize):
        allowed_missing_percentage = 0.25
        allowed_missing_count = int(allowed_missing_percentage * self.window_length)
        (out.beta, out.residual_var) = beta_residual(assets_returns, market_returns, allowed_missing_count, standardize)


class Previous(CustomFactor):
    def compute(self, today, assets, out, data):
        index = -self.window_length
        out[:] = data[index]


class ExcessReturn(CustomFactor):
    """
    Excess returns are computed as the difference between the trailing
    rate of return to the stock and the trailing return to the S&P 500 stock index

    SPY (Spdr S&P 500 Etf Trust) sid 118691
    """
    inputs = [USEquityPricing.close]

    def compute(self, today, assets, out, assets_close):
        market_index = np.where((assets == 118691) == True)[0][0]
        market_close = assets_close[:, market_index]

        assets_returns = (assets_close[-1] / assets_close[0]) - 1.0
        market_returns = (market_close[-1] / market_close[0]) - 1.0
        out[:] = assets_returns - market_returns


class MonthlyDollarVolume(CustomFactor):
    """
    Average Daily Dollar Volume over the trailing month
    """
    inputs = [USEquityPricing.close, USEquityPricing.volume]

    window_length = 20
    window_safe = True

    def compute(self, today, assets, out, close, volume):
        out[:] = np.nansum(close * volume, axis=0) / len(close)


class TradingVolume(CustomFactor):
    """
    Trading volume is computed as the total dollar amount of trading in the
    stock over the trailing month as a percent of total market capitalization.
    """
    #inputs = [MonthlyDollarVolume(), MarketCap()]
    inputs = [AverageDollarVolume(window_length=20), MarketCap()]

    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, monthly_dollar_volume, market_cap):
        length = self.window_length
        out[:] = monthly_dollar_volume[-length] / market_cap[-length]


class InvestmentToAssets(CustomFactor, BundleLoader):
    """
    Measured as asset growth YOY (Lu Zhang - q-factors and Investment CAPM)

    """
    inputs = []
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out):
        l = self.window_length
        assets_t = self.asset_finder().get_fundamentals(assets, 'assets_art', today, n=l)
        assets_t_minus_1 = self.asset_finder().get_fundamentals(assets, 'assets_art', today, n=(l + 4))
        out[:] = assets_t / assets_t_minus_1 - 1.0


def shift(arr, num, fill_value=np.nan):
    result = np.empty_like(arr)
    if num > 0:
        result[:num] = fill_value
        result[num:] = arr[:-num]
    elif num < 0:
        result[num:] = fill_value
        result[:num] = arr[-num:]
    else:
        result[:] = arr
    return result


class InvestmentToAssetsTrend(CustomFactor, BundleLoader):
    """
    Trend of InvestmentToAssets, measured as asset growth YOY (Lu Zhang - q-factors and Investment CAPM)

    """
    # 5 years
    window_length = 20
    inputs = []
    window_safe = True
    outputs = ['trend', 'std_err']

    def compute(self, today, assets, out):
        ta = self.asset_finder().get_fundamentals_df_window_length(assets, 'assets_art', today, self.window_length + 4)
        ta_log = logscale(ta)
        ta_log_py = shift(ta_log, -4)
        # flip to get chronological order
        ia = np.flip(ta_log - ta_log_py, axis=0)[4:]

        (out.trend, out.std_err) = time_trend(ia)
        out.trend = 2.0 * np.arctan(out.trend) / np.pi


class ForwardsReturns(CustomFactor, BundleLoader):
    """
    Calculates the percent change in close price over the given window_length in the future.
    Only for research purposes.
    """
    inputs = []
    window_safe = True

    def compute(self, today, assets, out):
        end_dt = self.bar_reader().trading_calendar.sessions_window(today, self.window_length)[-1]

        out[:] = returns(assets, today, end_dt)
