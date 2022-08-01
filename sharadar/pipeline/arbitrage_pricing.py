import numpy as np
from sharadar.pipeline.engine import symbol
from sharadar.pipeline.factors import beta_residual
from sharadar.util.numpy_invalid_values_util import nanlog, nanlog1p
from zipline.pipeline import CustomFactor
from zipline.pipeline.data import USEquityPricing


class Closes(CustomFactor):
    inputs = [USEquityPricing.close]
    window_safe = True
    window_length = 1

    #__name__ = "Closes()"

    def compute(self, today, assets, out, close):
        out[:] = close[-self.window_length]


def prices_by_sid(assets, close, sid):
    rate_index = np.where((assets == sid) == True)[0][0]
    rate = np.reshape(close[:, rate_index], (-1, 1))
    return rate


class TBillBeta(CustomFactor):
    inputs = [USEquityPricing.close, Closes()[symbol('TR3M')]]
    window_safe = True
    window_length = 252

    def compute(self, today, assets, out, close, rate):
        # monthly log returns
        monthly_r = np.diff(nanlog(close[0::21, :]), axis=0)
        r = 12.*monthly_r

        # Treasury Bonds rates means every 21 daily
        t_r = nanlog1p(rate)
        t_r = np.nanmean(t_r.reshape(-1, 21), axis=1)[1:].reshape(11, 1)

        beta = beta_residual(r, t_r, standardize=True)[0]
        out[:] = beta


class TBillBondSpreadBeta(CustomFactor):
    """
    the difference in the returns to 20y and 3m government bonds,
    """
    inputs = [USEquityPricing.close]
    window_safe = True
    window_length = 252

    def compute(self, today, assets, out, close):
        # monthly log returns
        monthly_r = np.diff(nanlog(close[0::21, :]), axis=0)
        r = 12.*monthly_r

        # Treasury Bonds spreads means every 21 daily
        t_bond_30y = nanlog1p(prices_by_sid(assets, close, 10240))
        t_bill_3m = nanlog1p(prices_by_sid(assets, close, 10003))
        t_r = t_bond_30y - t_bill_3m
        t_r = np.nanmean(t_r.reshape(-1, 21), axis=1)[1:].reshape(11, 1)

        beta = beta_residual(r, t_r, standardize=True)[0]
        out[:] = beta



class CorpGvrnBondsSpreadBeta(CustomFactor):
    """
    the difference in the returns to corporate and US Treasury Bond 7 YR
    """
    inputs = [USEquityPricing.close]
    window_safe = True
    window_length = 252

    def compute(self, today, assets, out, close):
        # monthly log returns
        monthly_r = np.diff(nanlog(close[0::21, :]), axis=0)
        r = 12.*monthly_r

        # Treasury Bonds spreads means every 21 daily
        t_bond = nanlog1p(prices_by_sid(assets, close, 10084))
        c_bond = nanlog1p(prices_by_sid(assets, close, 10400))
        t_r = t_bond - c_bond
        t_r = np.nanmean(t_r.reshape(-1, 21), axis=1)[1:].reshape(11, 1)

        beta = beta_residual(r, t_r, allowed_missing=2, standardize=True)[0]
        out[:] = beta


class PurchaseManagerIndexBeta(CustomFactor):
    inputs = [USEquityPricing.close]
    window_safe = True
    window_length = 252

    def compute(self, today, assets, out, close):
        # 10430	Purchasing Managers Index
        pmi = prices_by_sid(assets, close, 10430)

        monthly_close = np.diff(nanlog(close[0::21, :]), axis=0)
        monthly_rate = np.diff(nanlog(pmi[0::21, :]), axis=0)

        beta = beta_residual(monthly_close, monthly_rate, standardize=True)[0]

        out[:] = beta


class InterestRate(CustomFactor):
    inputs = [Closes()[symbol('TR1Y')]]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, int_rate):
        out[:] = int_rate[-self.window_length]


class InflationRate(CustomFactor):
    inputs = [USEquityPricing.close]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, prices):
        # 10450	US Inflation Rates YoY
        idx = np.where((assets == 10450) == True)[0][0]
        rate = np.reshape(prices[:, idx], (-1, 1))

        out[:] = rate[-self.window_length]


class InflationRateBeta(CustomFactor):
    inputs = [
        USEquityPricing.close,
        InflationRate()
    ]
    window_safe = True
    window_length = 252

    def compute(self, today, assets, out, close, rateinf):
        monthly_close = np.diff(nanlog(close[0::21, :]), axis=0)
        monthly_rateinf = rateinf[0::21, :][1:, :]
        beta = beta_residual(monthly_close, monthly_rateinf, standardize=True)[0]

        out[:] = beta


def adjust_for_inflation(rate, interest_rate, inflation_rate):
    """
    All rates expressed as decimals: for example 2% is 0.02
    """
    return ((1.0 + rate) / (1.0 + interest_rate) - 1.0) / (1.0 + inflation_rate)