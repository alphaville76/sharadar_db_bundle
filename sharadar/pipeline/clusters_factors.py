import pandas as pd
from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.engine import load_sharadar_bundle, symbol, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
import time
import datetime
from zipline.pipeline.factors import Latest
from sharadar.pipeline.factors import MarketCap, EV, Fundamentals, FundamentalsTTM, Previous, StdDev, Beta, Sector
import numpy as np
from zipline.pipeline.factors import Returns, DailyReturns
from zipline.pipeline.factors import AverageDollarVolume
import scipy.stats as st

AT = Fundamentals(field='assets')
AT_1ya = Fundamentals(field='assets', window_length=5)

OCF = FundamentalsTTM(field='ncfo')
OCF_1ya = FundamentalsTTM(field='ncfo', window_length=2)

ocf_at = OCF/AT
ocf_at_1ya = OCF_1ya/AT_1ya
ocf_at_chg1 = ocf_at/ocf_at_1ya

sale = FundamentalsTTM(field='revenue')
sale_1ya = FundamentalsTTM(field='revenue', window_length=2)
sale_2ya = FundamentalsTTM(field='revenue', window_length=3)
inv = FundamentalsTTM(field='inventory')
inv_1ya = FundamentalsTTM(field='inventory', window_length=2)
inv_2ya = FundamentalsTTM(field='inventory', window_length=3)

dsale = 2.0*sale/(sale_1ya+sale_2ya)
dinv = 2.0*inv/(inv_1ya+inv_2ya)
dsale_dinv = dsale/dinv

NFNA = Fundamentals(field='investments') - Fundamentals(field='debt')
NFNA_1ya = Fundamentals(field='investments', window_length=5) - Fundamentals(field='debt', window_length=5)
nfna_gr1a = (NFNA - NFNA_1ya) / AT

NI = FundamentalsTTM(field='netinc')
oaccruals_ni = 1. - (OCF/NI)

CA = Fundamentals(field='assetsc')
CHE = Fundamentals(field='cashneq')
COA = CA - CHE
CL = Fundamentals(field='liabilitiesc')
COL = CL - Fundamentals(field='debtc')
COWC = COA - COL

CA_1ya = Fundamentals(field='assetsc', window_length=5)
CHE_1ya = Fundamentals(field='cashneq', window_length=5)
CL_1ya = Fundamentals(field='liabilitiesc', window_length=5)
COL_1ya = CL_1ya - Fundamentals(field='debtc', window_length=5)
COA_1ya = CA_1ya - CHE_1ya
COWC_1ya = COA_1ya - COL_1ya

cowc_gr1a = (COWC - COWC_1ya) / AT

IVAO = Fundamentals(field='investments')
NCOA = AT - CA - IVAO

LT = Fundamentals(field='liabilities')
DLTT = Fundamentals(field='debtnc')
NCOL = LT - CL - DLTT

IVAO_1ya = Fundamentals(field='investments', window_length=5)
NCOA_1ya = AT_1ya - CA_1ya - IVAO_1ya

LT_1ya = Fundamentals(field='liabilities', window_length=5)
DLTT_1ya = Fundamentals(field='debtnc', window_length=5)
NCOL_1ya = LT_1ya - CL_1ya - DLTT_1ya

OA = COA + NCOA
OA_1ya = COA_1ya + NCOA_1ya
OL = COL + NCOL
OL_1ya = COL_1ya + NCOL_1ya
NOA = OA - OL
NOA_1ya = OA_1ya - OL_1ya
noa_at = NOA/AT
noa_gr1a = (NOA - NOA_1ya) / AT

ME = MarketCap()
ME_1ya = Previous([MarketCap()], window_length=260)
FCF = Fundamentals(field='fcf')
fcf_me = FCF / ME

NNCOA = NCOA - NCOL





NNCOA_1ya = NCOA_1ya - NCOL_1ya
nncoa_gr1a = (NNCOA - NNCOA_1ya) / AT

BE = Fundamentals(field='equity')
BE_1ya = Fundamentals(field='equity', window_length=5)
INTAN = Fundamentals(field='intangibles')
ALIQ = CHE + 0.75*COA + 0.5*(AT - CA - INTAN)
MAT_1ya = AT_1ya + BE_1ya + ME_1ya
aliq_mat = ALIQ / MAT_1ya

NIQ = Fundamentals(field='netinc')
niq_be = NIQ/BE

at_be = AT/BE


class HighestNDaysReturnLastZdays(CustomFactor):
    """
    Highest 5 days return in the last 21 day
    """
    inputs = [Returns(window_length=5)]
    window_length = 21
    window_safe = False

    def compute(self, today, assets, out, ret):
        out[:] = np.nanmax(ret, axis=0)


rmax5_21d = HighestNDaysReturnLastZdays()


class TotalSkewness(CustomFactor):
    """
    Total skewness daily return in the last 21 day
    """
    inputs = [DailyReturns()]
    window_length = 21
    window_safe = False

    def compute(self, today, assets, out, ret):
        out[:] = st.skew(ret, axis=0)


rskew_21d = TotalSkewness()
rvol252d = StdDev([DailyReturns()], window_length=252)
rmax5_rvol_21d = rmax5_21d / rvol252d

OACC = NI - OCF
XRD = FundamentalsTTM(field='rnd')
EBITDA = FundamentalsTTM(field='ebitda')
COP = EBITDA + XRD - OACC
cop_at = COP/AT

cop_atl1 = COP/AT_1ya

rd_me = XRD/ME
dolvol_126d = AverageDollarVolume(window_length=126)

DIV = FundamentalsTTM(field='ncfdiv')
EQNETIS = FundamentalsTTM(field='ncfcommon')
EQNPO = DIV - EQNETIS
eqnpo_me = EQNPO/ME

ocf_me = OCF/ME


class Momentum_1M_12M(CustomFactor):
    """
    12-month closing price rate of change, excluding the most recent month.
    """
    inputs = [USEquityPricing.close]
    window_length = 252

    def compute(self, today, assets, out, close):
        out[:] = (close[-21] - close[0]) / close[0]


ret_12_1 = Momentum_1M_12M()

res_var = Beta(window_length=252, standardize=True).residual_var

pipe_columns = columns = {
    'returns': Returns(window_length=21),

    # Accruals
    'cowc_gr1a': cowc_gr1a,
    'oaccruals_ni': oaccruals_ni,

    # Debt Issuance
    'noa_at': noa_at,
    'nfna_gr1a': nfna_gr1a,

    # Investment
    'noa_gr1a': noa_gr1a,
    'nncoa_gr1a': nncoa_gr1a,

    # Leverage
    'aliq_mat': aliq_mat,
    'at_be': at_be,

    # Low Risk
    'fcf_me': fcf_me,
    'rmax5_21d': rmax5_21d,

    # Momentum
    'res_var': res_var,
    'ret_12_1': ret_12_1,

    # Profit Growth
    'dsale_dinv': dsale_dinv,
    'ocf_at_chg1': ocf_at_chg1,

    # Profitability
    'ocf_at': ocf_at,
    'niq_be': niq_be,

    # Quality
    'cop_at': cop_at,
    'cop_atl1': cop_atl1,

    # Size
    'rd_me': rd_me,
    'dolvol_126d': dolvol_126d,

    # Skewness
    'rmax5_rvol_21d': rmax5_rvol_21d,
    'rskew_21d': rskew_21d,

    # Value
    'eqnpo_me': eqnpo_me,
    'ocf_me': ocf_me,

    'sector': Sector()
}

if __name__ == '__main__':
    engine = make_pipeline_engine()
    date = pd.to_datetime('2021-05-20', utc=True)

    pipe = Pipeline(pipe_columns,
                    screen=StaticAssets(symbols(['IBM', 'F', 'AAPL']))
                    )

    stocks = engine.run_pipeline(pipe, date, hooks=[])
    print(stocks.T)
