import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.factors import (
    MarketCap,
    EV,
    Fundamentals,
    LogFundamentalsTrend,
    LogTimeTrend,
    InvestmentToAssets
)
from sharadar.pipeline.engine import load_sharadar_bundle, symbol, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
from zipline.pipeline.factors import CustomFactor
import numpy as np

class EarningYield(CustomFactor):
    inputs = [
        Fundamentals(field='netinccmnusd_art'),
        MarketCap()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, earnings, mkt_cap):
        l = self.window_length
        out[:] = earnings[-l] / mkt_cap[-l]


month_length = 21
monthly = tuple(np.append(np.arange(-11 * month_length, 0, step=month_length), -1))

universe = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
pipe = Pipeline(columns={
    'revenue': Fundamentals(field='revenue', window_length=1),
    'revenue_trend': LogFundamentalsTrend(field='revenue', mask=universe).trend,
    'ey': EarningYield(),
    'ey_trend': LogTimeTrend([EarningYield()], periodic=monthly, mask=universe).trend,
},
screen = universe
)

engine = make_pipeline_engine()
pipe_date = pd.to_datetime('2017-09-07', utc=True)
stocks = engine.run_pipeline(pipe, pipe_date)
print(stocks)

#NO PERIODIC
#                                                      ey  ey_trend  \
#2017-09-07 00:00:00+00:00 Equity(199059 [AAPL])  0.055782  0.000066
#                          Equity(199623 [IBM])   0.085316 -0.000013
#                          Equity(199713 [F])     0.083262  0.000290