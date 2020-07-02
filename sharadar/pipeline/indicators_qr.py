from sharadar.pipeline.arbitrage_pricing import InterestRate, InflationRate, adjust_for_inflation
from zipline.pipeline.factors import CustomFactor
from sharadar.pipeline.factors import Fundamentals, EV


class EarningYieldEV(CustomFactor):
    inputs = [
        Fundamentals(field='netinccmnusd_arq'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, earning, ev, rateint, rateinf):
        l = self.window_length
        earning_yield_ev = earning[-l] / ev[-l]
        out[:] = adjust_for_inflation(earning_yield_ev, l, rateint, rateinf)


class BookValueYieldEV(CustomFactor):
    inputs = [
        Fundamentals(field='equityusd_arq'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, bv, ev, rateint, rateinf):
        l = self.window_length
        bv_yield_ev = bv[-l] / ev[-l]
        out[:] = adjust_for_inflation(bv_yield_ev, l, rateint, rateinf)


class CashFlowYieldEV(CustomFactor):
    inputs = [
        Fundamentals(field='ncfo_arq'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, cf, ev, rateint, rateinf):
        l = self.window_length
        cf_yield_ev = cf[-l] / ev[-l]
        out[:] = adjust_for_inflation(cf_yield_ev, l, rateint, rateinf)


class SalesYieldEV(CustomFactor):
    inputs = [
        Fundamentals(field='revenueusd_arq'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, sales, ev, rateint, rateinf):
        l = self.window_length
        cf_yield_ev = sales[-l] / ev[-l]
        out[:] = adjust_for_inflation(cf_yield_ev, l, rateint, rateinf)

