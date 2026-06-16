"""Quality/Risk indicators using quarterly (ARQ) fundamental data.

This module provides pipeline factors that compute yield metrics relative to
enterprise value (EV), adjusted for inflation. All factors use the most recent
quarterly filing data (ARQ suffix fields from Sharadar fundamentals).
"""
from sharadar.pipeline.arbitrage_pricing import InterestRate, InflationRate, adjust_for_inflation
from zipline.pipeline.factors import CustomFactor
from sharadar.pipeline.factors import Fundamentals, EV


class EarningYieldEV(CustomFactor):
    """Earning yield relative to enterprise value, adjusted for inflation.

    Computes net income common (USD, quarterly) divided by EV,
    then adjusts for inflation using interest and inflation rates.
    """
    inputs = [
        Fundamentals(field='netinccmnusd_arq'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, earning, ev, rateint, rateinf):
        """Compute inflation-adjusted earning yield relative to EV.

        Args:
            today: The current simulation date.
            assets: Array of asset identifiers.
            out: Output array to write results into.
            earning: Net income common (USD, quarterly).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        earning_yield_ev = earning[-l] / ev[-l]
        out[:] = adjust_for_inflation(earning_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)


class BookValueYieldEV(CustomFactor):
    """Book value yield relative to enterprise value, adjusted for inflation.

    Computes shareholder equity (USD, quarterly) divided by EV,
    then adjusts for inflation using interest and inflation rates.
    """
    inputs = [
        Fundamentals(field='equityusd_arq'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, bv, ev, rateint, rateinf):
        """Compute inflation-adjusted book value yield relative to EV.

        Args:
            today: The current simulation date.
            assets: Array of asset identifiers.
            out: Output array to write results into.
            bv: Shareholder equity (USD, quarterly).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        bv_yield_ev = bv[-l] / ev[-l]
        out[:] = adjust_for_inflation(bv_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)


class CashFlowYieldEV(CustomFactor):
    """Cash flow yield relative to enterprise value, adjusted for inflation.

    Computes net cash flow from operations (quarterly) divided by EV,
    then adjusts for inflation using interest and inflation rates.
    """
    inputs = [
        Fundamentals(field='ncfo_arq'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, cf, ev, rateint, rateinf):
        """Compute inflation-adjusted cash flow yield relative to EV.

        Args:
            today: The current simulation date.
            assets: Array of asset identifiers.
            out: Output array to write results into.
            cf: Net cash flow from operations (quarterly).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        cf_yield_ev = cf[-l] / ev[-l]
        out[:] = adjust_for_inflation(cf_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)


class SalesYieldEV(CustomFactor):
    """Sales yield relative to enterprise value, adjusted for inflation.

    Computes revenue (USD, quarterly) divided by EV,
    then adjusts for inflation using interest and inflation rates.
    """
    inputs = [
        Fundamentals(field='revenueusd_arq'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, sales, ev, rateint, rateinf):
        """Compute inflation-adjusted sales yield relative to EV.

        Args:
            today: The current simulation date.
            assets: Array of asset identifiers.
            out: Output array to write results into.
            sales: Revenue (USD, quarterly).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        cf_yield_ev = sales[-l] / ev[-l]
        out[:] = adjust_for_inflation(cf_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)
