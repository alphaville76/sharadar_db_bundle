"""Trailing Twelve Month (TTM) indicators using annual trailing (ART) data.

This module provides pipeline factors that compute yield metrics relative to
enterprise value (EV), adjusted for inflation. All factors use trailing twelve
month data (ART suffix fields from Sharadar fundamentals) and nandivide for
NaN-safe division.
"""
from sharadar.pipeline.arbitrage_pricing import InterestRate, InflationRate, adjust_for_inflation
from sharadar.util.numpy_invalid_values_util import nandivide
from zipline.pipeline.factors import CustomFactor
from sharadar.pipeline.factors import Fundamentals, EV


class EarningYieldEV(CustomFactor):
    """Earning yield relative to enterprise value, adjusted for inflation.

    Computes net income common (USD, TTM) divided by EV using NaN-safe
    division, then adjusts for inflation.
    """
    inputs = [
        Fundamentals(field='netinccmnusd_art'),
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
            earning: Net income common (USD, TTM).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        earning_yield_ev = nandivide(earning[-l], ev[-l])
        out[:] = adjust_for_inflation(earning_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)


class BookValueYieldEV(CustomFactor):
    """Book value yield relative to enterprise value, adjusted for inflation.

    Computes shareholder equity (USD, TTM) divided by EV using NaN-safe
    division, then adjusts for inflation.
    """
    inputs = [
        Fundamentals(field='equityusd_art'),
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
            bv: Shareholder equity (USD, TTM).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        bv_yield_ev = nandivide(bv[-l], ev[-l])
        out[:] = adjust_for_inflation(bv_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)


class CashFlowYieldEV(CustomFactor):
    """Cash flow yield relative to enterprise value, adjusted for inflation.

    Computes net cash flow from operations (TTM) divided by EV using NaN-safe
    division, then adjusts for inflation.
    """
    inputs = [
        Fundamentals(field='ncfo_art'),
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
            cf: Net cash flow from operations (TTM).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        cf_yield_ev = nandivide(cf[-l], ev[-l])
        out[:] = adjust_for_inflation(cf_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)


class FreeCashFlowYieldEV(CustomFactor):
    """Free cash flow yield relative to enterprise value, adjusted for inflation.

    Computes free cash flow (TTM) divided by EV using NaN-safe division,
    then adjusts for inflation.
    """
    inputs = [
        Fundamentals(field='fcf_art'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, cf, ev, rateint, rateinf):
        """Compute inflation-adjusted free cash flow yield relative to EV.

        Args:
            today: The current simulation date.
            assets: Array of asset identifiers.
            out: Output array to write results into.
            cf: Free cash flow (TTM).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        cf_yield_ev = nandivide(cf[-l], ev[-l])
        out[:] = adjust_for_inflation(cf_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)



class SalesYieldEV(CustomFactor):
    """Sales yield relative to enterprise value, adjusted for inflation.

    Computes revenue (USD, TTM) divided by EV using NaN-safe division,
    then adjusts for inflation.
    """
    inputs = [
        Fundamentals(field='revenueusd_art'),
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
            sales: Revenue (USD, TTM).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        cf_yield_ev = nandivide(sales[-l], ev[-l])
        out[:] = adjust_for_inflation(cf_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)


class SalesYieldNoUSDEV(CustomFactor):
    """Sales yield (native currency) relative to enterprise value, adjusted for inflation.

    Computes revenue in native currency (TTM) divided by EV using NaN-safe
    division, then adjusts for inflation. Unlike SalesYieldEV, this does not
    convert revenue to USD before computing the yield.
    """
    inputs = [
        Fundamentals(field='revenue_art'),
        EV(),
        InterestRate(),
        InflationRate()
    ]
    window_safe = True
    window_length = 1

    def compute(self, today, assets, out, sales, ev, rateint, rateinf):
        """Compute inflation-adjusted sales yield (native currency) relative to EV.

        Args:
            today: The current simulation date.
            assets: Array of asset identifiers.
            out: Output array to write results into.
            sales: Revenue in native currency (TTM).
            ev: Enterprise value.
            rateint: Interest rate (percentage).
            rateinf: Inflation rate (percentage).
        """
        l = self.window_length
        cf_yield_ev = nandivide(sales[-l], ev[-l])
        out[:] = adjust_for_inflation(cf_yield_ev, rateint[-l]/100.0, rateinf[-l]/100.0)
