"""Foreign exchange rate reader for the Zipline pipeline.

This module provides a simplified FX rate reader that bypasses currency
conversion by always returning a rate of 1.0, effectively treating all
assets as if they are denominated in the same currency.
"""
from interface import implements
import numpy as np
from zipline.data.fx.base import FXRateReader


class SimpleFXRateReader(implements(FXRateReader)):
    """An FXRateReader that bypasses currency conversion.

    Returns a rate of 1.0 for all currency pairs, effectively assuming all
    values are already in the desired quote currency. Useful when working
    with datasets that are pre-converted to a single currency.
    """

    def get_rates(self, rate, quote, bases, dts):
        """Return exchange rates of 1.0 for all requested currency pairs.

        Args:
            rate: The type of exchange rate to retrieve (e.g., spot, mid).
            quote: The target quote currency code.
            bases: Array of base currency codes to convert from.
            dts: Array of datetime values for which rates are requested.

        Returns:
            A numpy array of ones with shape (len(dts), len(bases)),
            indicating no currency conversion is applied.
        """
        return np.ones((len(dts), len(bases)))
