"""Stock blacklist utilities for trading algorithms.

Provides a time-expiring blacklist mechanism to temporarily exclude
stocks from the trading universe (e.g., after a loss event).
"""
import pandas as pd
from pandas.tseries.offsets import MonthEnd

class Blacklist:
    """A time-expiring stock blacklist.

    Maintains a set of blacklisted stock symbols that automatically
    expire after a configurable number of months.

    Attributes:
        n: Number of months before a blacklist entry expires.
        s: Series mapping symbols to their expiration dates.
    """
    def __init__(self, expires_in_months=2):
        self.n = expires_in_months
        self.s = pd.Series()

    def add_symbols(self, symbols, date):
        """Add symbols to the blacklist with expiration based on date.

        Args:
            symbols: Iterable of ticker symbols to blacklist.
            date: The date of the blacklist event.
        """
        for symbol in symbols:
            self.s.loc[symbol] = date + MonthEnd(self.n + 1)

    def get_symbols(self, date=None):
        """Get currently active blacklisted symbols.

        Args:
            date: If provided, prunes expired entries before returning.

        Returns:
            pd.Index: Index of currently blacklisted ticker symbols.
        """
        if date is not None:
            self.s = self.s.loc[self.s > date]
        return self.s.index