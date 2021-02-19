import pandas as pd
from pandas.tseries.offsets import MonthEnd

class Blacklist:
    def __init__(self, expires_in_months=2):
        self.n = expires_in_months
        self.s = pd.Series()

    def add_symbols(self, symbols, date):
        for symbol in symbols:
            self.s.loc[symbol] = date + MonthEnd(self.n + 1)

    def get_symbols(self, date=None):
        if date is not None:
            self.s = self.s.loc[self.s > date]
        return self.s.index