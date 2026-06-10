import pandas as pd
import pytest
from sharadar.util.blacklist import Blacklist


class TestBlacklist:
    def test_init(self):
        bl = Blacklist(1)
        assert bl is not None

    def test_add_and_get_symbols_within_expiry(self):
        bl = Blacklist(1)
        bl.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
        symbols = bl.get_symbols(pd.to_datetime('2020-02-15'))
        assert len(symbols) == 3
        assert 'IBM' in symbols
        assert 'F' in symbols
        assert 'AAPL' in symbols

    def test_symbols_expire_after_months(self):
        bl = Blacklist(1)
        bl.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
        symbols = bl.get_symbols(pd.to_datetime('2020-05-10'))
        assert len(symbols) == 0

    def test_multiple_additions(self):
        bl = Blacklist(1)
        bl.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
        bl.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))
        symbols = bl.get_symbols(pd.to_datetime('2020-03-10'))
        assert len(symbols) == 5

    def test_partial_expiry(self):
        bl = Blacklist(1)
        bl.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
        bl.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))
        symbols = bl.get_symbols(pd.to_datetime('2020-04-10'))
        assert len(symbols) == 2

    def test_2_month_expiry(self):
        bl = Blacklist(2)
        bl.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
        bl.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))
        assert len(bl.get_symbols(pd.to_datetime('2020-04-10'))) == 5
        assert len(bl.get_symbols(pd.to_datetime('2020-05-10'))) == 2
        assert len(bl.get_symbols(pd.to_datetime('2020-06-10'))) == 0

    def test_3_month_expiry(self):
        bl = Blacklist(3)
        bl.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
        bl.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))
        assert len(bl.get_symbols(pd.to_datetime('2020-04-10'))) == 5
        assert len(bl.get_symbols(pd.to_datetime('2020-05-10'))) == 5
        assert len(bl.get_symbols(pd.to_datetime('2020-06-10'))) == 2
        assert len(bl.get_symbols(pd.to_datetime('2020-07-10'))) == 0

    def test_empty_blacklist(self):
        bl = Blacklist(1)
        symbols = bl.get_symbols(pd.to_datetime('2020-01-01'))
        assert len(symbols) == 0

    def test_get_symbols_default_date(self):
        bl = Blacklist(12)
        bl.add_symbols(['TEST'], pd.to_datetime('today'))
        symbols = bl.get_symbols()
        assert len(symbols) == 1
