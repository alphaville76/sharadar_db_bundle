import pandas as pd
from sharadar.util.blacklist import Blacklist


def test_blacklist_1_month():
    blacklist = Blacklist(1)
    blacklist.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
    blacklist.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))

    assert len(blacklist.get_symbols(pd.to_datetime('2020-03-10'))) == 5
    assert len(blacklist.get_symbols(pd.to_datetime('2020-04-10'))) == 2
    assert len(blacklist.get_symbols(pd.to_datetime('2020-05-10'))) == 0


def test_blacklist_2_months():
    blacklist = Blacklist(2)
    blacklist.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
    blacklist.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))

    assert len(blacklist.get_symbols(pd.to_datetime('2020-03-10'))) == 5
    assert len(blacklist.get_symbols(pd.to_datetime('2020-04-10'))) == 5
    assert len(blacklist.get_symbols(pd.to_datetime('2020-05-10'))) == 2
    assert len(blacklist.get_symbols(pd.to_datetime('2020-06-10'))) == 0


def test_blacklist_3_months():
    blacklist = Blacklist(3)
    blacklist.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
    blacklist.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))

    assert len(blacklist.get_symbols(pd.to_datetime('2020-03-10'))) == 5
    assert len(blacklist.get_symbols(pd.to_datetime('2020-04-10'))) == 5
    assert len(blacklist.get_symbols(pd.to_datetime('2020-05-10'))) == 5
    assert len(blacklist.get_symbols(pd.to_datetime('2020-06-10'))) == 2
    assert len(blacklist.get_symbols(pd.to_datetime('2020-07-10'))) == 0
