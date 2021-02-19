import pandas as pd
from sharadar.util.blacklist import Blacklist

blacklist = Blacklist(1)
blacklist.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
blacklist.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))

assert len(blacklist.get_symbols(pd.to_datetime('2020-03-10'))) == 5, "len should be 5"
assert len(blacklist.get_symbols(pd.to_datetime('2020-04-10'))) == 2, "len should be 5"
assert len(blacklist.get_symbols(pd.to_datetime('2020-05-10'))) == 0, "len should be 0"

blacklist = Blacklist(2)
blacklist.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
blacklist.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))

assert len(blacklist.get_symbols(pd.to_datetime('2020-03-10'))) == 5, "len should be 5"
assert len(blacklist.get_symbols(pd.to_datetime('2020-04-10'))) == 5, "len should be 5"
assert len(blacklist.get_symbols(pd.to_datetime('2020-05-10'))) == 2, "len should be 2"
assert len(blacklist.get_symbols(pd.to_datetime('2020-06-10'))) == 0, "len should be 0"

blacklist = Blacklist(3)
blacklist.add_symbols(['IBM', 'F', 'AAPL'], pd.to_datetime('2020-02-03'))
blacklist.add_symbols(['ORCL', 'MSFT'], pd.to_datetime('2020-03-10'))

assert len(blacklist.get_symbols(pd.to_datetime('2020-03-10'))) == 5, "len should be 5"
assert len(blacklist.get_symbols(pd.to_datetime('2020-04-10'))) == 5, "len should be 5"
assert len(blacklist.get_symbols(pd.to_datetime('2020-05-10'))) == 5, "len should be 2"
assert len(blacklist.get_symbols(pd.to_datetime('2020-06-10'))) == 2, "len should be 0"
assert len(blacklist.get_symbols(pd.to_datetime('2020-07-10'))) == 0, "len should be 0"