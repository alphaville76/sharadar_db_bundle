import pytest
from sharadar.util.calendar_util import last_trading_date


class TestLastTradingDate:
    def test_weekday_returns_same(self):
        assert '2021-05-05' == last_trading_date('2021-05-05')
        assert '2021-05-06' == last_trading_date('2021-05-06')
        assert '2021-05-07' == last_trading_date('2021-05-07')

    def test_weekend_returns_friday(self):
        assert '2021-05-07' == last_trading_date('2021-05-08')
        assert '2021-05-07' == last_trading_date('2021-05-09')

    def test_monday_returns_monday(self):
        assert '2021-05-10' == last_trading_date('2021-05-10')

    def test_consecutive_weekdays(self):
        assert '2021-05-10' == last_trading_date('2021-05-10')
        assert '2021-05-11' == last_trading_date('2021-05-11')
        assert '2021-05-12' == last_trading_date('2021-05-12')
        assert '2021-05-13' == last_trading_date('2021-05-13')
        assert '2021-05-14' == last_trading_date('2021-05-14')

    def test_no_argument_returns_string(self):
        result = last_trading_date()
        assert isinstance(result, str)
        assert len(result) == 10
