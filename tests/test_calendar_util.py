import pandas as pd
from sharadar.util.calendar_util import last_trading_date


class TestLastTradingDate:
    def test_trading_day_returns_same_date(self):
        # A known trading day (Monday)
        result = last_trading_date('2023-10-16')
        assert result == '2023-10-16'

    def test_weekend_returns_friday(self):
        # Saturday should return Friday
        result = last_trading_date('2023-10-14')  # Saturday
        assert result == '2023-10-13'  # Friday

    def test_sunday_returns_friday(self):
        result = last_trading_date('2023-10-15')  # Sunday
        assert result == '2023-10-13'  # Friday

    def test_default_date_is_today(self):
        # Should not raise and return a valid date
        result = last_trading_date()
        assert isinstance(result, str)
        assert len(result) == 10  # YYYY-MM-DD format

    def test_default_calendar_is_nyse(self):
        # Should work without explicit calendar
        result = last_trading_date('2023-10-16')
        assert result is not None

    def test_custom_calendar(self):
        from exchange_calendars import get_calendar
        cal = get_calendar('XNYS', start=pd.Timestamp('2000-01-01'))
        result = last_trading_date('2023-10-16', calendar=cal)
        assert result == '2023-10-16'

    def test_no_mutable_default_argument_bug(self):
        # Call twice to ensure no state is shared
        result1 = last_trading_date('2023-10-14')
        result2 = last_trading_date('2023-10-15')
        assert result1 == '2023-10-13'
        assert result2 == '2023-10-13'

    def test_holiday_returns_previous_trading_day(self):
        # Christmas 2023 is Monday Dec 25
        result = last_trading_date('2023-12-25')
        assert result == '2023-12-22'  # Friday before
