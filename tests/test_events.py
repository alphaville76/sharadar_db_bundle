import pandas as pd
from sharadar.util.events import OnceAtStart, time_rules, date_rules

class TestOnceAtStart:
    def test_triggers_first_time(self):
        rule = OnceAtStart()
        assert rule.should_trigger(pd.Timestamp('2023-01-01')) is True

    def test_does_not_trigger_second_time(self):
        rule = OnceAtStart()
        rule.should_trigger(pd.Timestamp('2023-01-01'))
        assert rule.should_trigger(pd.Timestamp('2023-01-02')) is False

    def test_does_not_trigger_third_time(self):
        rule = OnceAtStart()
        rule.should_trigger(pd.Timestamp('2023-01-01'))
        rule.should_trigger(pd.Timestamp('2023-01-02'))
        assert rule.should_trigger(pd.Timestamp('2023-01-03')) is False

    def test_initial_state(self):
        rule = OnceAtStart()
        assert rule.already_trigged is False

class TestTimeRules:
    def test_every_minute_is_always(self):
        from zipline.utils.events import Always
        assert time_rules.every_minute is Always

    def test_market_open_returns_after_open(self):
        from zipline.utils.events import AfterOpen
        result = time_rules.market_open(hours=1, minutes=30)
        assert isinstance(result, AfterOpen)

    def test_market_close_returns_before_close(self):
        from zipline.utils.events import BeforeClose
        result = time_rules.market_close(hours=0, minutes=15)
        assert isinstance(result, BeforeClose)

    def test_live_algo_start_is_once_at_start(self):
        assert isinstance(time_rules.live_algo_start, type)  # it is the class itself

class TestDateRules:
    def test_every_day_is_always(self):
        from zipline.utils.events import Always
        assert date_rules.every_day is Always
