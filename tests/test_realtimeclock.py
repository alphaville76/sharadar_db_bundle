import pandas as pd
from sharadar.live.realtimeclock import RealtimeClock


class TestRealtimeClockInit:
    def test_stores_time_skew(self):
        sessions = pd.DatetimeIndex([])
        opens = pd.DatetimeIndex([])
        closes = pd.DatetimeIndex([])
        bts = pd.DatetimeIndex([])
        clock = RealtimeClock(
            sessions, opens, closes, bts,
            minute_emission=True,
            time_skew=pd.Timedelta("5s"),
        )
        assert clock.time_skew == pd.Timedelta("5s")

    def test_empty_sessions_iter_returns_nothing(self):
        sessions = pd.DatetimeIndex([])
        opens = pd.DatetimeIndex([])
        closes = pd.DatetimeIndex([])
        bts = pd.DatetimeIndex([])
        clock = RealtimeClock(
            sessions, opens, closes, bts,
            minute_emission=True,
        )
        events = list(clock)
        assert events == []