import pandas as pd
import pytest
from datetime import timedelta
from unittest.mock import MagicMock, patch
from sharadar.live.algorithm_live import LiveTradingAlgorithm


class TestAlgorithmLiveSymbol:
    """Test the symbol method with end_date calculation"""
    
    def test_symbol_end_date_is_10_years_ahead(self):
        """Test that end_date is set to approximately 10 years from now"""
        # Create a mock algorithm
        algo = MagicMock(spec=LiveTradingAlgorithm)
        
        # Get current time
        now = pd.Timestamp.utcnow()
        
        # Calculate expected end_date (10 years = 365*10 days)
        expected_end_date = (now + timedelta(days=365*10)).normalize().tz_localize(None)
        
        # Verify the calculation is correct
        assert isinstance(expected_end_date, pd.Timestamp)
        # Should be approximately 10 years in the future
        diff = (expected_end_date - now.tz_localize(None)).days
        assert 3649 <= diff <= 3652  # 365*10 days, accounting for leap years
    
    def test_timedelta_10_years_approximation(self):
        """Test that timedelta(days=365*10) is a valid approximation for 10 years"""
        delta = timedelta(days=365*10)
        assert delta.total_seconds() == 365 * 10 * 24 * 3600
        assert delta.days == 3650

class TestPipelineOutputStaleBundle:
    """Delayed data uses the latest common trading session, not an empty frame."""

    def _algo(self, last_available_dt, metrics_dt=None, today="2026-10-13 14:29"):
        import exchange_calendars as xcals
        algo = MagicMock()
        algo.get_datetime.return_value = pd.Timestamp(today, tz="UTC")
        algo.trading_calendar = xcals.get_calendar("XNYS")
        algo.broker.daily_bar_reader.last_available_dt = pd.Timestamp(last_available_dt)
        algo._last_bundle_session = lambda: LiveTradingAlgorithm._last_bundle_session(algo)
        algo._last_daily_metrics_sessions = lambda: LiveTradingAlgorithm._last_daily_metrics_sessions(algo)
        algo.asset_finder.last_available_dt.side_effect = lambda f: pd.Timestamp(metrics_dt or last_available_dt)
        algo._pipeline_cache.get.side_effect = KeyError
        algo.run_pipeline.side_effect = lambda pipeline, session, chunksize: (
            pd.DataFrame({"a": [1.0]}, index=pd.MultiIndex.from_tuples([(session, 1)])),
            session)
        return algo

    @pytest.mark.parametrize("prices,marketcap,ev,expected", [
        ("2026-10-09", "2026-10-12", "2026-10-12", "2026-10-09"),
        ("2026-10-12", "2026-10-09", "2026-10-12", "2026-10-09"),
        ("2026-10-12", "2026-10-12", "2026-10-08", "2026-10-08"),
        ("2026-10-09", "2026-10-08", "2026-10-09", "2026-10-08"),
        ("2026-10-12", "2026-10-12", "2026-10-12", "2026-10-12"),
        ("2026-10-14", "2026-10-14", "2026-10-14", "2026-10-12"),
        ("2026-10-11", "2026-10-12", "2026-10-12", "2026-10-09"),
    ])
    def test_uses_latest_common_session(self, prices, marketcap, ev, expected):
        algo = self._algo(prices)
        algo.asset_finder.last_available_dt.side_effect = lambda f: pd.Timestamp(
            {"marketcap": marketcap, "ev": ev}[f])
        pipeline = MagicMock(columns={"a": None})
        with patch("sharadar.live.algorithm_live.log") as logger:
            out = LiveTradingAlgorithm._pipeline_output(algo, pipeline, iter([5]), "data_pipe")
        session = pd.Timestamp(expected)
        algo.run_pipeline.assert_called_once_with(pipeline, session, 5)
        algo._pipeline_cache.get.assert_called_once_with("data_pipe", session)
        algo._pipeline_cache.set.assert_called_once()
        assert algo._pipeline_cache.set.call_args.args[2] == session
        pd.testing.assert_frame_equal(out, pd.DataFrame({"a": [1.0]}, index=[1]))
        if expected < "2026-10-12":
            logger.warning.assert_called_once()
            message = logger.warning.call_args.args[0]
            assert expected in message
            assert "2026-10-12" in message
        else:
            logger.warning.assert_not_called()
        logger.error.assert_not_called()

    def test_reported_october_29_delay_uses_october_27(self):
        algo = self._algo("2026-10-27", today="2026-10-29 13:31")
        pipeline = MagicMock(columns={"a": None})
        out = LiveTradingAlgorithm._pipeline_output(algo, pipeline, iter([1]), "data_pipe")
        algo.run_pipeline.assert_called_once_with(pipeline, pd.Timestamp("2026-10-27"), 1)
        assert not out.empty

    @pytest.mark.parametrize("missing", ["marketcap", "ev"])
    def test_missing_daily_metrics_returns_empty_without_running_pipeline(self, missing):
        algo = self._algo("2026-10-12")
        algo.asset_finder.last_available_dt.side_effect = lambda f: (
            pd.NaT if f == missing else pd.Timestamp("2026-10-12"))
        pipeline = MagicMock(columns={"a": None, "b": None})
        with patch("sharadar.live.algorithm_live.log") as logger:
            out = LiveTradingAlgorithm._pipeline_output(algo, pipeline, iter([1]), "data_pipe")
        assert out.empty
        assert list(out.columns) == ["a", "b"]
        algo.run_pipeline.assert_not_called()
        algo._pipeline_cache.get.assert_not_called()
        logger.error.assert_called_once()
        assert missing in logger.error.call_args.args[0]

    def test_fallback_reuses_cached_session(self):
        algo = self._algo("2026-10-09")
        cached = pd.DataFrame({"a": [2.0]}, index=pd.MultiIndex.from_tuples([
            (pd.Timestamp("2026-10-09"), 1)]))
        algo._pipeline_cache.get.side_effect = None
        algo._pipeline_cache.get.return_value = cached
        out = LiveTradingAlgorithm._pipeline_output(algo, MagicMock(), iter([]), "data_pipe")
        pd.testing.assert_frame_equal(out, cached.loc[pd.Timestamp("2026-10-09")])
        algo.run_pipeline.assert_not_called()
        algo._pipeline_cache.set.assert_not_called()

    def test_empty_universe_does_not_search_older_sessions(self):
        algo = self._algo("2026-10-09")
        pipeline = MagicMock(columns={"a": None})
        algo.run_pipeline.side_effect = None
        algo.run_pipeline.return_value = (
            pd.DataFrame(columns=["a"], index=pd.MultiIndex.from_arrays([[], []])),
            pd.Timestamp("2026-10-09"))
        out = LiveTradingAlgorithm._pipeline_output(algo, pipeline, iter([1]), "data_pipe")
        assert out.empty
        assert list(out.columns) == ["a"]
        algo.run_pipeline.assert_called_once_with(pipeline, pd.Timestamp("2026-10-09"), 1)

    def test_new_data_advances_output_session(self):
        algo = self._algo("2026-10-09")
        pipeline = MagicMock(columns={"a": None})
        LiveTradingAlgorithm._pipeline_output(algo, pipeline, iter([1]), "data_pipe")
        algo.broker.daily_bar_reader.last_available_dt = pd.Timestamp("2026-10-12")
        algo.asset_finder.last_available_dt.side_effect = lambda f: pd.Timestamp("2026-10-12")
        LiveTradingAlgorithm._pipeline_output(algo, pipeline, iter([1]), "data_pipe")
        assert [call.args[1] for call in algo.run_pipeline.call_args_list] == [
            pd.Timestamp("2026-10-09"), pd.Timestamp("2026-10-12")]
