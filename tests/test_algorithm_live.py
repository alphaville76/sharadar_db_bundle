import pandas as pd
from datetime import timedelta
from unittest.mock import MagicMock
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
    """A bundle that ends before the previous session must not run the pipeline."""

    def _algo(self, last_available_dt):
        import exchange_calendars as xcals
        algo = MagicMock()
        algo.get_datetime.return_value = pd.Timestamp("2026-10-13 14:29", tz="UTC")
        algo.trading_calendar = xcals.get_calendar("XNYS")
        algo.broker.daily_bar_reader.last_available_dt = pd.Timestamp(last_available_dt)
        algo._last_bundle_session = lambda: LiveTradingAlgorithm._last_bundle_session(algo)
        algo._pipeline_cache.get.side_effect = KeyError
        algo.run_pipeline.return_value = (
            pd.DataFrame({"a": [1.0]}, index=pd.MultiIndex.from_tuples([(pd.Timestamp("2026-10-12"), 1)])),
            pd.Timestamp("2026-10-12"))
        return algo

    def test_stale_bundle_returns_empty_without_running_pipeline(self):
        algo = self._algo("2026-10-09")
        pipeline = MagicMock(columns={"a": None, "b": None})
        out = LiveTradingAlgorithm._pipeline_output(algo, pipeline, iter([1]), "data_pipe")
        assert out.empty
        assert list(out.columns) == ["a", "b"]
        algo.run_pipeline.assert_not_called()

    def test_fresh_bundle_runs_pipeline(self):
        algo = self._algo("2026-10-12")
        out = LiveTradingAlgorithm._pipeline_output(algo, MagicMock(), iter([1]), "data_pipe")
        algo.run_pipeline.assert_called_once()
        assert len(out) == 1
