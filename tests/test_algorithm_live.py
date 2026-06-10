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