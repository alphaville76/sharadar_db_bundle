import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch
from sharadar.live.data_portal_live import DataPortalLive


class TestDataPortalLive:
    @patch('sharadar.live.data_portal_live.DataPortal.__init__', return_value=None)
    def test_get_last_traded_dt_delegates_to_broker(self, mock_init):
        broker = MagicMock()
        broker.get_last_traded_dt.return_value = pd.Timestamp('2023-06-15')
        portal = DataPortalLive.__new__(DataPortalLive)
        portal.broker = broker
        asset = MagicMock()
        result = portal.get_last_traded_dt(asset, pd.Timestamp('2023-06-15'), 'daily')
        broker.get_last_traded_dt.assert_called_once_with(asset)
        assert result == pd.Timestamp('2023-06-15')

    @patch('sharadar.live.data_portal_live.DataPortal.__init__', return_value=None)
    def test_get_spot_value_returns_nan_on_error(self, mock_init):
        broker = MagicMock()
        broker.get_spot_value.side_effect = Exception("Network error")
        portal = DataPortalLive.__new__(DataPortalLive)
        portal.broker = broker
        result = portal.get_spot_value(MagicMock(), 'close', pd.Timestamp('2023-06-15'), 'daily')
        assert np.isnan(result)

    @patch('sharadar.live.data_portal_live.DataPortal.__init__', return_value=None)
    def test_get_spot_value_returns_nat_for_last_traded_on_error(self, mock_init):
        broker = MagicMock()
        broker.get_spot_value.side_effect = Exception("Network error")
        portal = DataPortalLive.__new__(DataPortalLive)
        portal.broker = broker
        result = portal.get_spot_value(MagicMock(), 'last_traded', pd.Timestamp('2023-06-15'), 'daily')
        assert result is pd.NaT