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

    @patch('sharadar.live.data_portal_live.DataPortal.get_history_window')
    def test_get_history_window_fills_gaps_without_warnings(self, mock_super_history, recwarn):
        asset = 'AAPL'
        idx = pd.date_range('2023-06-12', periods=3, freq='D')
        # historical bars have a NaN gap that must be filled by ffill/bfill
        historical_bars = pd.DataFrame({asset: [np.nan, 10.0, np.nan]}, index=idx)
        mock_super_history.return_value = historical_bars

        broker = MagicMock()
        columns = pd.MultiIndex.from_tuples([(asset, 'close')])
        realtime_bars = pd.DataFrame([[11.0], [12.0], [13.0]], index=idx, columns=columns)
        broker.get_realtime_bars.return_value = realtime_bars

        trading_calendar = MagicMock()
        trading_calendar.sessions_window.return_value = [pd.Timestamp('2023-06-14')]

        portal = DataPortalLive.__new__(DataPortalLive)
        portal.broker = broker
        portal.trading_calendar = trading_calendar

        result = portal.get_history_window(
            [asset], pd.Timestamp('2023-06-15', tz='UTC'), 3, '1d', 'price', 'daily', ffill=True
        )

        assert not result[asset].isna().any()
        assert not any(
            issubclass(w.category, (FutureWarning, DeprecationWarning))
            for w in recwarn.list
        )

    @patch('sharadar.live.data_portal_live.DataPortal.__init__', return_value=None)
    def test_get_scalar_asset_spot_value_last_traded(self, mock_init):
        asset = MagicMock()
        idx = pd.date_range('2023-06-13', periods=2, freq='D')
        broker = MagicMock()
        broker.get_realtime_bars.return_value = {
            asset: pd.DataFrame({'close': [100.0, 101.0]}, index=idx)
        }
        portal = DataPortalLive.__new__(DataPortalLive)
        portal.broker = broker
        result = portal.get_scalar_asset_spot_value(asset, 'last_traded', pd.Timestamp('2023-06-14'), 'daily')
        assert result == idx[-1]

    @patch('sharadar.live.data_portal_live.DataPortal.__init__', return_value=None)
    def test_get_scalar_asset_spot_value_price(self, mock_init):
        asset = MagicMock()
        idx = pd.date_range('2023-06-13', periods=2, freq='D')
        broker = MagicMock()
        broker.get_realtime_bars.return_value = {
            asset: pd.DataFrame({'close': [100.0, 101.0]}, index=idx)
        }
        portal = DataPortalLive.__new__(DataPortalLive)
        portal.broker = broker
        result = portal.get_scalar_asset_spot_value(asset, 'price', pd.Timestamp('2023-06-14'), 'daily')
        assert result == 101.0