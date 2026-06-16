"""Abstract broker base class defining the interface for live trading brokers.

All broker implementations (e.g., Interactive Brokers) must implement
this interface to integrate with the live trading system.
"""
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import ABCMeta, abstractmethod


class Broker(object):
    """Abstract base class for live trading broker implementations.

    Defines the interface for order management, position tracking,
    market data subscriptions, and account information retrieval
    that all broker implementations must provide.

    Attributes:
        daily_bar_reader: Optional reader for daily bar data.
    """

    __metaclass__ = ABCMeta

    daily_bar_reader = None

    @abstractmethod
    def disconnect(self):
        """Disconnect from the broker."""
        pass

    @abstractmethod
    def subscribe_to_market_data(self, asset):
        """Subscribe to real-time market data for the given asset.

        Args:
            asset: The asset to subscribe to.
        """
        pass

    @property
    @abstractmethod
    def subscribed_assets(self):
        pass

    @property
    @abstractmethod
    def positions(self):
        pass

    @property
    @abstractmethod
    def portfolio(self):
        pass

    @property
    @abstractmethod
    def account(self):
        pass

    @property
    @abstractmethod
    def time_skew(self):
        pass

    @abstractmethod
    def order(self, asset, amount, style):
        """Place an order for the given asset.

        Args:
            asset: The asset to trade.
            amount: Number of shares (positive=buy, negative=sell).
            style: Execution style (MarketOrder, LimitOrder, etc.).

        Returns:
            A ZPOrder instance representing the placed order.
        """
        pass

    def is_alive(self):
        """Check if the broker connection is active.

        Returns:
            bool: True if the broker connection is alive.
        """
        pass

    @property
    @abstractmethod
    def orders(self):
        pass

    @property
    @abstractmethod
    def transactions(self):
        pass

    @abstractmethod
    def cancel_order(self, order_param):
        """Cancel an existing order.

        Args:
            order_param: The order or order ID to cancel.
        """
        pass

    @abstractmethod
    def get_last_traded_dt(self, asset):
        """Get the last traded datetime for an asset.

        Args:
            asset: The asset to query.

        Returns:
            pd.Timestamp: The last traded datetime.
        """
        pass

    @abstractmethod
    def get_spot_value(self, assets, field, dt, data_frequency):
        """Get the current spot value for one or more assets.

        Args:
            assets: Asset or assets to query.
            field: Data field ('price', 'open', 'high', 'low', 'close', 'volume').
            dt: Current datetime.
            data_frequency: Data frequency ('daily' or 'minute').

        Returns:
            The current value for the requested field.
        """
        pass

    @abstractmethod
    def get_realtime_bars(self, assets, frequency):
        """Get real-time OHLCV bars for assets.

        Args:
            assets: Assets to retrieve bars for.
            frequency: Bar frequency ('1m' or '1d').

        Returns:
            pd.DataFrame: Multi-level DataFrame with asset/OHLCV columns.
        """
        pass
