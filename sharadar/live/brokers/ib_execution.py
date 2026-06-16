"""Interactive Brokers order execution styles.

Defines custom execution styles for IB-specific order types such as
Market-to-Limit and Midprice orders, extending zipline's ExecutionStyle.
"""
from zipline.finance.execution import ExecutionStyle


class TWSOrder(ExecutionStyle):
    """Base class for TWS-specific order execution styles.

    Extends zipline's ExecutionStyle to support IB-specific order types
    with configurable time-in-force and exchange routing.

    Attributes:
        _exchange: Target exchange for order routing.
        _order_type: IB order type string (e.g., 'MTL', 'MIDPRICE').
        _time_in_force: Order duration (e.g., 'GTC', 'DAY').
    """

    def __init__(self, order_type, time_in_force="GTC", exchange=None):
        """Initialize a TWS order style.

        Args:
            order_type: IB order type string.
            time_in_force: Order duration policy. Defaults to 'GTC'.
            exchange: Target exchange. Defaults to None (SMART routing).
        """
        self._exchange = exchange
        self._order_type = order_type
        self._time_in_force = time_in_force

    def get_order_type(self):
        """Return the IB order type string."""
        return self._order_type

    def get_time_in_force(self):
        """Return the time-in-force setting for this order."""
        return self._time_in_force

    def get_limit_price(self, _is_buy):
        """Return the limit price (None for non-limit order types)."""
        return None

    def get_stop_price(self, _is_buy):
        """Return the stop price (None for non-stop order types)."""
        return None


class MarketToLimitOrder(TWSOrder):
    """
    A Market-to-Limit (MTL) order is submitted as a market order to execute at the current best market price.
    If the order is only partially filled, the remainder of the order is canceled and re-submitted
    as a limit order with the limit price equal to the price at which the filled portion of the order executed.
    """

    def __init__(self, time_in_force="GTC", exchange=None):
        super().__init__("MTL", time_in_force, exchange)


class MidPriceOrder(TWSOrder):
    """
    A Midprice order is designed to split the difference between the bid and ask prices, and fill at the current
    midpoint of the NBBO or better. Set an optional price cap to define the highest price (for a buy order) or the
    lowest price (for a sell order) you are willing to accept. Requires TWS 975+. Smart-routing to US stocks only.
    """

    def __init__(self, time_in_force="GTC", exchange=None):
        super().__init__("MIDPRICE", time_in_force, exchange)
