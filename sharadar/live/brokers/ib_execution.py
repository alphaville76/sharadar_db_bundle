from zipline.finance.execution import ExecutionStyle


class TWSOrder(ExecutionStyle):
    def __init__(self, order_type, time_in_force="GTC", exchange=None):
        self._exchange = exchange
        self._order_type = order_type
        self._time_in_force = time_in_force

    def get_order_type(self):
        return self._order_type

    def get_time_in_force(self):
        return self._time_in_force

    def get_limit_price(self, _is_buy):
        return None

    def get_stop_price(self, _is_buy):
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
