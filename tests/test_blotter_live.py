from unittest.mock import MagicMock
from sharadar.live.blotter_live import BlotterLive
from zipline.assets import Equity


class TestBlotterLive:
    def test_init(self):
        broker = MagicMock()
        blotter = BlotterLive(broker)
        assert blotter.broker is broker
        assert blotter._processed_transactions == []
        assert blotter._processed_closed_orders == []
        assert blotter.new_orders == []

    def test_orders_delegates_to_broker(self):
        broker = MagicMock()
        broker.orders = {'id1': MagicMock()}
        blotter = BlotterLive(broker)
        assert blotter.orders == broker.orders

    def test_order_zero_amount_returns_none(self):
        broker = MagicMock()
        blotter = BlotterLive(broker)
        asset = MagicMock(spec=Equity)
        result = blotter.order(asset, 0, MagicMock())
        assert result is None
        broker.order.assert_not_called()

    def test_order_nonzero_calls_broker(self):
        broker = MagicMock()
        order_mock = MagicMock()
        order_mock.id = 'order-123'
        broker.order.return_value = order_mock
        blotter = BlotterLive(broker)
        asset = MagicMock(spec=Equity)
        result = blotter.order(asset, 100, MagicMock())
        assert result == 'order-123'
        broker.order.assert_called_once()

    def test_cancel_delegates_to_broker(self):
        broker = MagicMock()
        blotter = BlotterLive(broker)
        blotter.cancel('order-123')
        broker.cancel_order.assert_called_once_with('order-123')

    def test_get_transactions_returns_delta(self):
        broker = MagicMock()
        tx1 = MagicMock()
        tx1.order_id = 'o1'
        tx2 = MagicMock()
        tx2.order_id = 'o2'
        broker.transactions = {'t1': tx1}
        order1 = MagicMock()
        order1.open = False
        order1.commission = 0.01
        broker.orders = {'o1': order1}
        blotter = BlotterLive(broker)
        # First call should return tx1
        new_tx, new_comm, new_closed = blotter.get_transactions(None)
        assert len(new_tx) == 1
        # Second call with same data should return empty
        new_tx2, _, _ = blotter.get_transactions(None)
        assert len(new_tx2) == 0

    def test_open_orders_groups_by_asset(self):
        broker = MagicMock()
        asset1 = MagicMock()
        order1 = MagicMock()
        order1.asset = asset1
        order1.open = True
        order2 = MagicMock()
        order2.asset = asset1
        order2.open = True
        broker.orders = {'o1': order1, 'o2': order2}
        blotter = BlotterLive(broker)
        open_orders = blotter.open_orders
        assert asset1 in open_orders
        assert len(open_orders[asset1]) == 2
