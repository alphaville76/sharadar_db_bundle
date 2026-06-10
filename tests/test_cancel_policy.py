from unittest.mock import MagicMock, patch


class TestCancelOrdersEod:
    @patch('sharadar.live.cancel_policy.cancel_order')
    @patch('sharadar.live.cancel_policy.get_open_orders')
    def test_creates_canceled_orders_dict(self, mock_get_open_orders, mock_cancel_order):
        from sharadar.live.cancel_policy import cancel_orders_eod
        mock_get_open_orders.return_value = {}
        context = MagicMock(spec=[])
        data = MagicMock()
        cancel_orders_eod(context, data)
        assert hasattr(context, 'canceled_orders')
        assert context.canceled_orders == {}

    @patch('sharadar.live.cancel_policy.cancel_order')
    @patch('sharadar.live.cancel_policy.get_open_orders')
    def test_accumulates_amounts(self, mock_get_open_orders, mock_cancel_order):
        from sharadar.live.cancel_policy import cancel_orders_eod
        asset1 = MagicMock()
        order1 = MagicMock()
        order1.sid = 'SID1'
        order1.amount = 100
        order1.filled = 30
        order2 = MagicMock()
        order2.sid = 'SID1'
        order2.amount = 50
        order2.filled = 10
        mock_get_open_orders.return_value = {asset1: [order1, order2]}
        context = MagicMock(spec=[])
        data = MagicMock()
        cancel_orders_eod(context, data)
        # (100 - 30) + (50 - 10) = 110
        assert context.canceled_orders['SID1'] == 110

    @patch('sharadar.live.cancel_policy.cancel_order')
    @patch('sharadar.live.cancel_policy.get_open_orders')
    def test_calls_cancel_order(self, mock_get_open_orders, mock_cancel_order):
        from sharadar.live.cancel_policy import cancel_orders_eod
        asset1 = MagicMock()
        order1 = MagicMock()
        order1.sid = 'SID1'
        order1.amount = 100
        order1.filled = 0
        mock_get_open_orders.return_value = {asset1: [order1]}
        context = MagicMock(spec=[])
        data = MagicMock()
        cancel_orders_eod(context, data)
        mock_cancel_order.assert_called_once_with(order1)

    @patch('sharadar.live.cancel_policy.cancel_order')
    @patch('sharadar.live.cancel_policy.get_open_orders')
    def test_clears_existing_canceled_orders(self, mock_get_open_orders, mock_cancel_order):
        from sharadar.live.cancel_policy import cancel_orders_eod
        mock_get_open_orders.return_value = {}
        context = MagicMock(spec=[])
        context.canceled_orders = {'OLD': 999}
        data = MagicMock()
        cancel_orders_eod(context, data)
        assert context.canceled_orders == {}


class TestResubmitCanceledOrders:
    @patch('sharadar.live.cancel_policy.order')
    @patch('sharadar.live.cancel_policy.get_datetime')
    def test_skips_if_day_exceeds_max_resubmit_day(self, mock_get_datetime, mock_order):
        from sharadar.live.cancel_policy import resubmit_canceled_orders
        mock_dt = MagicMock()
        mock_dt.day = 20
        mock_get_datetime.return_value = mock_dt
        context = MagicMock()
        context.PARAM = {'max_resubmit_day': 15}
        context.canceled_orders = {'SID1': 50}
        data = MagicMock()
        resubmit_canceled_orders(context, data)
        mock_order.assert_not_called()

    @patch('sharadar.live.cancel_policy.order')
    @patch('sharadar.live.cancel_policy.get_datetime')
    def test_resubmits_with_correct_amounts(self, mock_get_datetime, mock_order):
        from sharadar.live.cancel_policy import resubmit_canceled_orders
        mock_dt = MagicMock()
        mock_dt.day = 10
        mock_get_datetime.return_value = mock_dt
        context = MagicMock()
        context.PARAM = {'max_resubmit_day': 15}
        context.canceled_orders = {'SID1': 50, 'SID2': 25}
        data = MagicMock()
        resubmit_canceled_orders(context, data)
        assert mock_order.call_count == 2

    @patch('sharadar.live.cancel_policy.order')
    @patch('sharadar.live.cancel_policy.get_datetime')
    def test_clamps_negative_amount_to_position_size(self, mock_get_datetime, mock_order):
        from sharadar.live.cancel_policy import resubmit_canceled_orders
        mock_dt = MagicMock()
        mock_dt.day = 10
        mock_get_datetime.return_value = mock_dt
        context = MagicMock()
        context.PARAM = {'max_resubmit_day': 15}
        context.canceled_orders = {'SID1': -200}
        position = MagicMock()
        position.amount = 50
        context.portfolio.positions = {'SID1': position}
        data = MagicMock()
        resubmit_canceled_orders(context, data)
        # abs(-200) > 50, so amount should be clamped to -50
        mock_order.assert_called_once_with('SID1', -50)
