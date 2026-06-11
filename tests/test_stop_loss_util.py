from unittest.mock import MagicMock, patch
from sharadar.util.stop_loss_util import (
    compute_portfolio_return, 
    stop_loss_portfolio,
    close_all,
    await_no_open_orders
)


class TestComputePortfolioReturn:
    def test_empty_portfolio(self):
        context = MagicMock()
        context.portfolio.positions = {}
        result = compute_portfolio_return(context)
        assert result == 0

    def test_single_position_profit(self):
        context = MagicMock()
        position = MagicMock()
        position.cost_basis = 100.0
        position.last_sale_price = 110.0
        position.amount = 10
        context.portfolio.positions = {'AAPL': position}
        
        result = compute_portfolio_return(context)
        assert abs(result - 0.1) < 1e-10  # 10% profit

    def test_single_position_loss(self):
        context = MagicMock()
        position = MagicMock()
        position.cost_basis = 100.0
        position.last_sale_price = 90.0
        position.amount = 10
        context.portfolio.positions = {'AAPL': position}
        
        result = compute_portfolio_return(context)
        assert abs(result - (-0.1)) < 1e-10  # 10% loss

    def test_multiple_positions(self):
        context = MagicMock()
        pos1 = MagicMock()
        pos1.cost_basis = 100.0
        pos1.last_sale_price = 120.0
        pos1.amount = 10
        
        pos2 = MagicMock()
        pos2.cost_basis = 50.0
        pos2.last_sale_price = 40.0
        pos2.amount = 20
        
        context.portfolio.positions = {'AAPL': pos1, 'IBM': pos2}
        
        result = compute_portfolio_return(context)
        # Initial: 100*10 + 50*20 = 2000
        # Final: 120*10 + 40*20 = 2000
        assert abs(result) < 1e-10

    def test_zero_cost_basis_uses_last_price(self):
        context = MagicMock()
        position = MagicMock()
        position.cost_basis = 0.0
        position.last_sale_price = 100.0
        position.amount = 10
        context.portfolio.positions = {'AAPL': position}
        
        result = compute_portfolio_return(context)
        assert result == 0  # last_sale_price / last_sale_price - 1 = 0


class TestStopLossPortfolio:
    def test_no_stop_loss_triggered(self):
        context = MagicMock()
        context.portfolio.positions = {}
        context.PARAM = {'loss_limit': -0.15}
        data = MagicMock()
        
        result = stop_loss_portfolio(context, data)
        assert result is False

    @patch('sharadar.util.stop_loss_util.order_target')
    def test_stop_loss_triggered(self, mock_order):
        context = MagicMock()
        position = MagicMock()
        position.cost_basis = 100.0
        position.last_sale_price = 80.0  # -20% loss
        position.amount = 10
        context.portfolio.positions = {'AAPL': position}
        context.PARAM = {'loss_limit': -0.15}
        data = MagicMock()
        data.can_trade.return_value = True
        
        result = stop_loss_portfolio(context, data)
        assert result is True


class TestCloseAll:
    @patch('sharadar.util.stop_loss_util.order_target')
    def test_close_all_with_none_exclude(self, mock_order):
        context = MagicMock()
        context.portfolio.positions = {'AAPL': MagicMock(), 'IBM': MagicMock()}
        data = MagicMock()
        data.can_trade.return_value = True
        
        # Should not raise
        close_all(context, data, exclude=None)
        assert mock_order.call_count == 2

    @patch('sharadar.util.stop_loss_util.order_target')
    def test_close_all_with_empty_exclude_list(self, mock_order):
        context = MagicMock()
        context.portfolio.positions = {'AAPL': MagicMock(), 'IBM': MagicMock()}
        data = MagicMock()
        data.can_trade.return_value = True
        
        close_all(context, data, exclude=[])
        assert mock_order.call_count == 2

    @patch('sharadar.util.stop_loss_util.order_target')
    def test_close_all_with_exclusions(self, mock_order):
        context = MagicMock()
        context.portfolio.positions = {'AAPL': MagicMock(), 'IBM': MagicMock()}
        data = MagicMock()
        data.can_trade.return_value = True
        
        close_all(context, data, exclude=['AAPL'])
        # Should only close IBM, not AAPL
        mock_order.assert_called_once()


class TestAwaitNoOpenOrders:
    def test_returns_immediately_when_log_is_none(self):
        # Should not raise and return immediately
        await_no_open_orders(log=None)

    def test_returns_immediately_when_not_live(self):
        log = MagicMock()
        log.arena = 'backtest'
        
        # Should not raise and return immediately
        await_no_open_orders(log=log)

    def test_returns_immediately_when_arena_missing(self):
        log = MagicMock(spec=[])  # No arena attribute
        
        # Should not raise
        await_no_open_orders(log=log)

    @patch('sharadar.util.stop_loss_util.get_open_orders')
    def test_waits_for_orders_in_live_mode(self, mock_get_orders):
        log = MagicMock()
        log.arena = 'live'
        mock_get_orders.return_value = {}
        
        await_no_open_orders(timeout_sec=1, log=log)
        log.info.assert_called_with('No pending orders!')
