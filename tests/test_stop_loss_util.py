from unittest.mock import MagicMock
from sharadar.util.stop_loss_util import compute_portfolio_return


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
