"""Portfolio-level and equity-level stop loss utilities.

Provides functions for monitoring portfolio and individual equity losses,
automatically closing positions when configurable loss limits are breached,
and waiting for open orders to fill in live trading.
"""
import time
from sharadar.util.performance import print_portfolio
from zipline.api import order_target, get_open_orders


def compute_portfolio_return(context):
    """Compute the aggregate return of the current portfolio.

    Calculates total return based on cost basis vs last sale price
    across all positions.

    Args:
        context: Zipline algorithm context with portfolio.positions.

    Returns:
        Portfolio return as a decimal (e.g., -0.05 for a 5%% loss).
    """
    positions = context.portfolio.positions
    if len(positions) == 0:
        return 0

    initial_value = 0
    last_value = 0

    for stock, position in list(positions.items()):
        cost_basis = position.cost_basis
        if cost_basis == 0:
            cost_basis = position.last_sale_price

        initial_value += cost_basis * position.amount
        last_value += position.last_sale_price * position.amount

    if initial_value == 0:
        return 0

    return last_value / initial_value - 1.0


def stop_loss_portfolio(context, data, log=None):
    """Close all positions when portfolio loss exceeds the loss limit.

    Args:
        context: Zipline algorithm context. Must have context.PARAM['loss_limit'].
        data: Zipline data portal.
        log: Optional logger for warning messages.

    Returns:
        True if stop loss was triggered and positions closed, False otherwise.
    """
    portfolio_return = compute_portfolio_return(context)

    if portfolio_return <= context.PARAM['loss_limit']:
        if log:
            log.warn('Monthly loss (%.2f) exceeded the loss limit (%.2f): close all positions.' % (
                100.0 * portfolio_return, 100.0 * context.PARAM['loss_limit']))
        close_all(context, data)
        return True

    return False


def stop_loss_equities(context, data, log=None):
    """Close individual equity positions that exceed the loss limit.

    Args:
        context: Zipline algorithm context. Must have context.PARAM['loss_limit'].
        data: Zipline data portal.
        log: Optional logger for warning messages.
    """
    positions = context.portfolio.positions
    if len(positions) == 0:
        return

    for stock, position in list(positions.items()):
        cost_basis = position.cost_basis
        if cost_basis == 0:
            cost_basis = position.last_sale_price
        if cost_basis == 0:
            continue
        pl_pct = (position.last_sale_price - cost_basis) / cost_basis
        if pl_pct <= context.PARAM['loss_limit']:
            if log:
                log.warn('%s positions closed with a loss of %.2f.' % (stock, 100.0 * pl_pct))
            if data.can_trade(stock):
                order_target(stock, 0)


def close_all(context, data, exclude=None, log=None):
    """Close all positions, except those in the exclude list.

    Args:
        context: Zipline algorithm context.
        data: Zipline data portal.
        exclude: Optional list of assets to skip.
        log: Optional logger for debug messages.
    """
    if exclude is None:
        exclude = []
    
    print_portfolio(log, context)
    for stock in context.portfolio.positions:
        if stock in exclude:
            continue
        if data.can_trade(stock):
            if log:
                log.debug('Close all position for %s' % (str(stock)))
            order_target(stock, 0)


def await_no_open_orders(timeout_sec=3600, log=None):
    """Wait until all open orders are filled or timeout is reached.

    Only active in live trading mode. Polls open orders every second.

    Args:
        timeout_sec: Maximum seconds to wait. Default is 3600.
        log: Logger instance; also used to detect live trading via log.arena.
    """
    # Check if we're in live trading mode
    if log is None or not hasattr(log, 'arena') or log.arena != 'live':
        return
    
    start_time = time.time()
    while len(get_open_orders()) > 0:
        log.info('Still %d open orders: %s' % (
            len(get_open_orders()), str([k.symbol for (k, v) in get_open_orders().items()])))
        if (time.time() - start_time) > timeout_sec:
            log.info('Open orders timeout (%d seconds) reached!' % timeout_sec)
            return
        time.sleep(1)
    log.info('No pending orders!')
