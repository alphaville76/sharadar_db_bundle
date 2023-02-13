import numpy as np
import time
from sharadar.util.performance import print_portfolio
from zipline.api import order_target, get_open_orders


def compute_portfolio_return(context):
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


def stop_loss_portfolio(context, data, log):
    """
    Close all positions when the whole portfolio loss exceeded the loss_limit
    """
    portfolio_return = compute_portfolio_return(context)

    if portfolio_return <= context.PARAM['loss_limit']:
        log.warn("Monthly loss (%.2f) exceeded the loss limit (%.2f): close all positions." % (
        100.0 * portfolio_return, 100.0 * context.PARAM['loss_limit']))
        close_all(context, data)
        return True

    return False


def stop_loss_equities(context, data, log):
    """
    Close all positions of an equity, when its loss exceeded the loss_limit
    """
    positions = context.portfolio.positions
    if len(positions) == 0:
        return

    for stock, position in list(positions.items()):
        cost_basis = position.cost_basis
        if cost_basis == 0:
            cost_basis = position.last_sale_price
        pl_pct = (position.last_sale_price - cost_basis) / cost_basis
        if pl_pct <= context.PARAM['loss_limit']:
            log.warn("%s positions closed with a loss of %.2f." % (stock, 100.0 * pl_pct))
            if data.can_trade(stock):
                order_target(stock, 0)


def close_all(context, data, exclude=[], log=None):
    """
    Close all positions, except those in the exclude list
    """
    print_portfolio(log, context)
    for stock in context.portfolio.positions:
        if stock in exclude:
            continue
        if data.can_trade(stock):
            if log:
                log.debug("Close all position for %s" % (str(stock)))
            order_target(stock, 0)


def await_no_open_orders(timeout_sec=3600, log=None):
    if log.arena != 'live':
        return
    start_time = time.time()
    while len(get_open_orders()) > 0:
        if log:
            log.info("Still %d open orders: %s" % (
            len(get_open_orders()), str([k.symbol for (k, v) in get_open_orders().items()])))
        if (time.time() - start_time) > timeout_sec:
            log.info("Open orders timeout (%d seconds) reached!" % timeout_sec)
            return
        time.sleep(1)
    if log:
        log.info("No pending orders!")
