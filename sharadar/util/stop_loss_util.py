from zipline.api import get_datetime
from zipline.api import order_target
from sharadar.util.performance import print_portfolio

def stop_loss_portfolio(context, data, log):
    """
    Close all positions when the whole portfolio loss exceeded the loss_limit
    """

    positions = context.portfolio.positions
    if len(positions) == 0:
        return

    initial_value = 0
    last_value = 0

    for stock, position in list(positions.items()):
        cost_basis = position.cost_basis
        if cost_basis == 0:
            cost_basis = position.last_sale_price

        initial_value += cost_basis * position.amount
        last_value += position.last_sale_price * position.amount

    montly_return = last_value / initial_value - 1.0
    if montly_return <= context.PARAM['loss_limit']:
        log.warn("Montly loss (%.2f) exceeded the loss limit: close all positions." % (100.0*montly_return))
        close_all(context, data)


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
            log.warn("%s positions closed with a loss of %.2f." % (stock, 100.0*pl_pct))
            if data.can_trade(stock):
                order_target(stock, 0)

def close_all(context, data, exclude=[], log=None):
    """
    Close all positions, except those in the exclude list
    """

    if log:
        print_portfolio(log, context)
    for stock in context.portfolio.positions:
        if stock in exclude:
            continue
        if data.can_trade(stock):
            if log:
                log.info("Close all position for %s" % (str(stock)))
            order_target(stock, 0)