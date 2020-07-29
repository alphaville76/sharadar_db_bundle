from zipline.api import get_datetime
from zipline.api import order_target


def stop_loss_portfolio(context, data, log, close_all):
    """
    Close all positions when the whole portfolio loss exceeded the loss_limit
    """
    if context.portfolio_value_month_start is not None:
        montly_return = context.portfolio.portfolio_value / context.portfolio_value_month_start - 1.0
        if montly_return <= context.PARAM['loss_limit']:
            log.warn("Montly loss (%.2f) exceeded the loss limit: close all positions." % (100.0*montly_return))
            close_all(context, data)
            # To avoid repeated triggers
            context.portfolio_value_month_start = None
            context.INFO['stop_loss_date'].append(get_datetime())


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