from zipline.api import get_open_orders, cancel_order, get_datetime, order


def cancel_orders_eod(context, data):
    """
    Cancel all open orders at the end of the day. We do it manually, to store
    the canceled orders and resubmit them a day later at market open.

    See also zipline.finance.blotter.simulation_blotter.SimulationBlotter.execute_cancel_policy

    To apply, use:
        schedule_function(cancel_orders_eod, date_rules.every_day(), time_rules.market_close())
    """
    # Delete the previous stored orders

    # Store all orders that have been canceled to resubmit them later
    if not hasattr(context, 'canceled_orders'):
        context.canceled_orders = {}
    else:
        context.canceled_orders.clear()

    for security, open_orders in list(get_open_orders().items()):
        for order in open_orders:
            if order.sid in context.canceled_orders:
                context.canceled_orders[order.sid] += order.amount - order.filled
            else:
                context.canceled_orders[order.sid] = order.amount - order.filled

            cancel_order(order)


def resubmit_canceled_orders(context, data):
    """
    Try to resubmit canceled orders for the first 15 days of the month

    To apply, use:
        schedule_function(resubmit_canceled_orders, date_rules.every_day(), time_rules.market_open())
    """
    if get_datetime().day > context.PARAM.get('max_resubmit_day', 15):
        return

    for sid, amount in list(context.canceled_orders.items()):
        if amount < 0:
            # position.amount is always positive
            position = context.portfolio.positions[sid]
            # Avoid to hurt the long only constrain
            if abs(amount) > position.amount:
                amount = -position.amount

        order(sid, amount)
