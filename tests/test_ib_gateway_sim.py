import time

import pytest
from ibapi.contract import Contract
from ibapi.order import Order

import sharadar.live.brokers.ib_broker as ib_broker
from tests.ib_gateway_sim import IBGatewaySim

ACCOUNT = 'DU123456'
PRICES = {'AAPL': 200.0, 'SPY': 500.0}


def _wait(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


@pytest.fixture
def tws(monkeypatch):
    monkeypatch.setattr(ib_broker, '_poll_frequency', 0.05)
    monkeypatch.setattr(ib_broker, 'sleep', lambda s: time.sleep(min(s, 0.05)))
    sim = IBGatewaySim(account=ACCOUNT, cash=10_000.0, positions={'SPY': 10}, price_fn=PRICES.get).start()
    conn = ib_broker.TWSConnection('127.0.0.1:%d:7' % sim.port, ACCOUNT)
    conn.start()
    yield sim, conn
    conn.disconnect()
    sim.stop()


def test_connect_downloads_account_and_positions(tws):
    sim, conn = tws
    usd = conn.accounts[ACCOUNT]['USD']
    assert float(usd['CashBalance']) == 10_000.0
    assert float(usd['NetLiquidation']) == 15_000.0
    assert 'Leverage-S' in conn.accounts[ACCOUNT]['']
    assert int(conn.ib_positions['SPY'].position) == 10
    assert conn.time_skew is not None
    assert conn._next_order_id == 1


def test_market_data_snapshot_creates_bar(tws):
    _, conn = tws
    conn.subscribe_to_market_data('AAPL')
    assert _wait(lambda: 'AAPL' in conn.bars)
    assert conn.bars['AAPL'].last_trade_price.iloc[-1] == 200.0


def test_unknown_symbol_gets_no_bar(tws):
    _, conn = tws
    conn.subscribe_to_market_data('NOPE')
    time.sleep(0.2)
    assert 'NOPE' not in conn.bars


def test_market_order_is_filled_and_account_updated(tws):
    sim, conn = tws
    contract = Contract()
    contract.symbol, contract.secType, contract.exchange, contract.currency = 'AAPL', 'STK', 'SMART', 'USD'
    order = Order()
    order.action, order.totalQuantity, order.orderType, order.tif = 'BUY', 5, 'MKT', 'GTC'
    order.account = ACCOUNT
    order.orderRef = 'A:BUY Q:5 T:MKT L:0 S:0 D:0 !ZL'
    order_id = conn.next_order_id
    conn.placeOrder(order_id, contract, order)

    assert _wait(lambda: conn.order_statuses.get(order_id, {}).get('status') == 'Filled')
    assert _wait(lambda: order_id in conn.executions)
    exec_detail = list(conn.executions[order_id].values())[-1]['exec_detail']
    assert exec_detail.orderRef == order.orderRef
    assert float(exec_detail.shares) == 5 and exec_detail.price == 200.0
    assert _wait(lambda: float(conn.accounts[ACCOUNT]['USD']['CashBalance']) == 9_000.0)
    assert int(conn.ib_positions['AAPL'].position) == 5
    assert sim.positions['AAPL'][0] == 5


def test_non_marketable_limit_order_can_be_cancelled(tws):
    _, conn = tws
    contract = Contract()
    contract.symbol, contract.secType, contract.exchange, contract.currency = 'SPY', 'STK', 'SMART', 'USD'
    order = Order()
    order.action, order.totalQuantity, order.orderType, order.lmtPrice = 'BUY', 1, 'LMT', 400.0
    order_id = conn.next_order_id
    conn.placeOrder(order_id, contract, order)
    assert _wait(lambda: conn.order_statuses.get(order_id, {}).get('status') == 'Submitted')

    from ibapi.order_cancel import OrderCancel
    conn.cancelOrder(order_id, OrderCancel())
    assert _wait(lambda: conn.order_statuses[order_id]['status'] == 'Cancelled')
    assert order_id not in conn.executions
