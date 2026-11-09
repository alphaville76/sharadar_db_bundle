"""Minimal IB Gateway simulator for exercising IBBroker without a real gateway.

Speaks the classic (non-protobuf) TWS text protocol by advertising server
version 200, and implements only the requests used by
``sharadar.live.brokers.ib_broker``: startApi, reqIds, reqCurrentTime,
reqExecutions, reqAccountUpdates, reqMktData (snapshot), placeOrder and
cancelOrder. Orders are filled immediately at the last price (limit orders
only when marketable); account and portfolio updates are pushed after fills.

Standalone usage (prices from the local Sharadar bundle):

    python tests/ib_gateway_sim.py --port 4002 --cash 1000000 --positions AAPL:100,SPY:50
"""
import argparse
import itertools
import logging
import socket
import struct
import threading
import time

import pandas as pd

SERVER_VERSION = 200  # last version before protobuf (201)

log = logging.getLogger("ib_gateway_sim")

# incoming (client -> gateway) message ids
REQ_MKT_DATA, CANCEL_MKT_DATA, PLACE_ORDER, CANCEL_ORDER, REQ_OPEN_ORDERS = 1, 2, 3, 4, 5
REQ_ACCT_DATA, REQ_EXECUTIONS, REQ_IDS, REQ_ALL_OPEN_ORDERS = 6, 7, 8, 16
REQ_CURRENT_TIME, START_API = 49, 71

# outgoing (gateway -> client) message ids
TICK_PRICE, ORDER_STATUS, ERR_MSG, ACCT_VALUE, PORTFOLIO_VALUE = 1, 3, 4, 6, 7
ACCT_UPDATE_TIME, NEXT_VALID_ID, EXECUTION_DATA, MANAGED_ACCTS = 8, 9, 11, 15
TICK_STRING, CURRENT_TIME, OPEN_ORDER_END, ACCT_DOWNLOAD_END = 46, 49, 53, 54
EXECUTION_DATA_END, TICK_SNAPSHOT_END, COMMISSION_REPORT = 55, 57, 59

TICK_LAST, TICK_LAST_TIMESTAMP = 4, 45

USD_ACCOUNT_KEYS = ('CashBalance', 'AccruedCash', 'BuyingPower', 'EquityWithLoanValue',
                    'StockMarketValue', 'TotalCashValue', 'RegTEquity', 'RegTMargin',
                    'FullInitMarginReq', 'FullMaintMarginReq', 'AvailableFunds',
                    'ExcessLiquidity', 'NetLiquidation')


def _frame(*fields):
    payload = "".join("%s\0" % f for f in fields).encode()
    return struct.pack("!I", len(payload)) + payload


def bundle_price_fn():
    """Return a price lookup using the last close of the local Sharadar bundle."""
    from sharadar.pipeline.engine import BundleLoader
    finder = BundleLoader().asset_finder()
    reader = BundleLoader().bar_reader()
    last_dt = reader.last_available_dt
    cache = {}

    def price(ib_symbol):
        if ib_symbol not in cache:
            try:
                sid = finder.lookup_symbol(ib_symbol.replace(' ', '.'), as_of_date=None).sid
                value = reader.get_value(sid, last_dt, 'close')
                cache[ib_symbol] = float(value) if value == value and value > 0 else None
            except Exception:
                cache[ib_symbol] = None
        return cache[ib_symbol]

    return price


class IBGatewaySim:
    """Threaded TCP server emulating the subset of IB Gateway used by IBBroker.

    State (cash, positions, executions, order ids) survives client reconnects,
    so a strategy can be restarted against the same simulated account.
    """

    def __init__(self, port=0, account='DU000000', cash=1_000_000.0, positions=None,
                 price_fn=None, now_fn=None, commission_per_share=0.0):
        self.account = account
        self.cash = float(cash)
        self.price_fn = price_fn or (lambda s: 100.0)
        # timestamps of trades/ticks; currentTime always uses the real clock
        self.now_fn = now_fn or (lambda: pd.Timestamp.now(tz='UTC'))
        self.commission_per_share = commission_per_share
        self.positions = {}  # symbol -> [qty, avg_cost]
        for sym, qty in (positions or {}).items():
            px = self._price(sym) or 100.0
            self.positions[sym] = [int(qty), px]
        self.orders = {}       # order_id -> dict
        self.executions = []   # list of (order_id, client_id, fields-producing dict)
        self.next_order_id = 1
        self._exec_seq = itertools.count(1)
        self._lock = threading.RLock()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(('127.0.0.1', port))
        self._sock.listen(5)
        self.port = self._sock.getsockname()[1]
        self._conns = []
        self._stopped = False
        self.requests = []     # (msg_id, fields) log for assertions

    # ------------------------------------------------------------------ server
    def start(self):
        threading.Thread(target=self._accept_loop, daemon=True, name='ib-sim-accept').start()
        log.info("IB Gateway simulator listening on 127.0.0.1:%d", self.port)
        return self

    def stop(self):
        self._stopped = True
        for conn in list(self._conns):
            try:
                conn.shutdown(socket.SHUT_RDWR)
                conn.close()
            except OSError:
                pass
        self._sock.close()

    def _accept_loop(self):
        while not self._stopped:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            self._conns.append(conn)
            threading.Thread(target=self._serve, args=(conn,), daemon=True, name='ib-sim-conn').start()

    @staticmethod
    def _recv_exact(conn, n):
        buf = b''
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("client closed")
            buf += chunk
        return buf

    def _recv_msg(self, conn):
        size = struct.unpack("!I", self._recv_exact(conn, 4))[0]
        return self._recv_exact(conn, size)

    def _serve(self, conn):
        session = {'conn': conn, 'client_id': 0, 'send_lock': threading.Lock()}
        try:
            prefix = self._recv_exact(conn, 4)
            if prefix != b"API\0":
                raise ConnectionError("bad handshake prefix %r" % prefix)
            log.info("client versions %s", self._recv_msg(conn).decode())
            conn_time = time.strftime("%Y%m%d %H:%M:%S") + " UTC"
            conn.sendall(_frame(SERVER_VERSION, conn_time))
            while not self._stopped:
                fields = [f.decode('latin-1') for f in self._recv_msg(conn).split(b"\0")[:-1]]
                msg_id = int(fields[0])
                with self._lock:
                    self.requests.append((msg_id, fields))
                    try:
                        self._dispatch(session, msg_id, fields)
                    except (ValueError, IndexError, StopIteration):
                        log.exception("cannot handle message %d: %s", msg_id, fields)
        except (ConnectionError, OSError):
            log.info("client disconnected")
        finally:
            if conn in self._conns:
                self._conns.remove(conn)
            try:
                conn.close()
            except OSError:
                pass

    def _send(self, session, *fields):
        with session['send_lock']:
            session['conn'].sendall(_frame(*fields))

    # ---------------------------------------------------------------- handlers
    def _dispatch(self, s, msg_id, f):
        if msg_id == START_API:
            s['client_id'] = int(f[2])
            self._send(s, MANAGED_ACCTS, 1, self.account)
            self._send(s, NEXT_VALID_ID, 1, self.next_order_id)
        elif msg_id == REQ_IDS:
            self._send(s, NEXT_VALID_ID, 1, self.next_order_id)
        elif msg_id == REQ_CURRENT_TIME:
            self._send(s, CURRENT_TIME, 1, int(time.time()))
        elif msg_id == REQ_EXECUTIONS:
            req_id = int(f[2])
            for ex in self.executions:
                self._send_execution(s, req_id, ex)
            self._send(s, EXECUTION_DATA_END, 1, req_id)
        elif msg_id == REQ_ACCT_DATA:
            if f[2] == '1':
                self._send_account(s)
                self._send(s, ACCT_DOWNLOAD_END, 1, self.account)
        elif msg_id == REQ_MKT_DATA:
            self._market_data(s, req_id=int(f[2]), sym=f[4])
        elif msg_id == PLACE_ORDER:
            self._place_order(s, f)
        elif msg_id == CANCEL_ORDER:
            self._cancel_order(s, int(f[1]))
        elif msg_id in (REQ_OPEN_ORDERS, REQ_ALL_OPEN_ORDERS):
            self._send(s, OPEN_ORDER_END, 1)
        elif msg_id == CANCEL_MKT_DATA:
            pass
        else:
            log.debug("ignored message %d: %s", msg_id, f)

    def _price(self, sym):
        return self.price_fn(sym)

    def _error(self, s, req_id, code, text):
        self._send(s, ERR_MSG, req_id, code, text, "", int(time.time() * 1000))

    def _market_data(self, s, req_id, sym):
        px = self._price(sym)
        if px is None:
            self._error(s, req_id, 200, "No security definition has been found for the request")
            return
        self._send(s, TICK_PRICE, 6, req_id, TICK_LAST, px, 100, 0)
        self._send(s, TICK_STRING, 1, req_id, TICK_LAST_TIMESTAMP, int(self.now_fn().timestamp()))
        self._send(s, TICK_SNAPSHOT_END, 1, req_id)

    def _place_order(self, s, f):
        order_id, sym = int(f[1]), f[3]
        # the action follows the contract fields, whose count depends on the server version
        i = next(i for i in range(4, len(f)) if f[i] in ('BUY', 'SELL', 'SSHORT'))
        action, qty, order_type = f[i], int(float(f[i + 1])), f[i + 2]
        lmt = float(f[i + 3]) if f[i + 3] else 0.0
        order_ref = f[i + 10]
        self.next_order_id = max(self.next_order_id, order_id + 1)
        order = dict(id=order_id, symbol=sym, action=action, qty=qty, type=order_type,
                     lmt=lmt, ref=order_ref, client_id=s['client_id'], status='Submitted')
        self.orders[order_id] = order
        px = self._price(sym)
        if px is None:
            order['status'] = 'Cancelled'
            self._error(s, order_id, 200, "No security definition has been found for the request")
            self._order_status(s, order, 0, qty, 0.0)
            return
        self._order_status(s, order, 0, qty, 0.0)
        marketable = (order_type != 'LMT' or lmt <= 0 or
                      (action == 'BUY' and lmt >= px) or (action == 'SELL' and lmt <= px))
        if marketable:
            self._fill(s, order, px)

    def _fill(self, s, order, px):
        sign = 1 if order['action'] == 'BUY' else -1
        qty = order['qty']
        pos = self.positions.setdefault(order['symbol'], [0, px])
        new_qty = pos[0] + sign * qty
        if new_qty != 0 and (pos[0] == 0 or (pos[0] > 0) == (sign > 0)):
            pos[1] = (pos[0] * pos[1] + sign * qty * px) / new_qty
        pos[0] = new_qty
        commission = self.commission_per_share * qty
        self.cash -= sign * qty * px + commission
        if new_qty == 0:
            del self.positions[order['symbol']]

        order['status'] = 'Filled'
        ex = dict(order=order, exec_id="%08x.%d.01.01" % (int(time.time()), next(self._exec_seq)),
                  time=self.now_fn().tz_convert('US/Eastern').strftime('%Y%m%d %H:%M:%S') + ' US/Eastern',
                  price=px, commission=commission)
        self.executions.append(ex)
        self._order_status(s, order, qty, 0, px)
        self._send_execution(s, -1, ex)
        self._send(s, COMMISSION_REPORT, 1, ex['exec_id'], commission, 'USD', 1.7976931348623157e308,
                   1.7976931348623157e308, 0)
        self._send_portfolio(s, order['symbol'])
        self._send_account_values(s)

    def _cancel_order(self, s, order_id):
        order = self.orders.get(order_id)
        if order is None or order['status'] in ('Filled', 'Cancelled'):
            self._error(s, order_id, 161, "Cancel attempted when order is not in a cancellable state")
            return
        order['status'] = 'Cancelled'
        self._order_status(s, order, 0, order['qty'], 0.0)

    def _order_status(self, s, order, filled, remaining, avg_px):
        self._send(s, ORDER_STATUS, order['id'], order['status'], filled, remaining, avg_px,
                   order['id'] + 1000, 0, avg_px, order['client_id'], '', 0.0)

    def _send_execution(self, s, req_id, ex):
        o = ex['order']
        side = 'BOT' if o['action'] == 'BUY' else 'SLD'
        self._send(s, EXECUTION_DATA, req_id, o['id'], 0, o['symbol'], 'STK', '', 0.0, '', '',
                   'ISLAND', 'USD', o['symbol'], o['symbol'], ex['exec_id'], ex['time'], self.account,
                   'ISLAND', side, o['qty'], ex['price'], o['id'] + 1000, o['client_id'], 0,
                   o['qty'], ex['price'], o['ref'], '', 0.0, '', 0, 0, '')

    def _market_value(self):
        return sum(q * (self._price(sym) or c) for sym, (q, c) in self.positions.items())

    def _send_account_values(self, s):
        mv = self._market_value()
        nlv = self.cash + mv
        values = dict.fromkeys(USD_ACCOUNT_KEYS, 0.0)
        values.update(CashBalance=self.cash, BuyingPower=max(nlv, 0) * 2, EquityWithLoanValue=nlv,
                      StockMarketValue=mv, TotalCashValue=self.cash, RegTEquity=nlv,
                      AvailableFunds=nlv, ExcessLiquidity=nlv, NetLiquidation=nlv)
        for key, value in values.items():
            self._send(s, ACCT_VALUE, 2, key, "%.2f" % value, 'USD', self.account)
        for key, value in (('Cushion', 1.0), ('DayTradesRemaining', -1), ('Leverage-S', mv / nlv if nlv else 0)):
            self._send(s, ACCT_VALUE, 2, key, value, '', self.account)
        self._send(s, ACCT_UPDATE_TIME, 1, time.strftime('%H:%M'))

    def _send_portfolio(self, s, sym):
        qty, cost = self.positions.get(sym, [0, 0.0])
        px = self._price(sym) or cost
        self._send(s, PORTFOLIO_VALUE, 8, 0, sym, 'STK', '', 0.0, '', '', 'NASDAQ', 'USD', sym, sym,
                   qty, px, qty * px, cost, qty * (px - cost), 0.0, self.account)

    def _send_account(self, s):
        self._send_account_values(s)
        for sym in list(self.positions):
            self._send_portfolio(s, sym)


def _parse_positions(text):
    positions = {}
    for item in filter(None, (text or '').split(',')):
        sym, _, qty = item.partition(':')
        positions[sym.strip()] = int(qty)
    return positions


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--port', type=int, default=4002)
    parser.add_argument('--account', default='U7807544')
    parser.add_argument('--cash', type=float, default=1_000_000.0)
    parser.add_argument('--positions', default='', help='IB symbols with quantity, e.g. AAPL:100,"LGF B":10')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
    sim = IBGatewaySim(port=args.port, account=args.account, cash=args.cash,
                       positions=_parse_positions(args.positions), price_fn=bundle_price_fn()).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        sim.stop()


if __name__ == '__main__':
    main()
