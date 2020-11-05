#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import defaultdict
from copy import copy

from six import itervalues, iteritems

from zipline.assets import Equity, Future, Asset
from zipline.finance.blotter.blotter import Blotter
from zipline.extensions import register
from zipline.finance.order import Order
from zipline.finance.slippage import (
    DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT,
    VolatilityVolumeShare,
    FixedBasisPointsSlippage,
)
from zipline.finance.commission import (
    DEFAULT_PER_CONTRACT_COST,
    FUTURE_EXCHANGE_FEES_BY_SYMBOL,
    PerContract,
    PerShare,
)
from zipline.utils.input_validation import expect_types
import pandas as pd
from sharadar.util.logger import log

class BlotterLive(Blotter):
    def __init__(self, broker):
        self.broker = broker
        self._processed_closed_orders = []
        self._processed_transactions = []

        self.new_orders = []


        self.slippage_models = {
            Equity: FixedBasisPointsSlippage(),
            Future: VolatilityVolumeShare(
                volume_limit=DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT,
            ),
        }
        self.commission_models = {
            Equity: PerShare(),
            Future: PerContract(
                cost=DEFAULT_PER_CONTRACT_COST,
                exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
            ),
        }
        log.info('Initialized blotter_live')
    def __repr__(self):
        return """
    {class_name}(
        open_orders={open_orders},
        orders={orders},
        new_orders={new_orders},
    """.strip().format(class_name=self.__class__.__name__,
                       open_orders=self.open_orders,
                       orders=self.broker.orders,
                       new_orders=self.new_orders)
    @property
    def orders(self):
        return self.broker.orders

    @property
    def open_orders(self):
        return {order.asset: order for order in itervalues(self.broker.orders) if order.open}

    @expect_types(asset=Asset)
    def order(self, asset, amount, style, order_id=None):
        assert order_id is None
        if amount == 0:
            return None
        order = self.broker.order(asset, amount, style)
        self.new_orders.append(order)

        return order.id

    def cancel(self, order_id, relay_status=True):
        return self.broker.cancel_order(order_id)

    def cancel_all_orders_for_asset(self, asset, warn=False, relay_status=True):
        """
        Cancel all open orders for a given asset.
        """
        orders = self.open_orders[asset]
        for order in orders[:]:
            self.cancel(order.id, relay_status)

    def reject(self, order_id, reason=''):
        log.warning("Unexpected reject request for {}: '{}'".format(order_id, reason))

    def hold(self, order_id, reason=''):
        log.warning("Unexpected hold request for {}: '{}'".format(order_id, reason))

    def get_transactions(self, bar_data):
        # All returned values from this function are delta between
        # the previous and actual call.
        def _list_delta(lst_a, lst_b):
            return [elem for elem in lst_a if elem not in set(lst_b)]

        all_transactions = [tx for tx in itervalues(self.broker.transactions) if tx.order_id]
        new_transactions = _list_delta(all_transactions, self._processed_transactions)
        self._processed_transactions = all_transactions

        new_commissions = [{'asset': tx.asset,
                            'cost': self.broker.orders[tx.order_id].commission,
                            'order': self.broker.orders[tx.order_id]}
                           for tx in new_transactions]

        all_closed_orders = [order
                             for order in itervalues(self.broker.orders)
                             if not order.open]
        new_closed_orders = _list_delta(all_closed_orders,
                                        self._processed_closed_orders)
        self._processed_closed_orders = all_closed_orders

        return new_transactions, new_commissions, new_closed_orders

    def prune_orders(self, closed_orders):
        # Orders are handled at the broker
        pass

    def process_splits(self, splits):
        # Splits are handled at the broker
        pass

    def execute_cancel_policy(self, event):
        # Cancellation is handled at the broker
        pass