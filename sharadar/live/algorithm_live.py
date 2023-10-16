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
import os.path
import time
from datetime import datetime, timedelta
from sharadar.util.logger import log
import pandas as pd
from dateutil.relativedelta import relativedelta

from zipline.utils.math_utils import tolerant_equals, round_if_near_integer
from sharadar.live.blotter_live import BlotterLive
from zipline.algorithm import TradingAlgorithm
from zipline.errors import ZiplineError
from sharadar.live.realtimeclock import RealtimeClock
from zipline.gens.tradesimulation import AlgorithmSimulator
from zipline.utils.api_support import ZiplineAPI, api_method, require_initialized
from sharadar.util.serialization_utils import load_context, store_context
from zipline.finance.metrics import MetricsTracker

MARKET_OPEN = 'open'
MARKET_CLOSE = 'close'


# how many minutes before Trading starts needs the function before_trading_starts
# be launched
_minutes_before_trading_starts = 60*4

class RequireInitError(ZiplineError):
    msg = "{function} should only be called after initialization."

class LiveAlgorithmExecutor(AlgorithmSimulator):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)

    def _cleanup_expired_assets(self, dt, position_assets):
        # In simulation this is used to close assets in the simulation end date, which makes a lot of sense.
        # in our case, "simulation end" is set to 1 day from now (we might want to fix that in the future too) BUT,
        #  we don't really have a simulation end date, and we should let the algorithm decide when to close the assets.
        pass


class LiveTradingAlgorithm(TradingAlgorithm):
    def __init__(self, *args, **kwargs):
        self.broker = kwargs.pop('broker', None)
        self.orders = {}

        self.algo_filename = kwargs.get('algo_filename', "<algorithm>")
        self.state_filename = kwargs.pop('state_filename', None)

        # blotter is always initialized to SimulationBlotter in run_algo.py.
        # we override it here to use the LiveBlotter for live algos
        blotter_live = BlotterLive(broker=self.broker)
        kwargs['blotter'] = blotter_live

        super(self.__class__, self).__init__(*args, **kwargs)

    @api_method
    def get_datetime(self, tz=None):
        """Returns the current simulation datetime.

        Parameters
        ----------
        tz : tzinfo or str, optional
            The timezone to return the datetime in.

        Returns
        -------
        dt : datetime
            The current simulation datetime converted to ``tz``.
        """
        dt = self.datetime

        if tz is not None:
            dt = dt.tz_localize(tz)
        return dt

    def schedule_function(self, func, date_rule=None, time_rule=None, half_days=True, calendar=None):
        if hasattr(date_rule, 'execution_period_values'):
            values = date_rule.execution_period_values
            now = int(int(time.time()) * 1e9)
            filtered = sorted([x for x in values if x > now])
            next_date = datetime.fromtimestamp(filtered[0] / 1e9).strftime("%A, %B %d %Y")
            log.info('Next execution of %s scheduled on %s' % (func.__name__, next_date))

        return super().schedule_function(func, date_rule, time_rule, half_days, calendar)

    @api_method
    def get_environment(self, field='platform'):
        if field == 'arena':
            return 'live'
        return super(self.__class__, self).get_environment(field)

    def initialize(self, *args, **kwargs):
        if os.path.isfile(self.state_filename):
            log.info("Loading state from {}".format(self.state_filename))
            load_context(self.state_filename, context=self)

        self.initialized = False

        with ZiplineAPI(self):
            super(self.__class__, self).initialize(*args, **kwargs)
            store_context(self.state_filename, context=self)

        self.initialized = True
        log.info("initialization done")

    def handle_data(self, data):
        super(self.__class__, self).handle_data(data)
        store_context(self.state_filename, context=self)

    def teardown(self):
        store_context(self.state_filename, context=self)

    def _create_clock(self):
        # This method is taken from TradingAlgorithm.
        # The clock has been replaced to use RealtimeClock
        trading_o_and_c = self.trading_calendar.schedule.loc[
            self.sim_params.sessions]
        assert self.sim_params.data_frequency == 'minute'

        minutely_emission = True
        execution_opens = trading_o_and_c[MARKET_OPEN].dt.tz_localize(None)
        execution_closes = trading_o_and_c[MARKET_CLOSE].dt.tz_localize(None)
        before_trading_start_minutes = execution_opens - timedelta(minutes=_minutes_before_trading_starts)

        return RealtimeClock(
            self.sim_params.sessions,
            execution_opens,
            execution_closes,
            before_trading_start_minutes,
            minute_emission=minutely_emission,
            time_skew=self.broker.time_skew,
            is_broker_alive=self.broker.is_alive
        )

    def _create_generator(self, sim_params):
        TradingAlgorithm._create_generator(self, self.sim_params)

        self.metrics_tracker = metrics_tracker = self._create_live_metrics_tracker()
        benchmark_source = self._create_benchmark_source()
        metrics_tracker.handle_start_of_simulation(benchmark_source)

        # attach metrics_tracker to broker
        self.broker.set_metrics_tracker(self.metrics_tracker)

        self.trading_client = LiveAlgorithmExecutor(
            self,
            sim_params,
            self.data_portal,
            self.trading_client.clock,
            self._create_benchmark_source(),
            self.restrictions
        )

        return self.trading_client.transform()

    def _create_live_metrics_tracker(self):
        """
        creating the metrics_tracker but setting values from the broker and
        not from the simulation params
        :return:
        """
        account = self.broker.get_account_from_broker()
        capital_base = float(account['NetLiquidation'])

        return MetricsTracker(
            trading_calendar=self.trading_calendar,
            first_session=self.sim_params.start_session,
            last_session=self.sim_params.end_session,
            capital_base=capital_base,
            emission_rate=self.sim_params.emission_rate,
            data_frequency=self.sim_params.data_frequency,
            asset_finder=self.asset_finder,
            metrics=self._metrics_set,
        )

    def updated_portfolio(self):
        return self.broker.portfolio

    def updated_account(self):
        return self.broker.account

    @api_method
    def symbol(self, symbol_str):
        # This method works around the problem of not being able to trade
        # assets which does not have ingested data for the day of trade.
        # Normally historical data is loaded to bundle and the asset's
        # end_date and auto_close_date is set based on the last entry from
        # the bundle db. LiveTradingAlgorithm does not override order_value(),
        # order_percent() & order_target(). Those higher level ordering
        # functions provide a safety net to not to trade de-listed assets.
        # If the asset is returned as it was ingested (end_date=yesterday)
        # then CannotOrderDelistedAsset exception will be raised from the
        # higher level order functions.
        #
        # Hence, we are increasing the asset's end_date by 10 years.

        asset = super(self.__class__, self).symbol(symbol_str)
        tradeable_asset = asset.to_dict()
        end_date = (pd.Timestamp.now() + relativedelta(years=10)).date()
        tradeable_asset['end_date'] = end_date
        tradeable_asset['auto_close_date'] = end_date

        return asset.from_dict(tradeable_asset)

    def _check_delisted(self, asset):
        # on live trading this check is delegated to the broker
        pass

    def _calculate_order_value_amount(self, asset, value):
        """
        # on live trading the delisted check is delegated to the broker
        """
        last_price =  self.trading_client.current_data.current(asset, "price")
        if tolerant_equals(last_price, 0):
            zero_message = "Price of 0 for {psid}; can't infer value".format(
                psid=asset
            )
            if self.logger:
                self.logger.debug(zero_message)
            # Don't place any order
            return 0

        value_multiplier = asset.price_multiplier

        return value / (last_price * value_multiplier)

    def run(self, *args, **kwargs):
        daily_stats = super(self.__class__, self).run(*args, **kwargs)
        self.on_exit()
        return daily_stats

    def on_exit(self):
        self.teardown()
        self.broker.disconnect()
        log.info("Today's trading ended. The algo needs to be restarted daily.")

    def _pipeline_output(self, pipeline, chunks, name):
        # This method is taken from TradingAlgorithm.
        """
        Internal implementation of `pipeline_output`.

        For Live Algo's we have to get the previous session as the Pipeline wont work without,
        it will extrapolate such that it tries to get data for get_datetime which
        is today

        """
        today = self.get_datetime().normalize()
        prev_session = self.trading_calendar.previous_open(today).tz_localize(None).normalize()

        log.info('today in _pipeline_output : {}'.format(prev_session))

        try:
            data = self._pipeline_cache.get(name, prev_session)
        except KeyError:
            # Calculate the next block.
            data, valid_until = self.run_pipeline(
                pipeline, prev_session, next(chunks),
            )
            self._pipeline_cache.set(name, data, valid_until)

        # Now that we have a cached result, try to return the data for today.
        try:
            return data.loc[prev_session]
        except KeyError:
            # This happens if no assets passed the pipeline screen on a given
            # day.
            return pd.DataFrame(index=[], columns=data.columns)

    def _sync_last_sale_prices(self, dt=None):
        """
        we get the updates from the broker so we don't need to use this method which
        tries to get it from the ingested data
        :param dt:
        :return:
        """
        pass

    @property
    @require_initialized(RequireInitError(function="portfolio"))
    def portfolio(self):
        return self.updated_portfolio()

    @property
    @require_initialized(RequireInitError(function="account"))
    def account(self):
        return self.updated_account()

    @staticmethod
    def round_order(amount):
        if pd.isna(amount):
            return 0
        return int(round_if_near_integer(amount))

    def run_pipeline(self, pipeline, start_session, chunksize):
        # In Live mode a Pipeline can be run only for the current session (end_session = start_session)
        return self.engine.run_pipeline(pipeline, start_session, start_session), start_session






