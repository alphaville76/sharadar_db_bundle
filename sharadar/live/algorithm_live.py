"""Live trading algorithm implementation.

Extends zipline's TradingAlgorithm to support live trading with a real broker,
real-time clock, state persistence, and live portfolio/account updates.
"""
import datetime

from sharadar.util.events import AfterOpen, BeforeClose

"""Live trading algorithm implementation.

Extends zipline's TradingAlgorithm to support live trading with a real broker,
real-time clock, state persistence, and live portfolio/account updates.
"""
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
from datetime import timedelta
from sharadar.util.logger import log
import pandas as pd

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
    """Algorithm executor adapted for live trading.

    Overrides simulation-specific behavior that is inappropriate for
    live trading, such as expiring assets at simulation end date.
    """

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)

    def _cleanup_expired_assets(self, dt, position_assets):
        # In simulation this is used to close assets in the simulation end date, which makes a lot of sense.
        # in our case, "simulation end" is set to 1 day from now (we might want to fix that in the future too) BUT,
        #  we don't really have a simulation end date, and we should let the algorithm decide when to close the assets.
        pass


class LiveTradingAlgorithm(TradingAlgorithm):
    """Trading algorithm for live execution via a broker.

    Extends TradingAlgorithm with live broker integration, real-time clock,
    state persistence across restarts, and live portfolio tracking.

    Attributes:
        broker: The live broker instance.
        orders: Dictionary of placed orders.
        algo_filename: Path to the algorithm source file.
        state_filename: Path for persisting algorithm state.
    """

    def __init__(self, *args, **kwargs):
        """Initialize the live trading algorithm.

        Args:
            *args: Positional arguments passed to TradingAlgorithm.
            **kwargs: Keyword arguments. Special keys:
                broker: Broker instance for live trading.
                state_filename: Path to save/load algorithm state.
        """
        self.broker = kwargs.pop('broker', None)
        self.orders = {}

        self.algo_filename = kwargs.get('algo_filename', "<algorithm>")
        self.state_filename = kwargs.pop('state_filename', None)

        # blotter is always initialized to SimulationBlotter in run_algo.py.
        # we override it here to use the LiveBlotter for live algos
        blotter_live = BlotterLive(broker=self.broker)
        kwargs['blotter'] = blotter_live

        super(self.__class__, self).__init__(*args, **kwargs)

    def on_dt_changed(self, dt):
        # log.info('on_dt_changed: {}'.format(dt))
        super().on_dt_changed(dt)

    def schedule_function(self, func, date_rule=None, time_rule=None, half_days=True, calendar=None):
        now = pd.Timestamp.utcnow().normalize()
        # In case of every day schedulers, otherwise the value is set later.
        next_date = now.date()
        next_time = None

        if hasattr(date_rule, 'execution_period_values'):
            values = date_rule.execution_period_values

            filtered = sorted([x for x in values if x >= now.value])
            next_date = pd.to_datetime(filtered[0], unit='ns', origin='unix').date()

        if time_rule._period_start is None or time_rule._period_close <= next_date:
            my_calendar = self.data_portal.trading_calendar
            if isinstance(time_rule, AfterOpen):
                period_start = my_calendar.session_first_minute(my_calendar.minute_to_session(next_date))
                next_time = period_start + time_rule.offset - datetime.timedelta(minutes=1)
            elif isinstance(time_rule, BeforeClose):
                period_end = my_calendar.session_close(my_calendar.minute_to_session(next_date))
                next_time = period_end - time_rule.offset

        if next_time is not None:
            log.info('Next execution of %s scheduled on %s New York time...' % (func.__name__, str(next_time.tz_convert("America/New_York").strftime("%Y-%m-%d %H:%M"))))
        elif next_date is not None:
            log.info('Next execution of %s scheduled on %s...' % (func.__name__, str(next_date)))
        else:
            log.info('Next execution of %s...' % func.__name__)

        return super().schedule_function(func, date_rule, time_rule, half_days, calendar)

    @api_method
    def get_environment(self, field='platform'):
        if field == 'arena':
            return 'live'
        return super(self.__class__, self).get_environment(field)

    def initialize(self, *args, **kwargs):
        if self.state_filename is None:
            raise ValueError("State filename cannot be none in live trading.")
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
        # log.info('current_dt: {}'.format(data.current_dt))
        super(self.__class__, self).handle_data(data)
        store_context(self.state_filename, context=self)

    def teardown(self):
        store_context(self.state_filename, context=self)

    def _create_clock(self):
        """Create a RealtimeClock synchronized to the broker's wall clock.

        Returns:
            RealtimeClock: Clock instance emitting events in real time.
        """
        # This method is taken from TradingAlgorithm.
        # The clock has been replaced to use RealtimeClock
        trading_o_and_c = self.trading_calendar.schedule.loc[
            self.sim_params.sessions]
        assert self.sim_params.data_frequency == 'minute'

        minutely_emission = True
        # All datetime in UTC without offset
        execution_opens = trading_o_and_c[MARKET_OPEN].dt.tz_localize(None)
        execution_closes = trading_o_and_c[MARKET_CLOSE].dt.tz_localize(None)
        before_trading_start_minutes = execution_opens - timedelta(minutes=_minutes_before_trading_starts)

        # open/close for NYSE:
        # 9:30 am EDT (13:30 UTC) 4:00 pm EDT (20:00 UTC)

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
        """Create the live trading event generator.

        Args:
            sim_params: Simulation parameters (uses self.sim_params in practice).

        Returns:
            Generator yielding trading events from the live executor.
        """
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
        """Create a metrics tracker initialized with live broker account values.

        Uses the broker's NetLiquidation as capital_base instead of
        simulation parameters.

        Returns:
            MetricsTracker: Tracker initialized with live account data.
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
        """Get the current portfolio state from the broker."""
        return self.broker.portfolio

    def updated_account(self):
        """Get the current account state from the broker."""
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
        end_date = (pd.Timestamp.utcnow() + timedelta(days=365*10)).normalize().tz_localize(None)
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

        log.info('Today: {}'.format(prev_session))
        log.info('Pipeline output date (previous session) : {}'.format(prev_session))

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
        """Round an order amount to the nearest integer.

        Args:
            amount: The order amount to round.

        Returns:
            int: Rounded order amount, or 0 if NaN.
        """
        if pd.isna(amount):
            return 0
        return int(round_if_near_integer(amount))

    def run_pipeline(self, pipeline, start_session, chunksize):
        """Run a pipeline for the current session only.

        Args:
            pipeline: The Pipeline to run.
            start_session: Session date to run the pipeline for.
            chunksize: Ignored in live mode.

        Returns:
            Tuple of (pipeline_output, start_session).
        """
        # In Live mode a Pipeline can be run only for the current session (end_session = start_session)
        return self.engine.run_pipeline(pipeline, start_session, start_session), start_session






