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
from time import sleep
from sharadar.util.logger import log
import pandas as pd
from zipline.utils.date_utils import make_utc_aware
from zipline.gens.sim_engine import (
    BAR,
    SESSION_START,
    SESSION_END,
    MINUTE_END,
    BEFORE_TRADING_START_BAR
)

class RealtimeClock(object):
    """
    Realtime clock for live trading.

    This class is a drop-in replacement for
    :class:`zipline.gens.sim_engine.MinuteSimulationClock`.
    The key difference between the two is that the RealtimeClock's event
    emission is synchronized to the (broker's) wall time clock, while
    MinuteSimulationClock yields a new event on every iteration (regardless of
    wall clock).

    The :param:`time_skew` parameter represents the time difference between
    the Broker and the live trading machine's clock.
    """

    def __init__(self,
                 sessions,
                 execution_opens,
                 execution_closes,
                 before_trading_start_minutes,
                 minute_emission,
                 time_skew=pd.Timedelta("0s"),
                 is_broker_alive=None,
                 execution_id=None,
                 stop_execution_callback=None):
        today = pd.Timestamp.utcnow().tz_convert(None).date()
        beginning_of_today = pd.to_datetime(today, utc=None)
        self.sessions = sessions[(beginning_of_today <= sessions)]
        self.execution_opens_utc = execution_opens[(beginning_of_today <= execution_opens)]
        self.execution_closes_utc = execution_closes[(beginning_of_today <= execution_closes)]

        self.before_trading_start_minutes_utc = before_trading_start_minutes[
            (beginning_of_today <= before_trading_start_minutes)]

        self.minute_emission = minute_emission
        self.time_skew = time_skew
        self.is_broker_alive = is_broker_alive or (lambda: True)
        self._last_emit = None
        self._before_trading_start_bar_yielded = False
        self._execution_id = execution_id
        self._stop_execution_callback = stop_execution_callback

    def __iter__(self):
        if not len(self.sessions):
            return

        for index, session in enumerate(self.sessions):
            self._before_trading_start_bar_yielded = False

            yield make_utc_aware(session), SESSION_START

            if self._stop_execution_callback:
                if self._stop_execution_callback(self._execution_id):
                    break

            current_time_utc = pd.Timestamp.utcnow().tz_convert(None)
            server_time_utc = (current_time_utc + self.time_skew).floor('1 min')
            server_time_utc_offset = make_utc_aware(server_time_utc)
            if (self.is_broker_alive() and server_time_utc < self.execution_opens_utc[index] and index == 0) or \
                    (self.execution_closes_utc[index - 1] <= server_time_utc < self.execution_opens_utc[index]):
                # just for logging, the logic for the waiting is in the while loop
                log.info("Waiting for trading start....")
                log.info("Server Time UTC: %s" % str(server_time_utc))
                log.info("Execution opens UTC: %s" % str(self.execution_opens_utc[index]))
                log.info("Execution closes UTC: %s" % str(self.execution_closes_utc[index]))

            while self.is_broker_alive():
                if self._stop_execution_callback:  # put it here too, to break inner loop as well
                    if self._stop_execution_callback(self._execution_id):
                        break
                current_time_utc = pd.Timestamp.utcnow().tz_convert(None)
                server_time_utc = (current_time_utc + self.time_skew).floor('1 min')

                if (server_time_utc >= self.before_trading_start_minutes_utc[index] and
                        not self._before_trading_start_bar_yielded):
                    self._last_emit = server_time_utc
                    self._before_trading_start_bar_yielded = True
                    log.info("Before trading start...")
                    yield server_time_utc_offset, BEFORE_TRADING_START_BAR
                elif (server_time_utc < self.execution_opens_utc[index] and index == 0) or \
                        (self.execution_closes_utc[index - 1] <= server_time_utc <
                         self.execution_opens_utc[index]):
                    # Waiting for the start of the trading day:
                    # sleep anywhere between yesterday's close and today's open
                    sleep(1)
                elif self.execution_opens_utc[index] <= server_time_utc < self.execution_closes_utc[index]:
                    if self._last_emit is None or server_time_utc - self._last_emit >= pd.Timedelta('1 minute'):
                        self._last_emit = server_time_utc
                        yield server_time_utc_offset, BAR
                        if self.minute_emission:
                            yield server_time_utc_offset, MINUTE_END
                    else:
                        sleep(1)
                elif server_time_utc == self.execution_closes_utc[index]:
                    self._last_emit = server_time_utc
                    yield server_time_utc_offset, BAR
                    if self.minute_emission:
                        yield server_time_utc_offset, MINUTE_END
                    log.info("Session end")
                    yield server_time_utc_offset, SESSION_END
                    break
                elif server_time_utc > self.execution_closes_utc[index]:
                    break
                else:
                    # We should never end up in this branch
                    raise RuntimeError("Invalid state in RealtimeClock")
