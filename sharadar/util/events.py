from abc import ABCMeta

import numpy as np
import pandas as pd
import six
from zipline.utils.events import Always, NthTradingDayOfWeek, NDaysBeforeLastTradingDayOfWeek, AfterOpen, BeforeClose
from zipline.utils.events import lossless_float_to_int, _out_of_range_error, MAX_MONTH_RANGE, StatelessRule
from zipline.utils.input_validation import preprocess
from zipline.utils.memoize import lazyval
from zipline.utils.calendar_utils import get_calendar
from zipline.api import get_environment

class OnceAtStart(StatelessRule):

    def __init__(self):
        self.already_trigged = False

    def should_trigger(self, dt):
        if not self.already_trigged:
            self.already_trigged = True
            return True
        return False


class time_rules(object):
    every_minute = Always

    @staticmethod
    def market_open(offset=None, hours=None, minutes=None):
        return AfterOpen(offset=offset, hours=hours, minutes=minutes)

    @staticmethod
    def market_close(offset=None, hours=None, minutes=None):
        return BeforeClose(offset=offset, hours=hours, minutes=minutes)

    live_algo_start = OnceAtStart


class date_rules(object):
    every_day = Always

    @staticmethod
    def quarter_start(days_offset=0, rebalance_months = [1, 4, 7, 10]):
        # Most companies file at the end of January, April, July and October (best results).
        return TradingDayOfMonthRule(days_offset, False, rebalance_months)

    @staticmethod
    def quarter_end(days_offset=0, rebalance_months = [1, 4, 7, 10]):
        return TradingDayOfMonthRule(days_offset, True, rebalance_months)

    @staticmethod
    def month_start(days_offset=0):
        return TradingDayOfMonthRule(days_offset, False)

    @staticmethod
    def month_end(days_offset=0):
        return TradingDayOfMonthRule(n=days_offset, invert=True)

    @staticmethod
    def week_start(days_offset=0):
        return NthTradingDayOfWeek(n=days_offset)

    @staticmethod
    def week_end(days_offset=0):
        return NDaysBeforeLastTradingDayOfWeek(n=days_offset)

class TradingDayOfMonthRule(six.with_metaclass(ABCMeta, StatelessRule)):

    @preprocess(n=lossless_float_to_int('TradingDayOfMonthRule'))
    def __init__(self, n, invert, rebalance_months=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]):
        self.rebalance_months = rebalance_months
        if not 0 <= n < MAX_MONTH_RANGE:
            raise _out_of_range_error(MAX_MONTH_RANGE)
        if invert:
            self.td_delta = -n - 1
        else:
            self.td_delta = n

        #TODO check why self.cal could be null
        if self.cal is None:
            self.cal = get_calendar('XNYS', start=pd.Timestamp('2000-01-01 00:00:00'))

    def should_trigger(self, dt):
        dt_to_use = dt
        if dt_to_use.month not in self.rebalance_months:
            return False

        arena = get_environment(field='arena')
        if arena == 'backtest':
            dt_to_use = dt_to_use - pd.Timedelta(seconds=1)

        value = self.cal.minute_to_session(dt_to_use).value

        # is this market minute's period in the list of execution periods?
        return value in self.execution_period_values

    @lazyval
    def execution_period_values(self):
        # calculate the list of periods that match the given criteria
        sessions = self.cal.sessions
        return set(
            pd.Series(data=sessions)
            .groupby([sessions.year, sessions.month])
            .nth(self.td_delta)
            .astype(np.int64)
        )
