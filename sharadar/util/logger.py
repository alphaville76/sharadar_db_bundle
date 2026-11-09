"""Logging infrastructure for the sharadar_db_bundle trading system.

Provides two logger classes built on logbook:
- SharadarDbBundleLogger: General-purpose logger for the bundle (writes to
  file, stdout, and optionally systemd journal on Linux).
- AlgoLogger: Logger for backtests and live trading with a configurable clock
  and live-mode email notifications.
- NewRelicLogHandler: Optional forwarding for any Logbook logger, using an
  explicit algorithm name as the New Relic application name.
"""
import datetime
import os
import subprocess
import sys
import warnings
from importlib import import_module
from os import environ as env

from logbook import Logger, FileHandler, Handler, DEBUG, INFO, NOTSET, StreamHandler, set_datetime_format
from sharadar.util.mail import send_mail
from zipline.api import get_datetime
from zipline.utils.algo_instance import get_algo_instance
from pathlib import Path

# log in local time
set_datetime_format('local')
LOG_ENTRY_FMT = '[{record.time:%Y-%m-%d %H:%M:%S}] {record.level_name}: {record.message}'
LOG_LEVEL_MAP = {'CRITICAL': 2, 'ERROR': 3, 'WARNING': 4, 'NOTICE': 5, 'INFO': 6, 'DEBUG': 7, 'TRACE': 7}


def _get_log_dir():
    '''Get the log directory, defaulting to current directory if HOME is not set.'''
    home = env.get('HOME', env.get('USERPROFILE', '.'))
    return os.path.join(home, 'log')


class SharadarDbBundleLogger(Logger):
    def __init__(self, logname='sharadar_db_bundle', level=NOTSET):
        super().__init__(logname, level)

        now = datetime.datetime.now()

        log_dir = _get_log_dir()
        Path(log_dir).mkdir(parents=True, exist_ok=True)

        self.filename = os.path.join(log_dir,
                                     'sharadar-zipline' + '_' + now.strftime('%Y-%m-%d_%H%M') + '.log')

        ##############################################
        # Set here the log level for the file logger #
        ##############################################
        log_file_handler = FileHandler(self.filename, level=DEBUG, bubble=True)
        log_file_handler.format_string = LOG_ENTRY_FMT
        self.handlers.append(log_file_handler)

        #################################################
        # Set here the log level for the std out logger #
        #################################################
        log_std_handler = StreamHandler(sys.stdout, level=INFO)
        log_std_handler.format_string = LOG_ENTRY_FMT
        self.handlers.append(log_std_handler)

    def process_record(self, record):
        super().process_record(record)
        if os.name == 'posix':
            try:
                msg = str(record.message).encode('unicode_escape').decode('utf-8')
                msg = msg.replace('\n', ' ')
                msg = msg.replace('"', "'")
                cmd = ['systemd-cat', '-t', 'sharadar_db_bundle', '-p', str(LOG_LEVEL_MAP[record.level_name])]
                subprocess.run(cmd, input=msg.encode(), capture_output=True)
            except Exception:
                pass  # Don't fail logging if systemd-cat is unavailable


log = SharadarDbBundleLogger()


class NewRelicLogHandler(Handler):
    """Forward logs using NEW_RELIC_LICENSE_KEY and the supplied application name.

    Insert this handler at index 0 of logger.handlers so non-bubbling console
    handlers do not prevent forwarding. Existing file/console output is preserved.
    Call shutdown() in a finally block at the end of the executable entry point.
    Without a license key, warn and leave forwarding disabled. The optional
    newrelic package (agent >= 8.5.0) is required only when a license is set.
    Other NEW_RELIC_* settings remain configurable through the environment.

    Example:
        handler = NewRelicLogHandler(app_name='MyAlgorithm')
        logger.handlers.insert(0, handler)
        try:
            run_algorithm()
        finally:
            handler.shutdown()
    """

    def __init__(self, app_name, level=DEBUG):
        super().__init__(level=level, bubble=True)
        if not isinstance(app_name, str) or not app_name.strip():
            raise ValueError('New Relic application name must be a non-empty string.')
        self.app_name = app_name.strip()
        self.agent = None
        self.application = None

        if not env.get('NEW_RELIC_LICENSE_KEY', '').strip():
            warnings.warn(
                'New Relic log forwarding is disabled; set NEW_RELIC_LICENSE_KEY to enable it.',
                RuntimeWarning, stacklevel=2,
            )
            return

        try:
            agent = import_module('newrelic.agent')
        except ImportError as exc:
            raise RuntimeError(
                'NEW_RELIC_LICENSE_KEY is set, but the newrelic package could not be imported.'
            ) from exc
        if not hasattr(agent, 'record_log_event'):
            raise RuntimeError('New Relic log forwarding requires Python agent 8.5.0 or newer.')

        env['NEW_RELIC_APP_NAME'] = self.app_name
        env.setdefault('NEW_RELIC_APPLICATION_LOGGING_ENABLED', 'true')
        env.setdefault('NEW_RELIC_APPLICATION_LOGGING_FORWARDING_ENABLED', 'true')
        agent.initialize()
        self.application = agent.application(name=self.app_name)
        self.agent = agent

    def emit(self, record):
        if self.agent is not None:
            self.agent.record_log_event(
                record.message,
                level=record.level_name,
                application=self.application,
            )

    def shutdown(self, timeout=10):
        """Flush pending data and stop the process-wide New Relic agent."""
        if self.agent is not None:
            self.agent.shutdown_agent(timeout=timeout)


class AlgoLogger(Logger):
    """Log with the live wall clock or Zipline's simulation clock."""

    def __init__(self, filename, arena='backtest', logname='Backtest', level=NOTSET, record_time=None):
        super().__init__(logname, level)

        path, ext = os.path.splitext(filename)
        now = datetime.datetime.now()
        log_filename = path + '_' + now.strftime('%Y-%m-%d_%H%M') + '.log'
        file_handler = FileHandler(log_filename, level=DEBUG, bubble=True)
        file_handler.format_string = LOG_ENTRY_FMT
        self.handlers.append(file_handler)

        stream_handler = StreamHandler(sys.stdout, level=INFO)
        stream_handler.format_string = LOG_ENTRY_FMT
        self.handlers.append(stream_handler)

        self.arena = arena
        self.record_time = record_time

    def process_record(self, record):
        '''Use the date of the trading day for log purposes.'''
        super().process_record(record)
        if self.arena == 'live' and record.level >= INFO:
            send_mail(record.channel + ' ' + record.level_name, record.message)
        if self.record_time is not None:
            record.time = self.record_time()
        elif self.arena == 'live':
            # current wall clock, in local time like the other sharadar logs
            record.time = datetime.datetime.now()
        else:
            record.time = _simulation_time()


def _simulation_time():
    '''Zipline's clock; before the first bar (e.g. in initialize) it is a naive session date or unset.'''
    algo = get_algo_instance()
    dt = getattr(algo, 'datetime', None) if algo is not None else None
    if dt is None:
        return datetime.datetime.now(datetime.timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return get_datetime()


# Compatibility for algorithms still importing the previous name.
BacktestLogger = AlgoLogger


if __name__ == '__main__':
    test_log = SharadarDbBundleLogger()
    test_log.info('Hello World!')
    test_log.error('ciao')
    test_log.warning('ciao\nbello')

    import pandas as pd
    def log_time():
        return pd.to_datetime('today')
    AlgoLogger(__file__, arena='live', logname='Myname', record_time=log_time).warn('Hello World!')
