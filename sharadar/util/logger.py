"""Logging infrastructure for the sharadar_db_bundle trading system.

Provides two logger classes built on logbook:
- SharadarDbBundleLogger: General-purpose logger for the bundle (writes to
  file, stdout, and optionally systemd journal on Linux).
- BacktestLogger: Specialized logger for backtests and live trading that
  timestamps log entries with the simulated trading day.
"""
import datetime
import os
import subprocess
import sys
from os import environ as env

from logbook import Logger, FileHandler, DEBUG, INFO, NOTSET, StreamHandler, set_datetime_format
from sharadar.util.mail import send_mail
from zipline.api import get_datetime
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
        log_std_handler = StreamHandler(sys.stdout, level=DEBUG)
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


class BacktestLogger(Logger):
    def __init__(self, filename, arena='backtest', logname='Backtest', level=NOTSET, record_time=None):
        super().__init__(logname, level)

        if record_time is None:
            record_time = get_datetime

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
        record.time = self.record_time()


if __name__ == '__main__':
    test_log = SharadarDbBundleLogger()
    test_log.info('Hello World!')
    test_log.error('ciao')
    test_log.warning('ciao\nbello')

    import pandas as pd
    def log_time():
        return pd.to_datetime('today')
    BacktestLogger(__file__, arena='live', logname='Myname', record_time=log_time).warn('Hello World!')
