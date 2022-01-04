import datetime
import os
import subprocess
import sys
from os import environ as env

from logbook import Logger, FileHandler, DEBUG, INFO, NOTSET, StreamHandler, set_datetime_format
from sharadar.util.mail import send_mail
from zipline.api import get_datetime

# log in local time instead of UTC
set_datetime_format("local")
LOG_ENTRY_FMT = '[{record.time:%Y-%m-%d %H:%M:%S}] {record.level_name}: {record.message}'
LOG_LEVEL_MAP = {'CRITICAL': 2, 'ERROR': 3, 'WARNING': 4, 'NOTICE': 5, 'INFO': 6, 'DEBUG': 7, 'TRACE': 7}


class SharadarDbBundleLogger(Logger):
    def __init__(self, logname='sharadar_db_bundle', level=NOTSET):
        super().__init__(logname, level)

        now = datetime.datetime.now()
        self.filename = os.path.join(env["HOME"], "log",
                                   "sharadar-zipline" + '_' + now.strftime('%Y-%m-%d_%H%M') + ".log")

        log_file_handler = FileHandler(self.filename, level=DEBUG, bubble=True)
        log_file_handler.format_string = LOG_ENTRY_FMT
        self.handlers.append(log_file_handler)

        log_std_handler = StreamHandler(sys.stdout, level=INFO)
        log_std_handler.format_string = LOG_ENTRY_FMT
        self.handlers.append(log_std_handler)

    def process_record(self, record):
        super().process_record(record)
        if os.name == 'posix':
            msg = record.message.encode("unicode_escape").decode("utf-8")
            msg = msg.replace('\n', ' ')
            msg = msg.replace('"', "'")
            cmd = 'echo "%s" | systemd-cat -t sharadar_db_bundle -p %d' % (msg, LOG_LEVEL_MAP[record.level_name])
            subprocess.run(cmd, shell=True)


log = SharadarDbBundleLogger()


class BacktestLogger(Logger):
    def __init__(self, filename, arena='backtest', logname='Backtest', level=NOTSET, record_time=get_datetime):
        super().__init__(logname, level)

        path, ext = os.path.splitext(filename)
        now = datetime.datetime.now()
        log_filename = path + '_' + now.strftime('%Y-%m-%d_%H%M') + ".log"
        file_handler = FileHandler(log_filename, level=DEBUG, bubble=True)
        file_handler.format_string = LOG_ENTRY_FMT
        self.handlers.append(file_handler)

        stream_handler = StreamHandler(sys.stdout, level=INFO)
        stream_handler.format_string = LOG_ENTRY_FMT
        self.handlers.append(stream_handler)

        self.arena = arena
        self.record_time = record_time

    def process_record(self, record):
        """
        use the date of the trading day for log purposes
        """
        super().process_record(record)
        if self.arena == 'live' and record.level >= INFO:
            send_mail(record.channel + " " + record.level_name, record.message)
        record.time = self.record_time()


if __name__ == '__main__':
    log = SharadarDbBundleLogger()
    log.info("Hello World!")
    log.error("ciao")
    log.warning("ciao\nbello")


    import pandas as pd
    def log_time():
        return pd.to_datetime("today")
    BacktestLogger(__file__, arena='live', logname="Myname", record_time=log_time).warn("Hello World!")

