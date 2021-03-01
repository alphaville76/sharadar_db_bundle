import os, sys
from os import environ as env
from logbook import Logger, FileHandler, DEBUG, INFO, NOTSET, StreamHandler, set_datetime_format
from zipline.api import get_datetime
import datetime
import linecache
import os
import tracemalloc
from sharadar.util.mail import send_mail

# log in local time instead of UTC
set_datetime_format("local")
LOG_ENTRY_FMT = '[{record.time:%Y-%m-%d %H:%M:%S}] {record.level_name}: {record.message}'

now = datetime.datetime.now()
logfilename = os.path.join(env["HOME"], "log", "sharadar-zipline" + '_' + now.strftime('%Y-%m-%d_%H%M') + ".log")
log = Logger('sharadar_db_bundle')
log_file_handler = FileHandler(logfilename, level=DEBUG, bubble=True)
log_file_handler.format_string = LOG_ENTRY_FMT
log.handlers.append(log_file_handler)
log_std_handler = StreamHandler(sys.stdout, level=INFO)
log_std_handler.format_string = LOG_ENTRY_FMT
log.handlers.append(log_std_handler)


def log_top_mem_usage(logger, snapshot, key_type='lineno', limit=10):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)

    logger.info("Top %s lines" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        logger.info("#%s: %s:%s: %.1f KiB"
              % (index, frame.filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            logger.info('    %s' % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        logger.info("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    logger.info("Total allocated size: %.1f KiB" % (total / 1024))


class BacktestLogger(Logger):


    def __init__(self, filename, arena='backtest', logname='Backtest', level=NOTSET):
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

    def process_record(self, record):
        """
        use the date of the trading day for log purposes
        """
        super().process_record(record)
        if self.arena == 'live':
            send_mail(record.channel + " " + record.level_name, record.message)
        record.time = get_datetime()

if __name__ == '__main__':
    log.info("Hello World!")
    BacktestLogger(__file__, arena='live', logname="Myname").warn("Hello World!")

