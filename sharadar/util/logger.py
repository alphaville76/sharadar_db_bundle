import os, sys
from os import environ as env
from logbook import Logger, FileHandler, DEBUG, INFO, StreamHandler

logfilename = os.path.join(env["HOME"], "log", "sharadar-zipline.log")
log = Logger('sharadar_db_bundle')
log.handlers.append(FileHandler(logfilename, level=DEBUG, bubble=True))
log.handlers.append(StreamHandler(sys.stdout, level=INFO))

