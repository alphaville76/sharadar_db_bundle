import os
from os import environ as env
from logbook import Logger, FileHandler, DEBUG

FileHandler(os.path.join(env["HOME"], "log", "ingest_sep_db.log"), level=DEBUG).push_application()
log = Logger('sep_quandl_db.py')