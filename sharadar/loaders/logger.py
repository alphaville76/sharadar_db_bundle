import os, sys
from os import environ as env
from logbook import Logger, FileHandler, DEBUG, StreamHandler

handler = FileHandler(os.path.join(env["HOME"], "log", "ingest_sharadar.log"), level=DEBUG)
handler.push_application()
StreamHandler(sys.stdout, bubble=True).push_application()
log = Logger('sharadar_db_bundle')
