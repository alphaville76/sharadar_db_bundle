import sqlite3
from contextlib import closing

import numpy as np
import pandas as pd
from click import progressbar
from sharadar.pipeline.engine import make_pipeline_engine
from sharadar.util.logger import log
from zipline.pipeline import Pipeline
from singleton_decorator import singleton


SCHEMA = """
CREATE TABLE IF NOT EXISTS "%s" (
  "date" TIMESTAMP NOT NULL,
  "sid" INTEGER NOT NULL,
  
  PRIMARY KEY (date, sid)
);
CREATE INDEX "ix_date_%s" ON "%s" ("date");
"""


class UniverseWriter(object):
    def __init__(self, universes_db_path):
        self.engine = make_pipeline_engine()
        self.universes_db_path = universes_db_path

    def write(self, universe_name, screen, pipe_start, pipe_end):
        log.info("Computing pipeline from %s to %s..." % (pipe_start, pipe_end))
        stocks = self._execute_pipeline(screen, pipe_end, pipe_start)


        # Create schema, if not exists
        with closing(sqlite3.connect(self.universes_db_path)) as con, con, closing(con.cursor()) as c:
            c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='%s'" % universe_name)
            if c.fetchone()[0] == 0:
                c.executescript(SCHEMA % (universe_name, universe_name, universe_name))

            log.info("Inserting %d SIDs..." % len(stocks.index))
            with progressbar(stocks.index, show_pos=True) as bar:
                for i in bar:
                    c.execute("INSERT OR REPLACE INTO %s VALUES ('%s', %d);" % (universe_name, i[0].date(), i[1].sid))

        log.info("Universe '%s' successful created/updated" % universe_name)


    def _execute_pipeline(self, screen, pipe_end, pipe_start):
        pipe = Pipeline(columns={ }, screen = screen)
        stocks = self.engine.run_pipeline(pipe, pipe_start, pipe_end, chunksize=-1)
        return stocks

@singleton
class UniverseReader(object):
    def __init__(self, db_path):
        db = sqlite3.connect(db_path, isolation_level=None)
        db.row_factory = lambda cursor, row: row[0]
        self.cursor = db.cursor()

    def _query(self, sql):
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            c.execute(sql)
            return c.fetchall()

    def get_sid(self, universe_name, date):
        self.cursor.execute("SELECT sid FROM %s where date = '%s'" % (universe_name, date))
        return np.array(self.cursor.fetchall())

    def get_last_date(self, universe_name):
        sql = "SELECT MAX(date) FROM %s" % universe_name
        try:
            self.cursor.execute(sql)
        except:
            return pd.NaT
        res = self.cursor.fetchall()
        if len(res) == 0:
            return pd.NaT
        return pd.Timestamp(res[0], tz='UTC')

