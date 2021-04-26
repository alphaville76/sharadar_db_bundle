import sqlite3
from contextlib import closing

import numpy as np
import pandas as pd
from click import progressbar
from sharadar.pipeline.engine import make_pipeline_engine
from sharadar.pipeline.factors import Exchange, Sector, IsDomesticCommonStock, MarketCap, Fundamentals, EV
from sharadar.util.logger import log
from sharadar.util.output_dir import get_output_dir
from zipline.pipeline import Pipeline, CustomFilter
from singleton_decorator import singleton
import os
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume

TRADABLE_STOCKS_US = 'tradable_stocks_us'

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
        stocks = self.engine.run_pipeline(pipe, pipe_start, pipe_end, chunksize=120)
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


def create_tradable_stocks_universe(output_dir, prices_start, prices_end):
    universes_dbpath = os.path.join(output_dir, "universes.sqlite")
    universe_name = TRADABLE_STOCKS_US
    screen = TradableStocksUS()
    universe_start = prices_start.tz_localize('utc')
    universe_end = prices_end.tz_localize('utc')
    universe_last_date = UniverseReader(universes_dbpath).get_last_date(universe_name)
    if not pd.isnull(universe_last_date):
        universe_start = universe_last_date
    log.info("Start creating universe '%s' from %s to %s ..." % (universe_name, universe_start, universe_end))
    UniverseWriter(universes_dbpath).write(universe_name, screen, universe_start, universe_end)


def StocksUS():
    return (
            (USEquityPricing.close.latest > 3) &
            Exchange().element_of(['NYSE', 'NASDAQ', 'NYSEMKT']) &
            (Sector().notnull()) &
            (~Sector().element_of(['Financial Services', 'Real Estate', 'Energy', 'Utilities'])) &
            (IsDomesticCommonStock().eq(1)) &
            (Fundamentals(field='revenue_arq') > 0) &
            (Fundamentals(field='assets_arq') > 0) &
            (Fundamentals(field='equity_arq') > 0) &
            (EV() > 0)
    )

def TradableStocksUS(min_percentile = 30):
    return (
        (StocksUS()) &
        (AverageDollarVolume(window_length=200).percentile_between(min_percentile, 100.0, mask=StocksUS())) &
        (MarketCap().percentile_between(min_percentile, 100.0, mask=StocksUS()))
    )


class NamedUniverse(CustomFilter):
    inputs = []
    window_length = 1

    def __new__(self, universe_name):
        self.universe_name = universe_name

        universes_db_path = os.path.join(get_output_dir(), "universes.sqlite")
        self.universe_reader = UniverseReader(universes_db_path)
        return super(NamedUniverse, self).__new__(self)

    def compute(self, today, assets, out):
        sids = self.universe_reader.get_sid(self.universe_name, today.date())
        out[:] = assets.isin(sids)


if __name__ == "__main__":
    universe_start = pd.to_datetime('1998-10-16', utc=True)
    universe_end = pd.to_datetime('2020-12-30', utc=True)

    from sharadar.util.output_dir import get_output_dir
    universes_dbpath = os.path.join(get_output_dir(), "universes.sqlite")
    universe_name = TRADABLE_STOCKS_US
    screen = TradableStocksUS()
    universe_last_date = UniverseReader(universes_dbpath).get_last_date(universe_name)
    if not pd.isnull(universe_last_date):
        universe_start = universe_last_date
    log.info("Start creating universe '%s' from %s to %s ..." % (universe_name, universe_start, universe_end))
    UniverseWriter(universes_dbpath).write(universe_name, screen, universe_start, universe_end)