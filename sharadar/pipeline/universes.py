"""Universe definition and management for Zipline pipeline screening.

This module provides tools for defining, computing, persisting, and loading
stock universes used in pipeline-based equity strategies. Universes filter
the investable asset space based on criteria such as exchange listing,
market capitalization, dollar volume, sector, and fundamental quality.

Key components:
    UniverseWriter: Computes and persists universe membership to SQLite.
    UniverseReader: Reads pre-computed universe membership from SQLite.
    NamedUniverse: A CustomFilter that loads universe membership at runtime.
    stocks_us: Base US stock filter with fundamental quality screens.
    base_universe: Extended filter adding liquidity and size constraints.
    update_universe: Convenience function to incrementally update a universe.
"""
import sqlite3
from contextlib import closing

import numpy as np
import pandas as pd
from click import progressbar
from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarReader
from sharadar.pipeline.engine import make_pipeline_engine
from sharadar.pipeline.factors import Exchange, Sector, IsDomesticCommonStock, MarketCap, Fundamentals, EV
from sharadar.util.logger import log
from sharadar.util.output_dir import get_data_dir
from zipline.pipeline import Pipeline, CustomFilter
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
    """Computes pipeline-based universes and writes membership to SQLite."""
    """Computes pipeline-based universes and writes membership to SQLite."""
    def __init__(self, universes_db_path=os.path.join(get_data_dir(), "universes.sqlite")):
        """Initialize UniverseWriter.

        Args:
            universes_db_path: Path to the universes SQLite database file.
        """
        """Initialize UniverseWriter.

        Args:
            universes_db_path: Path to the universes SQLite database file.
        """
        self.engine = make_pipeline_engine()
        self.universes_db_path = universes_db_path

    def write(self, universe_name, screen, pipe_start, pipe_end):
        """Compute and write universe membership to the database.

        Args:
            universe_name: Name for the universe table.
            screen: Pipeline filter defining universe membership.
            pipe_start: Start date for pipeline computation.
            pipe_end: End date for pipeline computation.
        """
        """Compute and write universe membership to the database.

        Args:
            universe_name: Name for the universe table.
            screen: Pipeline filter defining universe membership.
            pipe_start: Start date for pipeline computation.
            pipe_end: End date for pipeline computation.
        """
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
        """Execute the pipeline and return filtered stock DataFrame.

        Args:
            screen: Pipeline filter to apply.
            pipe_end: End date for pipeline computation.
            pipe_start: Start date for pipeline computation.

        Returns:
            DataFrame of stocks passing the screen.
        """
        """Execute the pipeline and return filtered stock DataFrame.

        Args:
            screen: Pipeline filter to apply.
            pipe_end: End date for pipeline computation.
            pipe_start: Start date for pipeline computation.

        Returns:
            DataFrame of stocks passing the screen.
        """
        pipe = Pipeline(columns={ }, screen = screen)
        stocks = self.engine.run_pipeline(pipe, pipe_start, pipe_end, chunksize=120)
        return stocks


class UniverseReader(object):
    """Reads pre-computed universe membership from a SQLite database."""
    """Reads pre-computed universe membership from a SQLite database."""
    def __init__(self, db_path=os.path.join(get_data_dir(), "universes.sqlite")):
        """Initialize UniverseReader.

        Args:
            db_path: Path to the universes SQLite database file.
        """
        """Initialize UniverseReader.

        Args:
            db_path: Path to the universes SQLite database file.
        """
        db = sqlite3.connect(db_path, isolation_level=None)
        db.row_factory = lambda cursor, row: row[0]
        self.cursor = db.cursor()

    def _query(self, sql):
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            c.execute(sql)
            return c.fetchall()

    def get_sid(self, universe_name, date):
        """Get security IDs in the universe for a given date.

        Args:
            universe_name: Name of the universe table.
            date: Date to query.

        Returns:
            numpy array of security IDs.
        """
        """Get security IDs in the universe for a given date.

        Args:
            universe_name: Name of the universe table.
            date: Date to query.

        Returns:
            numpy array of security IDs.
        """
        self.cursor.execute("SELECT sid FROM %s where date = '%s'" % (universe_name, date))
        return np.array(self.cursor.fetchall())

    def get_last_date(self, universe_name):
        """Get the most recent date in the universe table.

        Args:
            universe_name: Name of the universe table.

        Returns:
            Timestamp of the last date, or NaT if unavailable.
        """
        """Get the most recent date in the universe table.

        Args:
            universe_name: Name of the universe table.

        Returns:
            Timestamp of the last date, or NaT if unavailable.
        """
        sql = "SELECT MAX(date) FROM %s" % universe_name
        try:
            self.cursor.execute(sql)
        except:
            return pd.NaT
        res = self.cursor.fetchall()
        if len(res) == 0:
            return pd.NaT
        return pd.Timestamp(res[0])

def stocks_us(context):
    """Create a base US stock filter with fundamental quality screens.

    Applies filters for price, exchange, sector, domestic common stock,
    and positive revenue/assets/equity/enterprise value.

    Args:
        context: Context object containing PARAM dict with exclude_sectors.

    Returns:
        Combined pipeline Filter for US stocks.
    """
    """Create a base US stock filter with fundamental quality screens.

    Applies filters for price, exchange, sector, domestic common stock,
    and positive revenue/assets/equity/enterprise value.

    Args:
        context: Context object containing PARAM dict with exclude_sectors.

    Returns:
        Combined pipeline Filter for US stocks.
    """
    return (
            (USEquityPricing.close.latest > 3) &
            Exchange().element_of(['NYSE', 'NASDAQ', 'NYSEMKT']) &
            (Sector().notnull()) &
            (~Sector().element_of(context.PARAM['exclude_sectors'])) &
            (IsDomesticCommonStock().eq(1)) &
            (Fundamentals(field='revenue_arq') > 0) &
            (Fundamentals(field='assets_arq') > 0) &
            (Fundamentals(field='equity_arq') > 0) &
            (EV() > 0)
    )

def base_universe(context):
    """Create a filtered universe with liquidity and market cap requirements.

    Extends stocks_us by adding percentile-based filters on average
    dollar volume and market capitalization.

    Args:
        context: Context object containing PARAM dict with min_percentile.

    Returns:
        Combined pipeline Filter for the base universe.
    """
    """Create a filtered universe with liquidity and market cap requirements.

    Extends stocks_us by adding percentile-based filters on average
    dollar volume and market capitalization.

    Args:
        context: Context object containing PARAM dict with min_percentile.

    Returns:
        Combined pipeline Filter for the base universe.
    """
    min_percentile = context.PARAM['min_percentile']
    return (
        (stocks_us(context)) &
        (AverageDollarVolume(window_length=200).percentile_between(min_percentile, 100.0, mask=stocks_us(context))) &
        (MarketCap().percentile_between(min_percentile, 100.0, mask=stocks_us(context)))
    )

def context():
    """Create a default context object with standard screening parameters.

    Returns:
        Object with PARAM dict containing min_percentile and exclude_sectors.
    """
    """Create a default context object with standard screening parameters.

    Returns:
        Object with PARAM dict containing min_percentile and exclude_sectors.
    """
    class Object(object):
        pass
    ctx = Object()

    ctx.PARAM = {}
    ctx.PARAM['min_percentile'] = 20
    ctx.PARAM['exclude_sectors'] = ['Financial Services', 'Real Estate', 'Energy', 'Utilities']

    return ctx


class NamedUniverse(CustomFilter):
    """CustomFilter that loads a pre-computed universe from the SQLite database.

    Attributes:
        universe_name: Name of the universe table in the database.
    """
    """CustomFilter that loads a pre-computed universe from the SQLite database.

    Attributes:
        universe_name: Name of the universe table in the database.
    """
    inputs = []
    window_length = 1

    def __new__(self, universe_name):
        self.universe_name = universe_name

        universes_db_path = os.path.join(get_data_dir(), "universes.sqlite")
        self.universe_reader = UniverseReader(universes_db_path)
        return super(NamedUniverse, self).__new__(self)

    def compute(self, today, assets, out):
        sids = self.universe_reader.get_sid(self.universe_name, today.date())
        out[:] = assets.isin(sids)


def update_universe(name, screen):
    """Incrementally update or create a named universe in the database.

    Args:
        name: Name of the universe to update.
        screen: Pipeline filter defining universe membership.
    """
    """Incrementally update or create a named universe in the database.

    Args:
        name: Name of the universe to update.
        screen: Pipeline filter defining universe membership.
    """
    universe_start = pd.Timestamp('1998-10-16')
    universe_end = SQLiteDailyBarReader().last_available_dt
    universe_last_date = UniverseReader().get_last_date(name)
    if not pd.isnull(universe_last_date):
        universe_start = universe_last_date
    log.info("Start creating or updating universe '%s' from %s to %s ..." % (name, universe_start, universe_end))
    UniverseWriter().write(name, screen, universe_start, universe_end)


if __name__ == "__main__":
    screen = base_universe(context())


    update_universe(TRADABLE_STOCKS_US, screen)