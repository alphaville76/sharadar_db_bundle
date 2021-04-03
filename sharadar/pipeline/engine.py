import datetime
import time
import os
import pandas as pd
from sharadar.util.cache import cached
from sharadar.pipeline.fx import SimpleFXRateReader
from sharadar.data.sql_lite_assets import SQLiteAssetFinder
from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarReader
from sharadar.util.output_dir import SHARADAR_BUNDLE_NAME, SHARADAR_BUNDLE_DIR
from sharadar.util.logger import log
from six import iteritems
from toolz import juxt, groupby
from zipline.data.bundles.core import BundleData, asset_db_path, adjustment_db_path
from zipline.data.adjustments import SQLiteAdjustmentReader
from zipline.pipeline import SimplePipelineEngine
from zipline.pipeline.loaders.equity_pricing_loader import USEquityPricingLoader
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.term import LoadableTerm
from zipline.utils import paths as pth
from zipline.pipeline.hooks.progress import ProgressHooks
from zipline.pipeline.domain import US_EQUITIES
from zipline.data.data_portal import DataPortal

def to_string(obj):
    try:
        return str(obj)
    except AttributeError:
        return type(obj).__name__


class BundlePipelineEngine(SimplePipelineEngine):
    def __init__(self, get_loader, asset_finder, default_domain=US_EQUITIES, populate_initial_workspace=None,
                 default_hooks=None):
        super().__init__(get_loader, asset_finder, default_domain, populate_initial_workspace, default_hooks)

    def run_pipeline(self, pipeline, start_date, end_date=None, chunksize=120, hooks=None):
        if end_date is None:
            end_date = start_date

        if hooks is None:
            hooks = [ProgressHooks.with_static_publisher(CliProgressPublisher())]

        if chunksize <= 1:
            log.info("Compute pipeline values without chunks.")
            return super().run_pipeline(pipeline, start_date, end_date, hooks)

        return super().run_chunked_pipeline(pipeline, start_date, end_date, chunksize, hooks)


class BundleLoader:
    _asset_finder = None
    _bar_reader = None

    def asset_finder(self):
        if self._asset_finder is None:
            self._asset_finder = _asset_finder()

        return self._asset_finder

    def bar_reader(self):
        if self._bar_reader is None:
            self._bar_reader = _bar_reader()

        return self._bar_reader

def daily_equity_path(bundle_name, timestr, environ=None):
    return pth.data_path(
        (bundle_name, timestr, 'prices.sqlite'),
        environ=environ,
    )

@cached
def load_sharadar_bundle(name=SHARADAR_BUNDLE_NAME, timestr=SHARADAR_BUNDLE_DIR, environ=os.environ):
    return BundleData(
        asset_finder = SQLiteAssetFinder(asset_db_path(name, timestr, environ=environ),),
        equity_minute_bar_reader = None,
        equity_daily_bar_reader = SQLiteDailyBarReader(daily_equity_path(name, timestr, environ=environ),),
        adjustment_reader = SQLiteAdjustmentReader(adjustment_db_path(name, timestr, environ=environ),),
    )


@cached
def _asset_finder(name=SHARADAR_BUNDLE_NAME, timestr=SHARADAR_BUNDLE_DIR, environ=os.environ):
    return SQLiteAssetFinder(asset_db_path(name, timestr, environ=environ))

@cached
def _bar_reader(name=SHARADAR_BUNDLE_NAME, timestr=SHARADAR_BUNDLE_DIR, environ=os.environ):
    return SQLiteDailyBarReader(daily_equity_path(name, timestr, environ=environ))

@cached
def symbol(ticker, as_of_date=None):
    return _asset_finder().lookup_symbol(ticker, as_of_date)

@cached
def symbols(tickers, as_of_date=None):
    return _asset_finder().lookup_symbols(tickers, as_of_date)

@cached
def sector(ticker, as_of_date=None):
    return _asset_finder().get_info(symbol(ticker).sid, 'sector')

@cached
def sectors(tickers, as_of_date=None):
    sids = [x.sid for x in symbols(tickers)]
    return _asset_finder().get_info(sids, 'sector')

@cached
def sid(sid):
    return sids((sid,))[0]


@cached
def sids(sids):
    return _asset_finder().retrieve_all(sids)


def make_pipeline_engine(bundle=None, start=None, end=None, live=False):
    """Creates a pipeline engine for the dates in (start, end).
    Using this allows usage very similar to run_pipeline in Quantopian's env."""
    if bundle is None:
        bundle = load_sharadar_bundle()

    if start is None:
        start = bundle.equity_daily_bar_reader.first_trading_day

    if end is None:
        end = pd.to_datetime('today', utc=True)

    #pipeline_loader = USEquityPricingLoader(bundle.equity_daily_bar_reader, bundle.adjustment_reader, SimpleFXRateReader())
    pipeline_loader = USEquityPricingLoader.without_fx(bundle.equity_daily_bar_reader, bundle.adjustment_reader)


    def choose_loader(column):
        if column in USEquityPricing.columns:
            return pipeline_loader
        raise ValueError("No PipelineLoader registered for column %s." % column)

    bundle.asset_finder.is_live_trading = live
    spe = BundlePipelineEngine(get_loader=choose_loader, asset_finder=bundle.asset_finder)
    return spe


def trading_date(date):
    """
    Given a date, return the same date if a trading session or the next valid one
    """
    if isinstance(date, str):
        date = pd.to_datetime(date, utc=True)
    cal = _bar_reader().trading_calendar
    if not cal.is_session(date):
        date = cal.next_close(date)
        # trick to fix the time (from 21:00 to 00:00)
        date = pd.to_datetime(date.date(), utc=True)
    return date


def to_sids(assets):
    if hasattr(assets, '__iter__'):
        return [x.sid for x in assets]
    return [assets.sid]

def prices(assets, start, end, field='close', offset=0):
    """
    Get price data for assets between start and end.
    """
    start = trading_date(start)
    end = trading_date(end)

    bundle = load_sharadar_bundle()
    trading_calendar = bundle.equity_daily_bar_reader.trading_calendar

    if offset > 0:
        start = trading_calendar.sessions_window(start, -offset)[0]

    bar_count = trading_calendar.session_distance(start, end)

    data_portal = DataPortal(bundle.asset_finder,
                             trading_calendar=trading_calendar,
                             first_trading_day=start,
                             equity_daily_reader=bundle.equity_daily_bar_reader,
                             adjustment_reader=bundle.adjustment_reader)

    df = data_portal.get_history_window(assets=assets, end_dt=end, bar_count=bar_count,
                                             frequency='1d',
                                             field=field,
                                             data_frequency='daily')

    return df if len(assets) > 1 else df.squeeze()

def history(assets, as_of_date, n, field='close'):
    as_of_date = trading_date(as_of_date)
    trading_calendar = load_sharadar_bundle().equity_daily_bar_reader.trading_calendar
    sessions = trading_calendar.sessions_window(as_of_date, -n + 1)
    return prices(assets, sessions[0], sessions[-1], field)

def returns(assets, start, end, periods=1, field='close'):
    """
    Fetch returns for one or more assets in a date range.
    """
    df = prices(assets, start, end, field, periods)
    df = df.sort_index().pct_change(1).iloc[1:]
    return df


class CliProgressPublisher(object):

    def publish(self, model):
        try:
            start = str(model.current_chunk_bounds[0].date())
            end = str(model.current_chunk_bounds[1].date())
            completed = model.percent_complete
            work = model.current_work
            if start == end:
                log.info("Percent completed: %3.0f%% (%s): %s" % (completed, start, work))
            else:
                log.info("Percent completed: %3.0f%% (%s - %s): %s" % (completed, start, end, work))
        except:
            log.error("Cannot publish progress state.")