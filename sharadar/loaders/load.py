import os
import pandas as pd
from zipline.data.bundles.core import BundleData, asset_db_path, adjustment_db_path, from_bundle_ingest_dirname
from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarReader, SQLiteAssetFinder
from sharadar.loaders.ingest import SEP_BUNDLE_NAME, SEP_BUNDLE_DIR
from zipline.data.us_equity_pricing import SQLiteAdjustmentReader
import zipline.utils.paths as pth
from toolz import complement
import errno
from zipline.pipeline import SimplePipelineEngine, USEquityPricingLoader
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import CustomFactor
from zipline.pipeline.classifiers import CustomClassifier
from zipline.lib.labelarray import LabelArray
import numpy as np
from zipline.utils.numpy_utils import object_dtype
from memoization import cached

def most_recent_data(bundle_name, timestamp, environ=None):
        """Get the path to the most recent data after ``date``for the
        given bundle.

        Parameters
        ----------
        bundle_name : str
            The name of the bundle to lookup.
        timestamp : datetime
            The timestamp to begin searching on or before.
        environ : dict, optional
            An environment dict to forward to zipline_root.
        """
        #if bundle_name not in bundles:
        #    raise UnknownBundle(bundle_name)

        try:
            candidates = os.listdir(
                pth.data_path([bundle_name], environ=environ),
            )
            return pth.data_path(
                [bundle_name,
                 max(
                     filter(complement(pth.hidden), candidates),
                     key=from_bundle_ingest_dirname,
                 )],
                environ=environ,
            )
        except (ValueError, OSError) as e:
            if getattr(e, 'errno', errno.ENOENT) != errno.ENOENT:
                raise
            raise ValueError(
                'no data for bundle {bundle!r} on or before {timestamp}\n'
                'maybe you need to run: $ zipline ingest -b {bundle}'.format(
                    bundle=bundle_name,
                    timestamp=timestamp,
                ),
            )

def daily_equity_path(bundle_name, timestr, environ=None):
    return pth.data_path(
        (bundle_name, timestr, 'prices.sqlite'),
        environ=environ,
    )


def load_sep_bundle():
        """Loads a previously ingested bundle.

        Parameters
        ----------
        name : str
            The name of the bundle.
        environ : mapping, optional
            The environment variables. Defaults of os.environ.
        timestamp : datetime, optional
            The timestamp of the data to lookup.
            Defaults to the current time.

        Returns
        -------
        bundle_data : BundleData
            The raw data readers for this bundle.
        """
        name = SEP_BUNDLE_NAME
        timestr = SEP_BUNDLE_DIR
        environ = os.environ
        #timestr = most_recent_data(name, timestamp=pd.Timestamp.utcnow(), environ=environ)
        return BundleData(
            asset_finder=SQLiteAssetFinder(
                asset_db_path(name, timestr, environ=environ),
            ),
            equity_minute_bar_reader=None,
            equity_daily_bar_reader=SQLiteDailyBarReader(
                daily_equity_path(name, timestr, environ=environ),
            ),
            adjustment_reader=SQLiteAdjustmentReader(
                adjustment_db_path(name, timestr, environ=environ),
            ),
        )


def _createAssetFinder(name, timestr=None, environ=os.environ, timestamp=pd.Timestamp.utcnow()):
    if timestr is None:
        timestr = most_recent_data(name, timestamp, environ=environ)
    return SQLiteAssetFinder(asset_db_path(name, timestr, environ=environ))

@cached
def symbol(ticker, as_of_date=None):
    return _createAssetFinder(SEP_BUNDLE_NAME, timestr=SEP_BUNDLE_DIR).lookup_symbol(ticker, as_of_date)

@cached
def symbols(ticker, as_of_date=None):
    return _createAssetFinder(SEP_BUNDLE_NAME, timestr=SEP_BUNDLE_DIR).lookup_symbols(ticker, as_of_date)

@cached
def sid(sid):
    return sids((sid,))[0]

@cached
def sids(sids):
    return _createAssetFinder(SEP_BUNDLE_NAME, timestr=SEP_BUNDLE_DIR).retrieve_all(sids)

class BundlePipelineEngine(SimplePipelineEngine):
    def run_pipeline(self, pipeline, start_date, end_date):
        for factor in pipeline.columns.values():
            if isinstance(factor, WithAssetFinder):
                factor.set_asset_finder(self._finder)
            for factor_input in factor.inputs:
                if isinstance(factor_input, WithAssetFinder):
                    factor_input.set_asset_finder(self._finder)
        
        if pipeline.screen is not None:
            for factor in pipeline.screen.inputs:
                if isinstance(factor, WithAssetFinder):
                    factor.set_asset_finder(self._finder)
                
        return super().run_pipeline(pipeline, start_date, end_date)

        
def make_pipeline_engine(start, end):
    """Creates a pipeline engine for the dates in (start, end).
    Using this allows usage very similar to run_pipeline in Quantopian's env."""

    bundle = load_sep_bundle()

    pipeline_loader = USEquityPricingLoader(bundle.equity_daily_bar_reader, bundle.adjustment_reader)

    def choose_loader(column):
        if column in USEquityPricing.columns:
            return pipeline_loader
        raise ValueError("No PipelineLoader registered for column %s." % column)

    # set up pipeline
    cal = bundle.equity_daily_bar_reader.trading_calendar.all_sessions
    cal2 = cal[(cal >= start) & (cal <= end)]

    spe = BundlePipelineEngine(get_loader=choose_loader,
                               calendar=cal2,
                               asset_finder=bundle.asset_finder)
    return spe


class WithAssetFinder:
  def set_asset_finder(self, asset_finder):
      pass

class Fundamentals(CustomFactor, WithAssetFinder):
    inputs = []
    window_length = 1
    params = ('field',)
    window_safe = True
    
    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self._asset_finder = None
        
    def set_asset_finder(self, asset_finder):
        self._asset_finder = asset_finder
        
    def compute(self, today, assets, out, field):
        out[:] = self._asset_finder.get_fundamentals(assets,  field, today, n=self.window_length)
      
class FundamentalsTTM(Fundamentals):
    def compute(self, today, assets, out, field):
        out[:] = self._asset_finder.get_fundamentals_ttm(assets, field, today, k=self.window_length)

class AbstractClassifier(CustomClassifier, WithAssetFinder):
    inputs = []
    window_length = 1  
    dtype = object_dtype
    missing_value = 'NA'
    
    def __init__(self, categories, field): 
        self.categories = categories
        self.field = field        
        self._asset_finder = None
        
    def set_asset_finder(self, asset_finder):
        self._asset_finder = asset_finder
    
    def _allocate_output(self, windows, shape):
        return LabelArray(np.full(shape, self.missing_value), self.missing_value, categories=self.categories)
       
    def compute(self, today, assets, out, *arrays):
        data = self._asset_finder.get_info(assets, self.field, today)
        out[:] = LabelArray(data, self.missing_value, categories=self.categories)

class Exchange(AbstractClassifier):
    def __init__(self): 
        categories = ['NYSE', 'NASDAQ', 'OTC', 'NYSEMKT', 'NYSEARCA', 'BATS']
        field = 'exchange'
        super().__init__(categories, field)
        
class Sector(AbstractClassifier):
    def __init__(self): 
        categories = ['Healthcare', 'Basic Materials', 'Financial Services', 'Consumer Cyclical', 'Technology', 'Consumer Defensive', 'Industrials', 'Real Estate', 'Energy', 'Communication Services', 'Utilities']
        field = 'sector'
        super().__init__(categories, field)

class IsDomestic(CustomClassifier, WithAssetFinder):
    inputs = []
    window_length = 1  
    dtype = np.int64
    missing_value = 0
        
    def set_asset_finder(self, asset_finder):
        self._asset_finder = asset_finder

    def compute(self, today, assets, out, *arrays):
        category = self._asset_finder.get_info(assets, 'category', today)
        out[:] = np.isin(category, ['Domestic', 'Domestic Primary'])

class AvgMarketCap(CustomFactor, WithAssetFinder):
    inputs = [USEquityPricing.close]
    window_length = 1
    window_safe = True

    def __init__(self, *args, **kwargs):
        super(AvgMarketCap, self).__init__(*args, **kwargs)
        self._asset_finder = None
        
    def set_asset_finder(self, asset_finder):
        self._asset_finder = asset_finder
        
    def compute(self, today, assets, out, close):
        sharesbas = self._asset_finder.get_fundamentals_df_window_length(assets, 'sharesbas_arq', today, self.window_length)
        sharefactor = self._asset_finder.get_fundamentals_df_window_length(assets, 'sharefactor_arq', today, self.window_length)
        shares = (sharefactor*sharesbas).values
        out[:] = np.nanmean(close*shares, axis=0)
        
class MarketCap(CustomFactor, WithAssetFinder):
    inputs = [USEquityPricing.close, Fundamentals(field='sharesbas_arq'), Fundamentals(field='sharefactor_arq')]
    window_length = 1
    window_safe = False
    
    def set_asset_finder(self, asset_finder):
        self._asset_finder = asset_finder
            
    def compute(self, today, assets, out, close, sharesbas, sharefactor):
        out[:] = close*sharefactor*sharesbas 