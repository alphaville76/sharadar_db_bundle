from sharadar.pipeline.engine import WithAssetFinder
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import CustomFactor
from zipline.pipeline.classifiers import CustomClassifier
from zipline.lib.labelarray import LabelArray
import numpy as np
from zipline.utils.numpy_utils import object_dtype


class Fundamentals(CustomFactor, WithAssetFinder):
    inputs = []
    window_length = 1
    params = ('field',)
    window_safe = True

    def compute(self, today, assets, out, field):
        field_name = field
        if '_' not in field_name:
            field_name += '_arq'
        out[:] = self.get_asset_finder().get_fundamentals(assets, field_name, today, n=self.window_length)


class FundamentalsTTM(Fundamentals):
    def compute(self, today, assets, out, field):
        out[:] = self.get_asset_finder().get_fundamentals_ttm(assets, field, today, k=self.window_length)


class AbstractClassifier(CustomClassifier, WithAssetFinder):
    inputs = []
    window_length = 1
    dtype = object_dtype
    missing_value = 'NA'

    def __init__(self, categories, field):
        self.categories = categories
        self.field = field

    def _allocate_output(self, windows, shape):
        return LabelArray(np.full(shape, self.missing_value), self.missing_value, categories=self.categories)

    def compute(self, today, assets, out, *arrays):
        data = self.get_asset_finder().get_info(assets, self.field, today)
        out[:] = LabelArray(data, self.missing_value, categories=self.categories)


class Exchange(AbstractClassifier):
    def __init__(self):
        categories = ['NYSE', 'NASDAQ', 'OTC', 'NYSEMKT', 'NYSEARCA', 'BATS']
        field = 'exchange'
        super().__init__(categories, field)


class Sector(AbstractClassifier):
    def __init__(self):
        categories = ['Healthcare', 'Basic Materials', 'Financial Services', 'Consumer Cyclical', 'Technology',
                      'Consumer Defensive', 'Industrials', 'Real Estate', 'Energy', 'Communication Services',
                      'Utilities']
        field = 'sector'
        super().__init__(categories, field)


class IsDomestic(CustomClassifier, WithAssetFinder):
    inputs = []
    window_length = 1
    dtype = np.int64
    missing_value = 0

    def compute(self, today, assets, out, *arrays):
        category = self.get_asset_finder().get_info(assets, 'category', today)
        out[:] = np.isin(category, ['Domestic', 'Domestic Primary'])


class AvgMarketCap(CustomFactor, WithAssetFinder):
    inputs = [USEquityPricing.close]
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out, close):
        sharesbas = self.get_asset_finder().get_fundamentals_df_window_length(assets, 'sharesbas_arq', today,
                                                                              self.window_length)
        sharefactor = self.get_asset_finder().get_fundamentals_df_window_length(assets, 'sharefactor_arq', today,
                                                                                self.window_length)
        shares = (sharefactor * sharesbas).values
        out[:] = np.nanmean(close * shares, axis=0)


class MarketCap(CustomFactor):
    inputs = [USEquityPricing.close, Fundamentals(field='sharesbas_arq'), Fundamentals(field='sharefactor_arq')]
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out, close, sharesbas, sharefactor):
        out[:] = close * sharefactor * sharesbas


class EV(CustomFactor):
    """
    Enterprise value is a measure of the value of a business as a whole; calculated as [MarketCap] plus [DebtUSD] minus [CashnEqUSD].
    """
    inputs = [MarketCap(), Fundamentals(field='debtusd_arq'), Fundamentals(field='cashnequsd_arq')]
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out, mkt_cap, debtusd, cashnequsd):
        out[:] = mkt_cap + debtusd - cashnequsd