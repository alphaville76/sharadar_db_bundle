import os
from sharadar.util.output_dir import get_output_dir
from sharadar.util.universe import UniverseReader
from zipline.pipeline import CustomFilter
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume
from sharadar.pipeline.factors import (
    Exchange,
    Sector,
    IsDomestic,
    EV,
    MarketCap,
    Fundamentals
)

TRADABLE_STOCKS_US = 'tradable_stocks_us'

def TradableStocksUS():
    return (
        (USEquityPricing.close.latest > 3) &
        Exchange().element_of(['NYSE', 'NASDAQ', 'NYSEMKT']) &
        (Sector().notnull()) &
        (~Sector().element_of(['Financial Services', 'Real Estate'])) &
        (IsDomestic().eq(1)) &
        (AverageDollarVolume(window_length=200) > 2.5e6) &
        (MarketCap() > 350e6) &
        (Fundamentals(field='revenue_arq') > 0) &
        (Fundamentals(field='assets_arq') > 0) &
        (Fundamentals(field='equity_arq') > 0) &
        (EV() > 0)
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