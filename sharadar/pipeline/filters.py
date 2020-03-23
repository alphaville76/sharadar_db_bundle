from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume
from sharadar.loaders.load import (
    Exchange,
    Sector,
    IsDomestic,
    AvgMarketCap,
    MarketCap,
)

def TradableStocksUS():
    return (
        (USEquityPricing.close.latest > 3) &
        (USEquityPricing.volume.latest > 0) &
        Exchange().element_of(['NYSE', 'NASDAQ', 'NYSEMKT']) &
        (Sector().notnull()) &
        (~Sector().element_of(['Financial Services', 'Real Estate'])) &
        (IsDomestic().eq(1)) &
        (AverageDollarVolume(window_length=200) > 2.5e6) &
        (MarketCap() > 350e6)
    )