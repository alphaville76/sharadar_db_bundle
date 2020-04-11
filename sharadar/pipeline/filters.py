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


def TradableStocksUS():
    return (
        (USEquityPricing.close.latest > 3) &
        (USEquityPricing.volume.latest > 0) &
        Exchange().element_of(['NYSE', 'NASDAQ', 'NYSEMKT']) &
        (Sector().notnull()) &
        (~Sector().element_of(['Financial Services', 'Real Estate'])) &
        (IsDomestic().eq(1)) &
        (AverageDollarVolume(window_length=200) > 2.5e6) &
        (MarketCap() > 350e6) &
        (Fundamentals(field='revenue_art') > 0) &
        (Fundamentals(field='assets_arq') > 0) &
        (Fundamentals(field='equity_arq') > 0) &
        (EV() > 0)
    )