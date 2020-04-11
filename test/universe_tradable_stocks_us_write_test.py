import pandas as pd
from sharadar.util.universe import UniverseWriter
from sharadar.loaders.ingest import get_output_dir
import os
from sharadar.pipeline.filters import TradableStocksUS
from sharadar.pipeline.factors import (
    Fundamentals,
    EV
)


#2019-01-01 -e 2020-01-01
pipe_start = pd.to_datetime('2019-01-02', utc=True)
pipe_end = pd.to_datetime('2020-02-28', utc=True)
screen = TradableStocksUS()
def base_universe():
    #return TradableStocksUS()
    return (
        (Fundamentals(field='revenue_art') > 0) &
        (Fundamentals(field='assets_arq') > 0) &
        (Fundamentals(field='equity_arq') > 0) &
        (EV() > 0)

    )
#screen = base_universe()
#screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))

universes_db_path = os.path.join(get_output_dir(), "universes.sqlite")

universe_name = 'tradable_stocks_us'
UniverseWriter(universes_db_path).write(universe_name, screen, pipe_start, pipe_end)
