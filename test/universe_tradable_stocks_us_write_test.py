import pandas as pd
from sharadar.util.universe import UniverseWriter
from sharadar.util.output_dir import get_output_dir
import os
from sharadar.pipeline.filters import TradableStocksUS
from sharadar.pipeline.factors import (
    Fundamentals,
    EV
)


#2019-01-01 -e 2020-01-01
#pipe_start = pd.to_datetime('2010-01-04', utc=True)
#pipe_end = pd.to_datetime('2020-02-28', utc=True)

pipe_start = pd.to_datetime('2010-01-04', utc=True)
pipe_end = pd.to_datetime('2020-04-13', utc=True)
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
print(universes_db_path)

universe_name = 'tradable_stocks_us'
screen = TradableStocksUS()
UniverseWriter(universes_db_path).write(universe_name, screen, pipe_start, pipe_end)
