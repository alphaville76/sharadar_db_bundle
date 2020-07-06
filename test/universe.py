import pandas as pd
from sharadar.pipeline.universes import UniverseWriter, UniverseReader, NamedUniverse
from zipline.pipeline.filters import StaticAssets
from sharadar.pipeline.engine import symbols, make_pipeline_engine
from sharadar.util.output_dir import get_output_dir
import os
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing

pipe_start = pd.to_datetime('2009-02-03', utc=True)
pipe_end = pd.to_datetime('2020-02-07', utc=True)
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))

universes_db_path = os.path.join(get_output_dir(), "universes.sqlite")

universe_name = 'my_universe1'
UniverseWriter(universes_db_path).write(universe_name, screen, pipe_start, pipe_end)

sids = UniverseReader(universes_db_path).get_sid(universe_name, '2020-02-07')
print(sids)

sids = UniverseReader(universes_db_path).get_sid(universe_name, '2002-02-07')
print(sids)

spe = make_pipeline_engine()

pipe = Pipeline(columns={
    'Close': USEquityPricing.close.latest,
},
    screen=NamedUniverse('my_universe1')
)

stocks = spe.run_pipeline(pipe, pipe_start, pipe_end)
print(stocks)