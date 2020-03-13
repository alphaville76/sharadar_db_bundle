import pandas as pd

import os

BUNDLE = 'sep_db'
start = pd.to_datetime('2020-02-03', utc=True)
end = pd.to_datetime('2020-02-07', utc=True)


from sharadar.loaders import load
bundle = load(BUNDLE, os.environ, None)