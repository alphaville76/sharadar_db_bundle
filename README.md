Sqlite based zipline bundle for the Sharadar datasets SEP, SFP and SF1.

Unlike the standard zipline bundles, it allows incremental updates, because sql tables are used instead of bcolz.

Step 1. Make sure you can access Quandl, and you have a Quandl api key. I have set my Quandl api key as an environment variable.

>export QUANDL_API_KEY="your API key"  

Step 2. Clone or download the code and install it using:

>python setup.py install 

For zipline in order to build the cython files run:
>python setup.py build_ext --inplace

Add this code to your ~/.zipline/extension.py:
```python
from zipline.data import bundles
from zipline.finance import metrics
from sharadar.loaders.ingest_sharadar import from_quandl
from sharadar.util.metric_daily import default_daily

bundles.register("sharadar", from_quandl(), create_writers=False)
metrics.register('default_daily', default_daily)
```

The new entry point is **sharadar-zipline** (it replaces *zipline*).

For example to ingest data use:
> sharadar-zipline ingest

To ingest price and fundamental data every day at 21:30 using cron
> 30 21 * * *	cd $HOME/zipline/lib/python3.6/site-packages/sharadar_db_bundle && $HOME/zipline/bin/python sharadar/__main__.py ingest > $HOME/log/sharadar-zipline-cron.log 2>&1

To run an algorithm
> sharadar-zipline -f algo.py -s 2017-01-01 -e 2020-01-01


To start a notebook 
> cd notebook
> jupyter notebook


Sharadar Fundamentals could be use as follows:
```python
from zipline.pipeline import Pipeline
import pandas as pd
from sharadar.pipeline.factors import (
    MarketCap,
    EV,
    Fundamentals
)
from sharadar.pipeline.engine import symbol, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets

pipe = Pipeline(columns={
    'mkt_cap': MarketCap(),
    'ev': EV(),
    'debt': Fundamentals(field='debtusd_arq'),
    'cash': Fundamentals(field='cashnequsd_arq')
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)
spe = make_pipeline_engine()

pipe_date = pd.to_datetime('2020-02-03', utc=True)

stocks = spe.run_pipeline(pipe, pipe_date)
stocks
```