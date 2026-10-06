Sqlite based zipline bundle for the Sharadar datasets SEP, SFP and SF1.

Unlike the standard zipline bundles, it allows incremental updates, because sql tables are used instead of bcolz.

Step 1. Make sure you can access Quandl, and you have a Quandl api key. I have set my Quandl api key as an environment variable.

>export NASDAQ_API_KEY="your API key"  

Step 2. Clone or download the code and install it using:

>python setup.py install 

For zipline in order to build the cython files run:
>python setup.py build_ext --inplace

Add this code to your ~/.zipline/extension.py:
```python
from zipline.data import bundles
from zipline.finance import metrics
from sharadar.loaders.ingest_sharadar import from_nasdaqdatalink
from sharadar.util.metric_daily import default_daily

bundles.register("sharadar", from_nasdaqdatalink(), create_writers=False)
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


## Repairing dividend adjustments

Older versions of the bundle wrote the `dividends` table of `adjustments.sqlite` with shifted columns
(sid, date and ratio in the wrong columns). As a result zipline never applied dividend adjustments to prices,
and `reduce_db.sh` deleted most of the dividend ratios. The ingest now writes the table correctly; to repair
an existing bundle run:

> python -m sharadar.util.fix_dividends --dry-run

> python -m sharadar.util.fix_dividends

Optionally pass the bundle folder (the one containing `adjustments.sqlite`, `prices.sqlite` and `assets-7.sqlite`),
default is `~/.zipline/data/sharadar/latest`:
> python -m sharadar.util.fix_dividends /path/to/sharadar/latest

The script:
1. moves the shifted values of the `dividends` table back into the right columns;
2. recomputes the ratios missing for the rows of `dividend_payouts` (`1 - amount / previous raw close`),
   skipping dividends without a previous close or with a ratio <= 0;
3. clears the pipeline cache of the default bundle, because cached results were computed without dividend
   adjustments (for a different folder, clear its `cache` subfolder manually).

`--dry-run` only prints how many rows would be repaired or rebuilt. The script can be run more than once:
a second run changes nothing. Stop algorithms and ingests using the bundle before running it, and make a
backup of `adjustments.sqlite`. Afterwards historical prices are adjusted for dividends, so backtests and
price-based factors will differ from previous runs.


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