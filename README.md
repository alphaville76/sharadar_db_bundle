Sqlite based zipline bundle for the Sharadar datasets SEP, SFP and SF1.

Unlike the standard zipline bundles, it allows incremental updates, because sql tables are used instead of bcolz.

Step 1. Make sure you can access Quandl, and you have a Quandl api key. I have set my Quandl api key as an environment variable.

>export QUANDL_API_KEY="your API key"  

Step 2. Clone or download the code and install it using:

>python setup.py install 

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
>from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from sharadar.pipeline.factors import (
    MarketCap,
    EV,
    Fundamentals
)
from sharadar.pipeline.engine import symbol, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets
>
> pipe = Pipeline(columns={
    'mkt_cap': MarketCap(),
    'ev': EV(),
    'debt': Fundamentals(field='debtusd_arq'),
    'cash': Fundamentals(field='cashnequsd_arq')
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)

>stocks = spe.run_pipeline(pipe, pipe_start, pipe_end)
stocks
