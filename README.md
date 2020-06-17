Sqlite based zipline bundle for the Sharadar datasets SEP, SFP and SF1.

Unlike the standard zipline bundles, it allows incremental updates, because sql tables are used instead of bcolz.

Step 1. Make sure you can access Quandl, and you have a Quandl api key. I have set my Quandl api key as an environment variable.

>export QUANDL_API_KEY="your API key"  

Step 2. Clone or download the code and install it using:

>python setup.py install 

The new entry point is **sharadar-zipline** (it replaces *zipline*).

For example to ingest data use:
> sharadar-zipline ingest

or to run an algorithm
> sharadar-zipline -f algo.py -s 2017-01-01 -e 2020-01-01


To start a notebook 
> cd notebook
> jupyter notebook


