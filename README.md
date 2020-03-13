Sqlite based zipline bundle for the Sharadar datasets SEP and SF1
Unlike the standard zipline bundles, it allows incremental updates, because bcolz tables are replaces by sql ones.

Step 1. Make sure you can access Quandl, and you have a Quandl api key. I have set my Quandl api key as an environment variable.

>export QUANDL_API_KEY="your API key"  

Step 2. Clone or download the code and finally install it using:

>python setup.py install 



pandas change: pandas/io/sql.py:        insert_statement = 'INSERT OR REPLACE INTO %s (%s) VALUES (%s)' % (
