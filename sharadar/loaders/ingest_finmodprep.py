from os import environ as env
from urllib.request import urlopen
import urllib.parse
import json



APIKEY = env["FINMODPREP_API_KEY"]

def head(entries, n=10):
    count = 0
    for entry in entries:
        print(entry)
        count += 1
        if count == n:
            break
    print(len(entries))

def to_dict_array(url, params={}):
    params['apikey']=APIKEY
    query_string = urllib.parse.urlencode(params)
    response = urlopen(url + "?" + query_string )
    data = response.read().decode("utf-8")
    return json.loads(data)

symbols = to_dict_array("https://financialmodelingprep.com/api/v3/stock/list")
head(symbols)

delisted = to_dict_array("https://financialmodelingprep.com/api/v3/delisted-companies", {'limit': 100})
head(delisted)

symbol = 'AAPL'
daily_prices = to_dict_array("https://financialmodelingprep.com/api/v3/historical-price-full/"+symbol, {'from': '1900-10-01', 'to':'2017-11-03'})['historical']
head(daily_prices)

dividends = to_dict_array("https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/"+symbol)['historical']
head(dividends)

splits = to_dict_array("https://financialmodelingprep.com/api/v3/historical-price-full/stock_split/"+symbol)['historical']
head(splits)

splits = to_dict_array("https://financialmodelingprep.com/api/v3/historical-price-full/stock_split/"+symbol, {'from': '2010-10-01', 'to':'2020-12-09'})['historical']
head(splits)

#Daily
mkt_cap = to_dict_array("https://financialmodelingprep.com/api/v3/market-capitalization/"+symbol, {'limit':'50'})
head(mkt_cap)

#Quartely
ev = to_dict_array("https://financialmodelingprep.com/api/v3/enterprise-values/"+symbol, {'limit':'50'})
head(ev)

# Important: weightedAverageShsOut
income_statement = to_dict_array("https://financialmodelingprep.com/api/v3/income-statement/"+symbol, {'limit':'50', 'period': 'quarter'})
head(income_statement)