import pandas as pd
from os import environ as env
from sharadar.loaders.load import sid, sids
import quandl

quandl.ApiConfig.api_key=env["QUANDL_API_KEY"]

print(sid(199059))

print(sids([573112, 199988, 199954, 199949]))

print(quandl.get_table('SHARADAR/SEP', date='2020-03-16', ticker=['ZDGE','AKRX','ANDR','SSI']))