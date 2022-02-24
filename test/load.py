from os import environ as env
from sharadar.pipeline.engine import sid, sids
import nasdaqdatalink

nasdaqdatalink.ApiConfig.api_key=env["NASDAQ_API_KEY"]

print(sid(199059))

print(sids([573112, 199988, 199954, 199949]))

print(nasdaqdatalink.get_table('SHARADAR/SEP', date='2020-03-16', ticker=['ZDGE','AKRX','ANDR','SSI']))