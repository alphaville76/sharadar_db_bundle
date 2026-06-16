"""Main entry point for standalone Sharadar data ingestion.

Runs the _ingest function directly with a hardcoded start date,
intended for execution outside of the zipline bundle system.
"""
import pandas as pd
from sharadar.loaders.ingest_sharadar import _ingest
from exchange_calendars import get_calendar

start_date = '2024-06-05'
calendar = get_calendar('XNYS', start=pd.Timestamp('2000-01-01 00:00:00'))

_ingest(start_date, calendar)

