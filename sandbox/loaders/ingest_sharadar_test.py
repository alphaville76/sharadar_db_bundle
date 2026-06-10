from sharadar.loaders.ingest_sharadar import _ingest
import pandas as pd
import exchange_calendars as xcals

start_session = pd.to_datetime('2021-03-21', utc=True)
_ingest(start_session, xcals.get_calendar('XNYS', start=pd.Timestamp('2000-01-01 00:00:00')), use_last_available_dt=False)