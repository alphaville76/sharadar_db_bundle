from sharadar.loaders.ingest_sharadar import _ingest
import pandas as pd
import exchange_calendars as xcals

start_session = pd.to_datetime('2021-03-21', utc=True)
_ingest(start_session, xcals.get_calendar('XNYS'), use_last_available_dt=False)