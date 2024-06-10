from sharadar.loaders.ingest_sharadar import _ingest
from exchange_calendars import get_calendar

start_date = '2024-06-05'
calendar = get_calendar('XNYS')

_ingest(start_date, calendar)

