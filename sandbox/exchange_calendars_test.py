import pandas as pd
from exchange_calendars import get_calendar

trading_calendar = get_calendar('XNYS', start=pd.Timestamp('2000-01-01 00:00:00'))
start = pd.Timestamp.utcnow().normalize()
print(start)
trading_calendar.is_session(start.date())
start = trading_calendar.next_open(start)