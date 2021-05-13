import pandas as pd
from trading_calendars import get_calendar


def last_trading_date(date:str = pd.to_datetime("today").strftime('%Y-%m-%d'), calendar = get_calendar('XNYS')):
    """
    The last trading date as of the given date (default: today)
    """
    dt = pd.to_datetime(date)
    return date if calendar.is_session(dt) else calendar.previous_open(dt).strftime('%Y-%m-%d')