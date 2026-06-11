import pandas as pd
from exchange_calendars import get_calendar


def last_trading_date(date=None, calendar=None):
    '''
    The last trading date as of the given date (default: today).
    
    Parameters
    ----------
    date : str, optional
        Date string in format 'YYYY-MM-DD'. Defaults to today.
    calendar : exchange_calendars.ExchangeCalendar, optional
        Trading calendar. Defaults to NYSE calendar.
    
    Returns
    -------
    str
        The last trading date in 'YYYY-MM-DD' format.
    '''
    if date is None:
        date = pd.to_datetime('today').strftime('%Y-%m-%d')
    
    if calendar is None:
        calendar = get_calendar('XNYS', start=pd.Timestamp('2000-01-01 00:00:00'))
    
    dt = pd.to_datetime(date)
    if calendar.is_session(dt):
        return date
    return calendar.previous_open(dt).strftime('%Y-%m-%d')
