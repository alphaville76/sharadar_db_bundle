from sharadar.util.calendar_util import last_trading_date


def test_last_trading_date_weekdays():
    assert '2021-05-05' == last_trading_date('2021-05-05')
    assert '2021-05-06' == last_trading_date('2021-05-06')
    assert '2021-05-07' == last_trading_date('2021-05-07')
    assert '2021-05-10' == last_trading_date('2021-05-10')
    assert '2021-05-11' == last_trading_date('2021-05-11')


def test_last_trading_date_weekends():
    assert '2021-05-07' == last_trading_date('2021-05-08')
    assert '2021-05-07' == last_trading_date('2021-05-09')
