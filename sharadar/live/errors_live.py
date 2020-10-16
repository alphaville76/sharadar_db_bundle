from zipline.errors import ZiplineError

class ScheduleFunctionOutsideTradingStart(ZiplineError):
    """
    Raised when an algorithm schedules functions outside of
    before_trading_start()
    """
    msg = "schedule_function() should only be called in before_trading_start()"