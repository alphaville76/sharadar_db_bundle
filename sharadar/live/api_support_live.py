from zipline.utils.compat import wraps

def allowed_only_in_before_trading_start(exception):
    """
    Decorator for API methods that can be called only from within
    TradingAlgorithm.before_trading_start.  `exception` will be raised if the
    method is called outside `before_trading_start`.

    Usage
    -----
    @allowed_only_in_before_trading_start(SomeException("Don't do that!"))
    def method(self):
        # Do stuff that is only allowed inside before_trading_start.
    """
    def decorator(method):
        @wraps(method)
        def wrapped_method(self, *args, **kwargs):
            if not self._in_before_trading_start:
                raise exception
            return method(self, *args, **kwargs)
        return wrapped_method
    return decorator