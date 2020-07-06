import memoization
from memoization.constant.flag import CachingAlgorithmFlag

cached = memoization.cached
def cached_tmp(user_function=None, max_size=None, ttl=None,
           algorithm=CachingAlgorithmFlag.LRU, thread_safe=True, order_independent=False):
    """
    Entry point for the configuration of the memoization cache
    """
    return memoization.cached(user_function, max_size, ttl, algorithm, thread_safe, order_independent)

    # Return direct the user function to disable the caching
    #return user_function