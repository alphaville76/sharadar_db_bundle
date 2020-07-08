import memoization
from memoization.constant.flag import CachingAlgorithmFlag

wrappers = []

def cached_wrappers(user_function=None, max_size=0, ttl=None,
           algorithm=CachingAlgorithmFlag.LRU, thread_safe=True, order_independent=False):
    """
    Entry point for the configuration of the memoization cache

    To find the optimal max-size value:

    from sharadar.util.cache import wrappers
    for wrapper in wrappers:
        print(wrapper, wrapper.cache_info())
    """
    wrapper = memoization.cached(user_function, max_size, ttl, algorithm, thread_safe, order_independent)
    wrappers.append(wrapper)
    return wrapper


def cached(user_function=None, max_size=500000, ttl=None, algorithm=CachingAlgorithmFlag.LRU, thread_safe=True, order_independent=False):
    """
    Entry point for the configuration of the memoization cache
    """

    return memoization.cached(user_function, max_size, ttl, algorithm, thread_safe, order_independent)

    # Return direct the user function to disable the caching
    #return user_function