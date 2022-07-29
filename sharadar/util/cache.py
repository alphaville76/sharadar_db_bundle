import memoization
from psutil import virtual_memory as mem
from memoization.constant.flag import CachingAlgorithmFlag


def cached(user_function=None, max_size=None, ttl=None, algorithm=CachingAlgorithmFlag.FIFO, thread_safe=True, order_independent=False):
    """
    Entry point for the configuration of the memoization cache
    """

    # if mem().percent > 75:
    #     # Do not cache if memory usage more than 75%
    #     return user_function
    #
    # return memoization.cached(user_function, max_size, ttl, algorithm, thread_safe, order_independent)

    #FIXME no cache
    return user_function

