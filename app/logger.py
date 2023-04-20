import time
import logging
from functools import wraps

logging.basicConfig(filename='app.log', filemode='a', level=logging.DEBUG, format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', force=True)

def timed(func):
    @wraps(func)
    def function_time(*args, **kwargs):
        signature = ''
        # args_repr = [repr(a) for a in args]
        # kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        # signature = ", ".join(args_repr + kwargs_repr)
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        if signature:
            # logging.debug("{} ran with args {} in {}s".format(func.__name__, signature, round(end - start, 3)))
            print("{} ran with args {} in {}s".format(func.__name__, signature, round(end - start, 3)))
        else:
            # logging.debug("{} ran in {}s".format(func.__name__, round(end - start, 3)))
            print("{} ran in {}s".format(func.__name__, round(end - start, 3)))
        return result

    return function_time
