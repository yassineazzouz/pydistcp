from threading import Thread
from functools import wraps


def run_async(func):
	@wraps(func)
	def async_func(*args, **kwargs):
		func_hl = Thread(target=func, args=args, kwargs=kwargs, name='Thread_{}'.format(func.__name__))
		func_hl.start()
		return func_hl
return async_func