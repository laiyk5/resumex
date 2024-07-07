from logging import getLogger
import logging
import logging.handlers
import random

class ShallRetryError(RuntimeError):
  pass

class RetryFailedError(RuntimeError):
  pass

logger = getLogger(__name__)

def retry(max_retry, logger=logger): # decorator factory
  def _wrapper_1(func): # decorator
    def _wrapper_2(*args, **kwargs): # decorated fucntion.
      last_e = None
      for i in range(max_retry):
        try:
          res = func(*args, **kwargs)
          return res
        except ShallRetryError:
          logger.info(f'{i}_th retry')
          continue
      raise RetryFailedError
    return _wrapper_2
  return _wrapper_1


if __name__ == '__main__':
  @retry(3)
  def dummy(a, b):
    if random.random() < 0.5:
      raise ShallRetryError
    return a / b
  
  formatter = logging.Formatter('%(asctime)s %(message)s')
  ch = logging.StreamHandler()
  ch.setFormatter(formatter)
  ch.setLevel(logging.DEBUG)
  logger.addHandler(ch)
  logger.setLevel(logging.DEBUG)
  print(dummy(1, 0))