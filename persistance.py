import random
import string
import os
from sqlitedict import SqliteDict
import traceback
import logging
from pytictoc import TicToc
import sys
import secrets

sys.path.append(os.getcwd())

random.seed(secrets.randbelow(10000000000))

def generate_random_string(size = 8,  chars = string.ascii_lowercase + string.digits):
  return ''.join([secrets.choice(string.ascii_lowercase)]+[secrets.choice(chars) for _ in range(size-1)])

class cache_list_manager(object):
  def __init__(self, cache_name = 'temp', cache_path = os.getenv('CACHE_PATH', '.cache_data')) -> None:
      super().__init__()
      self.cache_path = cache_path
      self.cache_name = cache_name
      if not os.path.exists(self.cache_path ):
        os.makedirs(self.cache_path)
      self.cache_path = os.path.join(self.cache_path, self.cache_name)
  
  def generate_keys(self, size = 8, 
                      chars = string.ascii_lowercase + string.digits):
    return ''.join([secrets.choice(string.ascii_lowercase)]+[secrets.choice(chars) for _ in range(size-1)])

  def __getitem__(self, key):
    try:
      with SqliteDict(self.cache_path) as mydict:
        return mydict[key]
    except:
      return None

  def update(self,data = None, *args, **kwargs):
    try:
      if isinstance(data, dict):
        with SqliteDict(self.cache_path) as mydict:
          for k,v in data.items():
            mydict[k] = v
            mydict.commit()
      elif data is None:
        
        for arg in args:
          self.update(data = arg)
      
        self.update(data = kwargs)
      else:
        raise Exception(f'{data} is not dict')
  
      
    except Exception as err:
      logging.error(f'{self.update.__name__}: {traceback.format_exc()}')


    
  def add(self, value, key = None):
    if isinstance(value, list):
      return [self.add(v) for v in value]
    else:
      try:
        key = self.generate_keys() if key is None else key
        with SqliteDict(self.cache_path) as mydict:
          mydict[key] = value # Using dict[key] to store
          mydict.commit() # Need to commit() to actually flush the data
        return key
      except Exception as ex:
        logging.warning(self.add.__name__, 
              traceback.format_exc())
        return None


  def delete(self, key):
    try:
      if isinstance(key, str):
        with SqliteDict(self.cache_path) as mydict:
          mydict.pop(key)
          mydict.commit()
      elif isinstance(key, list):
        for k in key:
          self.delete(k)
      else:
          raise Exception(f'{key} is not list or str.')
    except Exception as ex:
      logging.warning(f'cache: {self.cache_name} cannot delete {key}')

  
  def iterkeys(self):
    try:
      with SqliteDict(self.cache_path) as mydict:
        for k in mydict.keys():
          yield k
    except Exception as ex:
      logging.error(self.iterkeys.__name__, 
            traceback.format_exc())
  
  def itervalues(self):
    try:
      with SqliteDict(self.cache_path) as mydict:
        for value in mydict.values():
          yield value
    except Exception as ex:
      logging.error(self.itervalues.__name__, 
            traceback.format_exc())

  def iteritems(self):
    try:
      with SqliteDict(self.cache_path) as mydict:
        for k, value in mydict.iteritems():
          yield k, value
    except Exception as ex:
      logging.error(self.itervalues.__name__, 
            traceback.format_exc())
  def clear_all_cache(self):
    if os.path.exists(self.cache_path):
      os.remove(self.cache_path)
  
  def __len__(self):
    try:
      with SqliteDict(self.cache_path) as mydict:
        return len(mydict)
    except Exception as ex:
      logging.error(self.itervalues.__name__, 
            traceback.format_exc())

  
  @classmethod
  def clean(cls, cache_name):
    try:
      obj = cls(cache_name = cache_name)
      obj.clear_all_cache()
    except Exception as err:
      logging.error(f'Cannot clear: {cache_name}')

if __name__ == '__main__':

  cache_list_manager.clean(cache_name = 'temp')
  test_cache = cache_list_manager(cache_name = 'temp')

  # test data
  nsample = 100
  timer = TicToc()

  adder_counter = 0

  print(f'nsample: {nsample}')
  for id in range(nsample): 
    timer.tic()
    k = test_cache.add(value = {'id': id})
    adder_counter += timer.tocvalue()
    #print(f'{k}: {test_cache[k]}')
  print(f'Size: {len(test_cache)}')

  # get all
  # for k,v in test_cache.iteritems():
  #   print(k, v)
  print(f'Size: {len(test_cache)}')
  all_keys = list(test_cache.iterkeys())

  # update 
  test_cache.update(edtl334l = {'id': id+1})
  test_cache.update(data = {'abce1234': {'id':id+2}})
  test_cache.update({'fijk1234': {'id':id+3}})

  timer_sum = 0
  for k in test_cache.iterkeys():
    timer.tic()
    _ = test_cache[k]
    timer_sum += timer.tocvalue()

  print(f'Size: {len(test_cache)}')
  print(f'append time per record: {adder_counter/len(test_cache)}')
  print(f'access time per record: {timer_sum/len(test_cache)}')
  print(test_cache['abce1234']['id'])
  