# A class with a dictionary interface that stores data either in memory
# of in redis, depending on how it is initialized.
import os
import pickle
import redis
import time
import logging

import size

CACHE_PATH = "data/cache.pickle"

class MemStore:
    def __init__(self, redis_url:str=None, no_cache:bool = False, keys_config:dict[dict[str]]={}):
        # Keys_config is a dictionary specifying treatment of different pieces of data.
        # Each key is the prefix ending before the first '%' in keys that it should be matched against.
        # Potential values are:
        #  - memoize: store the value in memory as well as redis for faster retrieval next time
        #  - expiry: set an expiry time on the key
        self.keys_config = keys_config
        self.data = {}
        self.cache = {}
        if redis_url:
            self.redis = redis.from_url(redis_url)
        else:
            self.redis = None
            if os.path.exists(CACHE_PATH) and not no_cache:
                with open(CACHE_PATH, "rb") as f:
                    logging.info("Loading GTFS static data from cache.")
                    self.data = pickle.load(f)
    
    def store_cache(self):
        if self.redis:
            self.redis.save()
        else:
            with open(CACHE_PATH, "wb") as f:
                pickle.dump(self.data, f)
    
    def profile_memory(self):
        if self.redis:
            return self.redis.info('memory')
        else:
            return size.total_size(self.data)
    
    def _get_key_config(self, key):
        return self.keys_config.get(key.split(':')[0], {})

    def get(self, key, default=None):
        config = self._get_key_config(key)
        if self.redis:
            t = int(time.time())
            
            if config.get('memoize') and key in self.cache:
                cache_t, cache_value = self.cache[key]
                expiry = config.get('expiry')
                if expiry is None or t - cache_t < expiry:
                    return cache_value
                else:
                    del self.cache[key]
            value = self.redis.get(key)
            if value is not None:
                value = pickle.loads(value)
            else:
                value = default
            
            if config.get('memoize'):
                t = int(time.time())
                self.cache[key] = (t, value)
            return value
        else:
            value = self.data.get(key)
            if value is None:
                return default
            return value
    
    def set(self, key, value):
        config = self._get_key_config(key)
        t = int(time.time())
        if self.redis:
            expiry = config.get('expiry')
            self.redis.set(key, pickle.dumps(value), ex=expiry)
        else:
            self.data[key] = value
    
    # set operations including add, remove and has
    def add(self, key, value):
        config = self._get_key_config(key)
        if self.redis:
            self.redis.sadd(key, value)
        else:
            self.data.setdefault(key, set()).add(value)
    
    def remove(self, key, value):
        config = self._get_key_config(key)
        if self.redis:
            self.redis.srem(key, value)
        else:
            self.data.setdefault(key, set()).remove(value)
    
    def has(self, key, value):
        config = self._get_key_config(key)
        if self.redis:
            return self.redis.sismember(key, value)
        else:
            return value in self.data.setdefault(key, set())
        

    