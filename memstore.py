# A class with a dictionary interface that stores data either in memory
# of in redis, depending on how it is initialized.
import os
import pickle
import redis
import time
import logging
import collections

import size

DATA_PATH = "data/data.pickle"

class MemStore:
    def __init__(self, redis_url:str=None, no_cache:bool = False, namespace_config:dict[dict[str]]={}):
        # namespace_config is a dictionary specifying treatment of different pieces of data.
        # Each key is the prefix ending before the first '%' in keys that it should be matched against.
        # Potential values are:
        #  - cache: store the value in memory as well as redis for faster retrieval next time
        #  - expiry: set an expiry time on the key
        self.namespace_config = namespace_config
        self.data = collections.defaultdict(dict)
        if redis_url:
            logging.info("Using redis for data storage at %s", redis_url)
            self.redis = redis.from_url(redis_url)
        else:
            self.redis = None
            if os.path.exists(DATA_PATH) and not no_cache:
                with open(DATA_PATH, "rb") as f:
                    logging.info("Loading GTFS static data from cache.")
                    self.data = pickle.load(f)
    
    def clear_data(self):
        if self.redis:
            self.redis.flushdb()
        else:
            self.data = collections.defaultdict(dict)
    
    def persist_data(self):
        if self.redis:
            self.redis.save()
        else:
            with open(DATA_PATH, "wb") as f:
                pickle.dump(self.data, f)
    
    def profile_memory(self):
        res = {}
        if self.redis:
            res['redis'] = self.redis.info('memory')['used_memory']
        in_proc = {}
        for key in self.data:
            in_proc[f"In-process '{key}'"] = size.total_size(self.data[key])
        res['in_process'] = sum(in_proc.values())
        res.update(in_proc)
        return res

    def get(self, namespace, key, default=None):
        config = self.namespace_config.get(namespace, {})
        now = int(time.time())
        expiry = config.get('expiry')
        
        value = None
        cached_item = self.data.get(namespace, {}).get(key)
        if cached_item:
            t, cached_value = cached_item
            if expiry is None or now - t < expiry:
                value = cached_value
            else:
                del self.data[namespace][key]
        
        if value is None and self.redis:
            value = self.redis.hget(namespace, key)
            if value is not None:
                t, value = pickle.loads(value)
                if expiry and now - t > expiry:
                    # if it's expired, delete it
                    self.redis.hdel(namespace, key)
                    value = None
            # if we still don't have a value, use the default
            if value is None:
                value = default
            # cache the value if we're supposed to
            if config.get('cache'):
                self.data[namespace][key] = (now, value)
        
        return value if value is not None else default

    def set(self, namespace, key, value):
        config = self.namespace_config.get(namespace, {})
        t = int(time.time())
        expiry = config.get('expiry')
        if self.redis:
            self.redis.hset(namespace, key, pickle.dumps((t, value)))
        else:
            self.data[namespace][key] = (t, value)
    
    # set operations including add, remove and has
    def add(self, namespace, value):
        config = self.namespace_config.get(namespace, {})
        if self.redis:
            self.redis.sadd(namespace, value)
        else:
            self.data.setdefault(namespace, set()).add(value)
    
    def remove(self, namespace, value):
        config = self.namespace_config.get(namespace, {})
        if self.redis:
            self.redis.srem(namespace, value)
        else:
            self.data.setdefault(namespace, set()).remove(value)
    
    def has(self, namespace, value):
        config = self.namespace_config.get(namespace, {})
        if self.redis:
            return self.redis.sismember(namespace, value) == 1
        else:
            return value in self.data.setdefault(namespace, set())
        

    