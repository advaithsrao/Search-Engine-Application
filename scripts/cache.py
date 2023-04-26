"""
File : /scripts/cache.py

Description:
1. Implement the cache function for our twitter search applet
2. Optimally store/retrieve cache info from/to the disk
"""
import json
import os
from collections import OrderedDict
import asyncio
from datetime import datetime
import time

class CustomJSONEncoder(json.JSONEncoder):
    """
    This is a custom class that extends the `json.JSONEncoder` class. It overrides the `default()` method
    to handle datetime objects in a JSON-serializable format.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class CacheManager:
    def __init__(self, cache_file_path = "./data/diskCache.json", max_size=1024):
        """
        Initializes a new instance of the DiskLRUCache class.

        Parameters:
        cache_file_path (str): The path to the file that will be used for caching.
        max_size (int): The maximum number of entries that can be stored in the cache.
        """
        self.cache_file_path = cache_file_path
        
        self.max_size = max_size
        
        # Load cache file
        self.cache = self.loadCache()

    def loadCache(self):
        """
        Loads the cache from the file on disk.
        """
        try:
            if os.path.exists(self.cache_file_path):
                with open(self.cache_file_path, 'r') as f:
                    return OrderedDict(json.load(f))
            else:
                return OrderedDict()
        except Exception as e:
            print(f'Cache Load Failed as: {e}. Defaulting to empty cache')
            return OrderedDict()

    async def saveCache(self):
        """
        Saves the cache to the file on disk.
        """
        try:
            with open(self.cache_file_path, 'w') as f:
                json.dump(self.cache, f)
        except Exception as e:
            print(f'Cache Save Failed as: {e}. Defaulting to empty cache')
    
    def __contains__(self, key):
        """
        Returns True if the key is in the cache, False otherwise.

        Parameters:
        key (str): The key to search for in the cache.
        """
        if isinstance(key, str):
            return key in self.cache
        elif isinstance(key, dict):
            return json.dumps(key) in self.cache

    def getQuery(self, key):
        """
        Returns the value associated with the given key in the cache, else returns None

        Parameters:
        key (str): The key to retrieve the value for.
        """
        _start = time.time()
        if isinstance(key, dict):
            key = json.dumps(key)

        # Add the key as new entry to the dict by moving it to top of the order
        value = self.cache.pop(key)
        
        self.cache[key] = value

        response_time = time.time() - _start

        #update response time on fetch from cache
        self.cache[key]['response_time'] = response_time
        self.cache[key]['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if response_time < 10:
            _ttl = 300
        else:
            _ttl = 300 + response_time
        
        return self.cache[key]['result']

    def putQuery(self, key, result, response_time):
        """
        Adds the given key-value pair to the cache.

        Parameters:
        key (str): The key to add to the cache.
        results (any): The value to add to the cache.
        """
        if response_time < 10:
            _ttl = 300
        else:
            _ttl = 300 + response_time

        if isinstance(key, dict):
            key = json.dumps(key)
        
        if len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        
        self.cache[key] = {
            'result'        : result, 
            'time-to-live'  : _ttl,
            'created_at'    : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'response_time' : response_time
        }

    def delQuery(self, key):
        """
        Removes the given key-value pair from the cache.

        Parameters:
        key (str): The key to remove from the cache.
        """
        if isinstance(key, dict):
            key = json.dumps(key)
        
        del self.cache[key]
    
    def dictInspect(self):
        """
        Return cache dict to inspect
        """
        return self.cache

    def clear(self):
        """
        Clears all items from the cache.
        """
        self.cache.clear()
    
    async def close(self, save = True):
        """
        Saves the cache to the JSON file and close it
        """
        if save:
            asyncio.run(self.saveCache())