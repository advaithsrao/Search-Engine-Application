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
from scripts.utils import CustomJSONEncoder

class CacheManager:
    def __init__(self, cache_file = "diskCache.json", max_size=1024):
        """
        Initializes a new instance of the DiskLRUCache class.

        Parameters:
        cache_file (str): The path to the file that will be used for caching.
        max_size (int): The maximum number of entries that can be stored in the cache.
        """
        self.cache_file = cache_file
        
        self.max_size = max_size
        
        # Load cache file
        self.cache = self.loadCache()

    def loadCache(self):
        """
        Loads the cache from the file on disk.
        """
        try:
            if not os.path.exists('./data'):
                os.makedirs('./data')
            
            if os.path.exists(os.path.join('./data',self.cache_file)):
                with open(os.path.join('./data',self.cache_file), 'r') as f:
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
            with open(os.path.join('./data',self.cache_file), 'w') as f:
                json.dump(self.cache, f, cls=CustomJSONEncoder)
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
    
    def backupCache(self,current_timestamp):
        """
        Clears all items from the cache.
        """
        if not bool(current_timestamp):
            current_timestamp = datetime.now().strptime("%Y-%m-%d %H:%M:%S")
        
        try:
            bckup_file = f'./data/diskCache_{current_timestamp}.json'
            with open(os.path.join('./data',bckup_file), 'w') as f:
                json.dump(self.cache, f, cls=CustomJSONEncoder)
            print(f'Cache Backup Successful at {current_timestamp}')
        except Exception as e:
            print(f'Cache Backup Failed at {current_timestamp} as : {e}')

    def close(self, save = True):
        """
        Saves the cache to the JSON file and close it
        """
        if save:
            asyncio.run(self.saveCache())