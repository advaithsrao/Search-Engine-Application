"""
File : /scripts/staleCacheChecker.py

Description:
1. Implement a function that periodically checks stale entries in cache and removes it
"""
from datetime import datetime
from scripts.cache import CacheManager

class StaleCacheRemover:
    def __init__(self):
        """
        Initialize the StaleCacheRemover object.

        This method sets up the object by creating a CacheManager object and 
        initializing various instance variables, such as the cache and the 
        clean interval.
        """
        self.cacher = CacheManager()
        self.cache = self.cacher.dictInspect()
        # self.clean_interval = 3600  # clean the cache every hour

    def cleanCache(self):
        """
        Clean the cache.

        This method removes all entries from the cache that have expired based 
        on their time-to-live (TTL). It also saves the updated cache to disk 
        and backs up the cache if there are at least 5 more entries in the 
        updated cache than in the old cache.
        """
        old_len = len(self.cache)
        now = datetime.now()
        keys_to_remove = []
        for key, entry in self.cache.items():
            created_at = datetime.strptime(entry['created_at'], "%Y-%m-%d %H:%M:%S")
            ttl = entry['time-to-live']
            if (now - created_at).total_seconds() >= ttl:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.cache[key]
        new_len = len(self.cache)
        
        self.cacher.close(save = True)

        if new_len - old_len >= 5:
            self.cacher.backupCache(created_at)


    def startClean(self):
        """
        Start the cache cleaning process.

        This method starts the cache cleaning process by calling the cleanCache 
        method every hour (as specified by the clean_interval instance variable).
        """
        self.clean_cache()

if __name__ == '__main__':
    remover = StaleCacheRemover()
    remover.startClean()