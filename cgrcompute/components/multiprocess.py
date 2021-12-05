from multiprocessing import Manager
from threading import Semaphore
from typing import Any, Callable
import pickle

class SharableCache:
    cache_miss_lock : Semaphore
    shared_cache : dict[str, bytes]
    shared_cache_version : dict[str, int]
    local_cache : dict[str, tuple[int, Any]]

    def __init__(self, manager: Manager):
        self.cache_miss_lock = manager.Semaphore()
        self.shared_cache = manager.dict()
        self.shared_cache_version = manager.dict()
        self.local_cache = dict()

    def get(self, key: str):
        if key in self.local_cache and self.local_cache[key][0] == self.shared_cache_version[key]:
            return self.local_cache[key][1]
        else:
            self.local_cache[key] = (self.shared_cache_version[key], pickle.loads(self.shared_cache[key]))
            return self.local_cache[key][1]

    def get_or_create(self, key: str, initfn: Callable[[], Any]):
        try:
            return self.get(key)
        except KeyError:
            self.cache_miss_lock.acquire()
            try:
                if key not in self.shared_cache:
                    self.update(key, initfn())
                return self.get(key)
            finally:
                self.cache_miss_lock.release() 

    def update(self, key: str, val: Any):
        try:
            v = self.shared_cache_version[key]
        except KeyError:
            v = 0
        self.shared_cache_version[key] = v+1
        self.shared_cache[key] = pickle.dumps(val)
