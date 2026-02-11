from collections import OrderedDict
from typing import Optional

class LRUSet:
    """Simple LRU set for deduplication of string keys."""
    def __init__(self, capacity: int = 2048):
        self.capacity = capacity
        self._data: OrderedDict[str, None] = OrderedDict()

    def add(self, key: str) -> bool:
        """Returns True if newly added, False if duplicate."""
        if key in self._data:
            self._data.move_to_end(key)
            return False
        self._data[key] = None
        if len(self._data) > self.capacity:
            self._data.popitem(last=False)
        return True

DEDUP = LRUSet()
