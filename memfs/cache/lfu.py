"""
LFU (Least Frequently Used) cache implementation for MemFS.
Tracks access frequency to determine eviction candidates.
"""

import threading
from collections import OrderedDict
from typing import Any, Dict, Optional, Callable


class LFUCache:
    """
    Least Frequently Used cache implementation.

    Evicts the least frequently accessed items when capacity is reached.
    Uses a frequency map and ordered dictionaries to track access patterns.
    """

    def __init__(self, max_capacity: int = 100):
        """
        Initialize LFU cache.

        Args:
            max_capacity: Maximum number of items in cache.
        """
        self.max_capacity = max_capacity
        self._lock = threading.Lock()

        self._cache: Dict[str, Any] = {}
        self._frequency: Dict[str, int] = {}
        self._frequency_map: Dict[int, OrderedDict[str, None]] = {}
        self._min_frequency: int = 0

    def get(self, key: str) -> Optional[Any]:
        """
        Get item from cache.

        Args:
            key: Item key.

        Returns:
            Item value or None if not found.
        """
        with self._lock:
            if key not in self._cache:
                return None

            self._update_frequency(key)
            return self._cache[key]

    def put(self, key: str, value: Any) -> Optional[str]:
        """
        Put item in cache.

        Args:
            key: Item key.
            value: Item value.

        Returns:
            Evicted key if eviction occurred, None otherwise.
        """
        with self._lock:
            if key in self._cache:
                self._cache[key] = value
                self._update_frequency(key)
                return None

            evicted = None

            if len(self._cache) >= self.max_capacity and self.max_capacity > 0:
                evicted = self._evict()

            self._cache[key] = value
            self._frequency[key] = 1

            if 1 not in self._frequency_map:
                self._frequency_map[1] = OrderedDict()
            self._frequency_map[1][key] = None

            self._min_frequency = 1

            return evicted

    def remove(self, key: str) -> bool:
        """
        Remove item from cache.

        Args:
            key: Item key.

        Returns:
            True if item was removed, False if not found.
        """
        with self._lock:
            if key not in self._cache:
                return False

            freq = self._frequency[key]
            if freq in self._frequency_map and key in self._frequency_map[freq]:
                del self._frequency_map[freq][key]

                if not self._frequency_map[freq]:
                    del self._frequency_map[freq]

                    if freq == self._min_frequency:
                        self._min_frequency = freq + 1

            del self._cache[key]
            del self._frequency[key]
            return True

    def contains(self, key: str) -> bool:
        """Check if key is in cache."""
        with self._lock:
            return key in self._cache

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)

    def clear(self):
        """Clear all items from cache."""
        with self._lock:
            self._cache.clear()
            self._frequency.clear()
            self._frequency_map.clear()
            self._min_frequency = 0

    def get_all_keys(self) -> list[str]:
        """Get all keys in cache."""
        with self._lock:
            return list(self._cache.keys())

    def get_frequency(self, key: str) -> int:
        """Get access frequency for a key."""
        with self._lock:
            return self._frequency.get(key, 0)

    def _update_frequency(self, key: str):
        """Update frequency for a key (must hold lock)."""
        old_freq = self._frequency[key]
        new_freq = old_freq + 1

        del self._frequency_map[old_freq][key]

        if not self._frequency_map[old_freq]:
            del self._frequency_map[old_freq]

            if old_freq == self._min_frequency:
                self._min_frequency = new_freq

        self._frequency[key] = new_freq

        if new_freq not in self._frequency_map:
            self._frequency_map[new_freq] = OrderedDict()
        self._frequency_map[new_freq][key] = None

    def _evict(self) -> Optional[str]:
        """Evict least frequently used item (must hold lock)."""
        if not self._cache:
            return None

        freq = self._min_frequency

        if freq not in self._frequency_map:
            freq = min(self._frequency_map.keys())

        evicted_key, _ = self._frequency_map[freq].popitem(last=False)

        if not self._frequency_map[freq]:
            del self._frequency_map[freq]

            if freq == self._min_frequency and self._frequency_map:
                self._min_frequency = min(self._frequency_map.keys())

        del self._cache[evicted_key]
        del self._frequency[evicted_key]

        return evicted_key

    def get_stats(self) -> dict:
        """Get cache statistics."""
        with self._lock:
            return {
                "size": len(self._cache),
                "max_capacity": self.max_capacity,
                "unique_frequencies": len(self._frequency_map),
                "min_frequency": self._min_frequency,
            }
