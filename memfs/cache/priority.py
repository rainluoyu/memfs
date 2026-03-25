"""
Priority queue for MemFS file eviction.
Combines LFU with priority-based eviction.
"""

import heapq
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass(order=True)
class PriorityEntry:
    """Entry in priority queue with combined scoring."""

    score: float
    timestamp: float = field(compare=False)
    key: str = field(compare=False)
    priority: int = field(compare=False, default=5)
    frequency: int = field(compare=False, default=1)
    size: int = field(compare=False, default=0)
    value: Any = field(compare=False, default=None)


class PriorityQueue:
    """
    Priority queue for file eviction.

    Uses a combined scoring formula:
    score = (1 / frequency) * (1 / priority_weight) + recency_factor

    Lower score = higher eviction priority
    """

    PRIORITY_WEIGHTS = {
        0: 0.1,
        1: 0.2,
        2: 0.3,
        3: 0.5,
        4: 0.7,
        5: 1.0,
        6: 1.5,
        7: 2.0,
        8: 3.0,
        9: 5.0,
        10: 10.0,
    }

    def __init__(
        self,
        max_size: int = 1000,
        scoring_fn: Optional[Callable[[str, int, int, float, int], float]] = None,
    ):
        """
        Initialize priority queue.

        Args:
            max_size: Maximum number of items.
            scoring_fn: Custom scoring function (optional).
                       Signature: (key, priority, frequency, recency_seconds, size) -> score
        """
        self.max_size = max_size
        self._scoring_fn = scoring_fn or self._default_score
        self._lock = threading.Lock()

        self._heap: List[PriorityEntry] = []
        self._entry_map: Dict[str, PriorityEntry] = {}
        self._removed_keys: set[str] = set()

    @staticmethod
    def _default_score(
        key: str, priority: int, frequency: int, recency_seconds: float, size: int
    ) -> float:
        """
        Calculate default eviction score.

        Args:
            key: File key.
            priority: File priority (0-10).
            frequency: Access frequency count.
            recency_seconds: Seconds since last access.
            size: File size in bytes.

        Returns:
            Eviction score (lower = evict first).
        """
        priority_weight = PriorityQueue.PRIORITY_WEIGHTS.get(priority, 1.0)

        freq_factor = 1.0 / (frequency + 1)
        priority_factor = 1.0 / priority_weight
        recency_factor = recency_seconds / (recency_seconds + 1000)

        score = (freq_factor * priority_factor) + (recency_factor * 0.1)

        return score

    def put(
        self, key: str, value: Any, priority: int = 5, frequency: int = 1, size: int = 0
    ) -> Optional[str]:
        """
        Add item to queue.

        Args:
            key: Item key.
            value: Item value.
            priority: Priority level (0-10, higher = keep longer).
            frequency: Access frequency.
            size: Item size in bytes.

        Returns:
            Evicted key if eviction occurred, None otherwise.
        """
        with self._lock:
            if key in self._entry_map:
                self._update(key, priority, frequency, size)
                self._entry_map[key].value = value
                return None

            evicted = None

            if len(self._entry_map) >= self.max_size and self.max_size > 0:
                evicted = self._evict()

            recency_seconds = 0.0

            score = self._scoring_fn(key, priority, frequency, recency_seconds, size)

            entry = PriorityEntry(
                score=score,
                timestamp=time.time(),
                key=key,
                priority=priority,
                frequency=frequency,
                size=size,
                value=value,
            )

            heapq.heappush(self._heap, entry)
            self._entry_map[key] = entry

            return evicted

    def get(self, key: str) -> Optional[Any]:
        """
        Get item from queue.

        Args:
            key: Item key.

        Returns:
            Item value or None if not found.
        """
        with self._lock:
            if key not in self._entry_map:
                return None

            entry = self._entry_map[key]

            if key in self._removed_keys:
                return None

            entry.frequency += 1

            recency_seconds = 0.0
            new_score = self._scoring_fn(
                entry.key, entry.priority, entry.frequency, recency_seconds, entry.size
            )

            entry.score = new_score

            heapq.heapify(self._heap)

            return entry.value

    def update_priority(self, key: str, priority: int) -> bool:
        """
        Update priority for an item.

        Args:
            key: Item key.
            priority: New priority level.

        Returns:
            True if updated, False if not found.
        """
        with self._lock:
            if key not in self._entry_map:
                return False

            entry = self._entry_map[key]
            entry.priority = priority

            recency_seconds = time.time() - entry.timestamp
            new_score = self._scoring_fn(
                entry.key, entry.priority, entry.frequency, recency_seconds, entry.size
            )

            entry.score = new_score

            heapq.heapify(self._heap)

            return True

    def remove(self, key: str) -> bool:
        """
        Remove item from queue.

        Args:
            key: Item key.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            if key not in self._entry_map:
                return False

            self._removed_keys.add(key)
            del self._entry_map[key]

            while self._heap and self._heap[0].key in self._removed_keys:
                entry = heapq.heappop(self._heap)
                self._removed_keys.discard(entry.key)

            return True

    def contains(self, key: str) -> bool:
        """Check if key is in queue."""
        with self._lock:
            return key in self._entry_map and key not in self._removed_keys

    def size(self) -> int:
        """Get current queue size."""
        with self._lock:
            return len(self._entry_map)

    def clear(self):
        """Clear all items from queue."""
        with self._lock:
            self._heap.clear()
            self._entry_map.clear()
            self._removed_keys.clear()

    def _update(self, key: str, priority: int, frequency: int, size: int):
        """Update existing entry (must hold lock)."""
        entry = self._entry_map[key]
        entry.priority = priority
        entry.frequency = frequency
        entry.size = size

        recency_seconds = time.time() - entry.timestamp
        new_score = self._scoring_fn(
            entry.key, entry.priority, entry.frequency, recency_seconds, entry.size
        )

        entry.score = new_score

        heapq.heapify(self._heap)

    def _evict(self) -> Optional[str]:
        """Evict lowest priority item (must hold lock)."""
        while self._heap:
            entry = heapq.heappop(self._heap)

            if entry.key in self._removed_keys:
                self._removed_keys.discard(entry.key)
                continue

            del self._entry_map[entry.key]
            return entry.key

        return None

    def get_eviction_candidates(self, count: int = 10) -> List[Tuple[str, float]]:
        """
        Get top eviction candidates.

        Args:
            count: Number of candidates to return.

        Returns:
            List of (key, score) tuples.
        """
        with self._lock:
            valid_entries = [e for e in self._heap if e.key not in self._removed_keys]

            valid_entries.sort(key=lambda e: e.score)

            return [(e.key, e.score) for e in valid_entries[:count]]

    def get_stats(self) -> dict:
        """Get queue statistics."""
        with self._lock:
            return {
                "size": len(self._entry_map),
                "max_size": self.max_size,
                "pending_removals": len(self._removed_keys),
            }
