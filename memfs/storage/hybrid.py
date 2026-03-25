"""
Hybrid storage for MemFS.
Combines memory and disk storage with automatic tiering.
"""

import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

from .memory import MemoryManager
from .disk import DiskStorage
from ..cache.tracker import AccessTracker
from ..cache.priority import PriorityQueue
from ..utils.stats import Statistics
from ..async_worker.worker import AsyncWorker, TaskType


class HybridStorage:
    """
    Hybrid storage combining memory and disk.

    Automatically tiers files between memory and disk based on
    access patterns, priority, and memory pressure.
    """

    def __init__(
        self,
        memory_limit: float = 0.8,
        persist_path: str = "./memfs_data",
        compression: str = "gzip",
        compression_level: int = 6,
        worker_threads: int = 4,
        on_swap: Optional[Callable[[str, str], None]] = None,
    ):
        """
        Initialize hybrid storage.

        Args:
            memory_limit: Memory usage limit (0-1).
            persist_path: Path for persistent storage.
            compression: Compression algorithm.
            compression_level: Compression level.
            worker_threads: Number of background worker threads.
            on_swap: Callback for swap events (key, direction).
        """
        self._lock = threading.Lock()
        self._on_swap = on_swap

        self.memory = MemoryManager(
            memory_limit=memory_limit, on_eviction=self._on_memory_eviction
        )

        self.disk = DiskStorage(
            storage_path=persist_path,
            compression=compression,
            compression_level=compression_level,
        )

        self.tracker = AccessTracker()

        self.priority_queue = PriorityQueue(max_size=10000)

        self.worker = AsyncWorker(max_workers=worker_threads)

        self.stats = Statistics()
        self.stats.set_memory_limit(self.memory._max_bytes)
        self.stats.update_disk(path=persist_path, current_usage=0)

        self._pending_swaps: Dict[str, bool] = {}
        self._file_locations: Dict[str, str] = {}

    def _on_memory_eviction(self, key: str):
        """Callback when memory manager evicts a file."""
        self._swap_out_async(key)

    def put(self, key: str, data: bytes, priority: int = 5, sync: bool = False) -> bool:
        """
        Store file in hybrid storage.

        Args:
            key: File key.
            data: File data.
            priority: File priority (0-10).
            sync: If True, wait for completion.

        Returns:
            True if successful.
        """
        start_time = time.time()

        try:
            evicted_keys = self.memory.put(key, data, priority)

            self._file_locations[key] = "memory"

            self.priority_queue.put(key, data, priority, frequency=1, size=len(data))

            self.tracker.record_access(key, is_write=True, size=len(data))

            self.stats.update_memory(
                current_usage=self.memory.get_usage()["current_usage"],
                file_count=len(self._file_locations),
                total_size=sum(
                    self.memory.get_file_info(k)["size"]
                    for k in self._file_locations
                    if self.memory.get_file_info(k)
                ),
            )

            duration_ms = (time.time() - start_time) * 1000
            self.stats.operations.record_write(duration_ms)

            self.stats.record_cache_hit()

            return True

        except Exception as e:
            return False

    def get(self, key: str, priority: Optional[int] = None) -> Optional[bytes]:
        """
        Retrieve file from hybrid storage.

        Args:
            key: File key.
            priority: Optional priority update.

        Returns:
            File data or None.
        """
        start_time = time.time()

        self.tracker.record_access(key, is_write=False)

        data = self.memory.get(key)

        if data is not None:
            self.stats.record_cache_hit()

            if priority is not None:
                self.set_priority(key, priority)

            duration_ms = (time.time() - start_time) * 1000
            self.stats.operations.record_read(duration_ms)

            return data

        self.stats.record_cache_miss()

        data = self._swap_in(key)

        if data is not None and priority is not None:
            self.set_priority(key, priority)

        duration_ms = (time.time() - start_time) * 1000
        self.stats.operations.record_read(duration_ms)

        return data

    def contains(self, key: str) -> bool:
        """Check if file exists in storage."""
        with self._lock:
            return key in self._file_locations

    def remove(self, key: str) -> bool:
        """
        Remove file from storage.

        Args:
            key: File key.

        Returns:
            True if removed.
        """
        with self._lock:
            removed_from_memory = self.memory.remove(key)
            removed_from_disk = self.disk.remove(key)

            self.priority_queue.remove(key)
            self.tracker.remove_record(key)

            if key in self._file_locations:
                del self._file_locations[key]

            self.stats.update_memory(
                current_usage=self.memory.get_usage()["current_usage"],
                file_count=len(self._file_locations),
            )

            return removed_from_memory or removed_from_disk

    def set_priority(self, key: str, priority: int) -> bool:
        """
        Update file priority.

        Args:
            key: File key.
            priority: New priority (0-10).

        Returns:
            True if updated.
        """
        with self._lock:
            if key not in self._file_locations:
                return False

            self.memory.update_priority(key, priority)
            self.disk.update_priority(key, priority)
            self.priority_queue.update_priority(key, priority)

            return True

    def preload(self, key: str, priority: int = 5) -> str:
        """
        Preload file into memory asynchronously.

        Args:
            key: File key.
            priority: File priority.

        Returns:
            Task ID.
        """

        def _preload():
            if self.memory.contains(key):
                return True

            data = self.disk.get(key)
            if data is None:
                return False

            self.memory.put(key, data, priority)
            self._file_locations[key] = "memory"

            self.stats.record_preload()

            return True

        return self.worker.submit(
            _preload, task_type=TaskType.PRELOAD, priority=priority
        )

    def _swap_in(self, key: str) -> Optional[bytes]:
        """Swap file from disk to memory."""
        with self._lock:
            if key in self._pending_swaps:
                return None

            self._pending_swaps[key] = True

        try:
            data = self.disk.get(key)

            if data is None:
                return None

            self.stats.record_swap_in()

            priority = 5
            metadata = self.disk.get_metadata(key)
            if metadata:
                priority = metadata.get("priority", 5)

            evicted = self.memory.put(key, data, priority)

            self._file_locations[key] = "memory"

            if self._on_swap:
                self._on_swap(key, "swap_in")

            return data

        finally:
            with self._lock:
                if key in self._pending_swaps:
                    del self._pending_swaps[key]

    def _swap_out_async(self, key: str):
        """Schedule asynchronous swap-out."""

        def _swap_out():
            with self._lock:
                if key in self._pending_swaps:
                    return
                self._pending_swaps[key] = True

            try:
                data = self.memory.get(key)

                if data is None:
                    return

                metadata = self.disk.get_metadata(key)
                priority = 5
                if metadata:
                    priority = metadata.get("priority", 5)

                self.disk.put(key, data, priority)

                self.memory.remove(key)

                if key in self._file_locations:
                    self._file_locations[key] = "disk"

                self.stats.record_swap_out()

                if self._on_swap:
                    self._on_swap(key, "swap_out")

            finally:
                with self._lock:
                    if key in self._pending_swaps:
                        del self._pending_swaps[key]

        self.worker.submit(_swap_out, task_type=TaskType.SWAP_OUT)

    def gc(self, target_usage: float = 0.5) -> int:
        """
        Trigger garbage collection.

        Args:
            target_usage: Target memory usage (0-1).

        Returns:
            Number of files swapped out.
        """
        current = self.memory.get_usage()
        current_ratio = current["usage_percent"] / 100

        if current_ratio <= target_usage:
            return 0

        candidates = self.priority_queue.get_eviction_candidates(count=100)

        swapped = 0

        for key, score in candidates:
            if not self.memory.contains(key):
                continue

            file_info = self.memory.get_file_info(key)
            if not file_info:
                continue

            if file_info["priority"] >= 9:
                continue

            self._swap_out_async(key)
            swapped += 1

            new_usage = self.memory.get_usage()
            if new_usage["usage_percent"] / 100 <= target_usage:
                break

        return swapped

    def get_stats(self) -> dict:
        """Get storage statistics."""
        mem_usage = self.memory.get_usage()
        disk_usage = self.disk.get_usage()

        self.stats.update_memory(
            current_usage=mem_usage["current_usage"], file_count=mem_usage["file_count"]
        )

        self.stats.update_disk(
            path=disk_usage["path"],
            current_usage=disk_usage["total_size"],
            file_count=disk_usage["file_count"],
        )

        return self.stats.to_dict()

    def get_file_location(self, key: str) -> str:
        """
        Get file location.

        Args:
            key: File key.

        Returns:
            'memory', 'disk', or 'unknown'.
        """
        with self._lock:
            return self._file_locations.get(key, "unknown")

    def shutdown(self, wait: bool = True):
        """
        Shut down storage.

        Args:
            wait: Whether to wait for pending operations.
        """
        self.worker.shutdown(wait=wait)

    def clear(self):
        """Clear all files from both memory and disk."""
        self.memory.clear()
        self.disk.clear()
        self._file_locations.clear()
        self.priority_queue.clear()
        self.tracker.clear()
        self.stats.reset()
