"""
Hybrid storage for MemFS.
Combines memory and real path storage with automatic tiering.
"""

import threading
import logging
import time
from typing import Any, Callable, Dict, Optional, Tuple
from pathlib import Path

from .memory import MemoryManager
from .real_path import RealPathStorage
from .lock_manager import FileLockManager
from ..cache.tracker import AccessTracker
from ..cache.priority import PriorityQueue
from ..utils.stats import Statistics
from ..async_worker.worker import AsyncWorker, TaskType


logger = logging.getLogger("memfs")


class HybridStorage:
    """
    Hybrid storage combining memory and real path storage.

    Automatically tiers files between memory and real paths based on
    access patterns, priority, and memory pressure.
    """

    def __init__(
        self,
        memory_limit: float = 0.8,
        persist_path: str = "./memfs_data",
        storage_mode: str = "temp",
        worker_threads: int = 4,
        on_swap: Optional[Callable[[str, str], None]] = None,
        directory_manager=None,
    ):
        """
        Initialize hybrid storage.

        Args:
            memory_limit: Memory usage limit (0-1).
            persist_path: Root path for real files.
            storage_mode: Storage mode - "temp" (temporary, cleanup on shutdown) or "persist" (keep files after shutdown).
            worker_threads: Number of background worker threads.
            on_swap: Callback for swap events (key, direction).
            directory_manager: Optional DirectoryManager for syncing persisted files.
        """
        self._lock = threading.Lock()
        self._on_swap = on_swap
        self.storage_mode = storage_mode
        self.persist_mode = storage_mode == "persist"

        self.memory = MemoryManager(
            memory_limit=memory_limit,
            on_eviction=self._on_memory_eviction,
        )

        temp_mode = storage_mode == "temp"
        self.real_storage = RealPathStorage(
            real_root=persist_path,
            temp_mode=temp_mode,
        )

        self.lock_manager = FileLockManager()

        self.tracker = AccessTracker()

        self.priority_queue = PriorityQueue(max_size=10000)

        self.worker = AsyncWorker(max_workers=worker_threads)

        self.stats = Statistics()
        self.stats.set_memory_limit(self.memory._max_bytes)
        self.stats.update_disk(path=persist_path, current_usage=0)

        self._pending_ops: Dict[str, str] = {}
        self._file_locations: Dict[str, str] = {}
        self._file_hashes: Dict[str, tuple] = {}
        self._directory_manager = directory_manager

        if self.persist_mode and self._directory_manager:
            self._sync_persisted_files_to_directories()

    def _on_memory_eviction(self, key: str):
        """Callback when memory manager evicts a file."""
        self._swap_out_async(key)

    def put(
        self,
        key: str,
        data: bytes,
        priority: int = 5,
        sync: bool = False,
    ) -> bool:
        """
        Store file in hybrid storage.

        Args:
            key: File key.
            data: File data.
            priority: File priority (0-10).
            sync: If True, wait for real path write.

        Returns:
            True if successful.
        """
        start_time = time.time()

        try:
            self.lock_manager.acquire_write(key, timeout=None)

            try:
                evicted_keys = self.memory.put(key, data, priority)

                self._file_locations[key] = "memory"

                mtime = time.time()
                size = len(data)
                self._file_hashes[key] = (mtime, size)
                self.real_storage.update_file_info(key, mtime, size)

                self.priority_queue.put(key, data, priority, frequency=1, size=size)

                self.tracker.record_access(key, is_write=True, size=size)

                self.stats.update_memory(
                    current_usage=self.memory.get_usage()["current_usage"],
                    file_count=len(self._file_locations),
                    total_size=sum(
                        self.memory.get_file_info(k).get("size", 0)
                        for k in self._file_locations
                        if self.memory.get_file_info(k)
                    ),
                )

                self._schedule_real_write(key, data, priority)

                duration_ms = (time.time() - start_time) * 1000
                self.stats.operations.record_write(duration_ms)

                self.stats.record_cache_hit()

                logger.debug("Put file: key=%s, priority=%d, location=memory", key, priority)
                return True

            finally:
                self.lock_manager.release_write(key)

        except Exception:
            logger.exception("Put file failed: key=%s", key)
            return False

    def _schedule_real_write(self, key: str, data: bytes, priority: int):
        """Schedule asynchronous write to real path."""

        def _write_real():
            with self._lock:
                if key in self._pending_ops:
                    return
                self._pending_ops[key] = "write"

            try:
                self.real_storage.write_sync(key, data)

                with self._lock:
                    if key in self._pending_ops:
                        del self._pending_ops[key]

                if self._on_swap:
                    self._on_swap(key, "write_real")

            except Exception:
                with self._lock:
                    if key in self._pending_ops:
                        del self._pending_ops[key]

        self.worker.submit(_write_real, task_type=TaskType.WRITE, priority=priority)

    def get(
        self,
        key: str,
        priority: Optional[int] = None,
        check_external: bool = True,
    ) -> Optional[bytes]:
        """
        Retrieve file from hybrid storage.

        Args:
            key: File key.
            priority: Optional priority update.
            check_external: If True, check for external modifications.

        Returns:
            File data or None.
        """
        start_time = time.time()

        self.tracker.record_access(key, is_write=False)

        data = self.memory.get(key)

        if data is not None:
            if check_external:
                file_info = self.real_storage.get_file_info(key)
                if file_info:
                    modified, new_data = self.real_storage.reload_if_modified(
                        key,
                        file_info["mtime"],
                        file_info["size"],
                    )
                    if modified:
                        if new_data:
                            data = new_data
                            self.memory.put(key, data, priority or 5)
                        else:
                            raise ExternalModificationError(
                                f"File {key} was modified externally"
                            )

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

        location = self._file_locations.get(key, "unknown")
        logger.debug("Get file: key=%s, location=%s, size=%d, duration_ms=%.2f", key, location, len(data) if data else 0, duration_ms)

        return data

    def contains(self, key: str) -> bool:
        """Check if file exists in storage."""
        with self._lock:
            return key in self._file_locations or self.real_storage.exists(key)

    def remove(self, key: str) -> bool:
        """
        Remove file from storage.

        Args:
            key: File key.

        Returns:
            True if removed.
        """
        with self._lock:
            self.lock_manager.acquire_write(key, timeout=None)

            try:
                removed_from_memory = self.memory.remove(key)

                self._schedule_real_delete(key)

                self.priority_queue.remove(key)
                self.tracker.remove_record(key)

                if key in self._file_locations:
                    del self._file_locations[key]
                if key in self._file_hashes:
                    del self._file_hashes[key]

                self.real_storage.clear_file_info(key)

                self.stats.update_memory(
                    current_usage=self.memory.get_usage()["current_usage"],
                    file_count=len(self._file_locations),
                )

                logger.debug("Remove file: key=%s, success=%s", key, removed_from_memory)
                return removed_from_memory

            finally:
                self.lock_manager.release_write(key)

    def _schedule_real_delete(self, key: str):
        """Schedule asynchronous delete from real path."""

        def _delete_real():
            with self._lock:
                if key in self._pending_ops:
                    return
                self._pending_ops[key] = "delete"

            try:
                self.real_storage.delete_sync(key)

                with self._lock:
                    if key in self._pending_ops:
                        del self._pending_ops[key]

                if self._on_swap:
                    self._on_swap(key, "delete_real")

            except Exception:
                with self._lock:
                    if key in self._pending_ops:
                        del self._pending_ops[key]

        self.worker.submit(_delete_real, task_type=TaskType.DELETE)

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
            if key not in self._file_locations and not self.memory.contains(key):
                return False

            self.memory.update_priority(key, priority)
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

            data = self.real_storage.read_sync(key)
            if data is None:
                return False

            self.memory.put(key, data, priority)

            with self._lock:
                self._file_locations[key] = "memory"
                mtime = time.time()
                size = len(data)
                self._file_hashes[key] = (mtime, size)

            self.stats.record_preload()

            return True

        return self.worker.submit(
            _preload,
            task_type=TaskType.PRELOAD,
            priority=priority,
        )

    def _swap_in(self, key: str) -> Optional[bytes]:
        """Swap file from real path to memory."""
        with self._lock:
            if key in self._pending_ops:
                return None

            self._pending_ops[key] = "swap_in"

        try:
            self.lock_manager.acquire_read(key, timeout=None)

            try:
                data = self.real_storage.read_sync(key)

                if data is None:
                    return None

                self.stats.record_swap_in()

                priority = 5
                file_info = self.real_storage.get_file_info(key)
                if file_info:
                    priority = file_info.get("priority", 5)

                self.memory.put(key, data, priority)

                with self._lock:
                    self._file_locations[key] = "memory"
                    if key in self._pending_ops:
                        del self._pending_ops[key]

                if self._on_swap:
                    self._on_swap(key, "swap_in")

                logger.debug("Swap in: key=%s, size=%d", key, len(data) if data else 0)
                return data

            finally:
                self.lock_manager.release_read(key)

        finally:
            with self._lock:
                if key in self._pending_ops:
                    del self._pending_ops[key]

    def _swap_out_async(self, key: str):
        """Schedule asynchronous swap-out."""

        def _swap_out():
            with self._lock:
                if key in self._pending_ops:
                    return
                self._pending_ops[key] = "swap_out"

            try:
                self.lock_manager.acquire_write(key, timeout=None)

                try:
                    data = self.memory.get(key)

                    if data is None:
                        return

                    file_info = self.real_storage.get_file_info(key)

                    need_write = True
                    if file_info:
                        cached_mtime, cached_size = (
                            file_info["mtime"],
                            file_info["size"],
                        )
                        current_mtime = time.time()
                        current_size = len(data)

                        if (
                            self.real_storage.exists(key)
                            and cached_size == current_size
                        ):
                            need_write = False

                    if need_write:
                        self.real_storage.write_sync(key, data)

                    self.memory.remove(key)

                    with self._lock:
                        if key in self._file_locations:
                            self._file_locations[key] = "real"
                        if key in self._pending_ops:
                            del self._pending_ops[key]

                    self.stats.record_swap_out()

                    if self._on_swap:
                        self._on_swap(key, "swap_out")

                finally:
                    self.lock_manager.release_write(key)

            except Exception:
                with self._lock:
                    if key in self._pending_ops:
                        del self._pending_ops[key]

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

        logger.debug("GC started: current_usage=%.2f%%, target_usage=%.2f%%", current["usage_percent"], target_usage * 100)

        if current_ratio <= target_usage:
            logger.debug("GC skipped: current usage already below target")
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

        logger.debug("GC completed: swapped=%d files, new_usage=%.2f%%", swapped, new_usage["usage_percent"])
        return swapped

    def get_stats(self) -> dict:
        """Get storage statistics."""
        mem_usage = self.memory.get_usage()

        real_files = self.real_storage.get_all_files()
        disk_usage = 0
        for k in real_files:
            file_info = self.memory.get_file_info(k)
            if file_info:
                disk_usage += file_info.get("size", 0)

        self.stats.update_memory(
            current_usage=mem_usage["current_usage"],
            file_count=mem_usage["file_count"],
        )

        self.stats.update_disk(
            path=str(self.real_storage.real_root),
            current_usage=disk_usage,
            file_count=len(real_files),
        )

        return self.stats.to_dict()

    def get_file_location(self, key: str) -> str:
        """
        Get file location.

        Args:
            key: File key.

        Returns:
            'memory', 'real', or 'unknown'.
        """
        with self._lock:
            if key in self._file_locations:
                return self._file_locations[key]
            if self.memory.contains(key):
                return "memory"
            if self.real_storage.exists(key):
                return "real"
            return "unknown"

    def shutdown(self, wait: bool = True) -> int:
        """
        Shut down storage.

        Args:
            wait: Whether to wait for pending operations.

        Returns:
            Number of pending operations if not waiting.
        """
        pending = self.worker.shutdown(wait=wait)
        self.real_storage.shutdown()
        return pending

    def _sync_persisted_files_to_directories(self):
        """Sync persisted files from disk to directory manager."""
        if not self._directory_manager:
            return

        all_files = self.real_storage.get_all_files()
        for virtual_path in all_files:
            # Use PurePosixPath for virtual paths (always use / separator)
            from pathlib import PurePosixPath

            pure_path = PurePosixPath(virtual_path)

            dir_path = str(pure_path.parent)
            if dir_path == ".":
                dir_path = "/"

            filename = pure_path.name

            directory = self._directory_manager.get_or_create_directory(dir_path)
            directory.add_file(filename)

            self._file_locations[virtual_path] = "real"

    def clear(self):
        """Clear all files from both memory and real path."""
        self.memory.clear()
        self.real_storage.clear()
        self._file_locations.clear()
        self._file_hashes.clear()
        self.priority_queue.clear()
        self.tracker.clear()
        self.stats.reset()
        self.lock_manager.clear()


class ExternalModificationError(Exception):
    """Raised when a file is modified externally."""

    pass
