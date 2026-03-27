"""
File lock manager for MemFS.
Provides file-level locking for thread-safe operations.
"""

import threading
from typing import Dict, Optional
from dataclasses import dataclass, field
import time


@dataclass
class LockInfo:
    """Information about a file lock."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    write_lock: threading.RLock = field(default_factory=threading.RLock)
    read_count: int = 0
    read_lock: threading.Lock = field(default_factory=threading.Lock)
    write_waiting: int = 0


class FileLockManager:
    """
    File lock manager for thread-safe file operations.

    Provides read-write locking semantics:
    - Multiple readers can access a file simultaneously
    - Writers have exclusive access
    """

    def __init__(self):
        """Initialize file lock manager."""
        self._lock = threading.Lock()
        self._locks: Dict[str, LockInfo] = {}

    def _get_lock_info(self, virtual_path: str) -> LockInfo:
        """Get or create lock info for a file."""
        with self._lock:
            if virtual_path not in self._locks:
                self._locks[virtual_path] = LockInfo()
            return self._locks[virtual_path]

    def _cleanup_lock(self, virtual_path: str):
        """Clean up lock if no longer needed."""
        with self._lock:
            if virtual_path in self._locks:
                lock_info = self._locks[virtual_path]
                if lock_info.read_count == 0 and lock_info.write_waiting == 0:
                    del self._locks[virtual_path]

    def acquire_read(self, virtual_path: str, timeout: Optional[float] = None) -> bool:
        """
        Acquire read lock.

        Args:
            virtual_path: Virtual file path.
            timeout: Lock acquisition timeout.

        Returns:
            True if lock acquired.
        """
        lock_info = self._get_lock_info(virtual_path)

        acquired = lock_info.read_lock.acquire(timeout=timeout if timeout else -1)
        if not acquired:
            return False

        try:
            with lock_info.lock:
                lock_info.read_count += 1
        finally:
            lock_info.read_lock.release()

        return True

    def release_read(self, virtual_path: str):
        """
        Release read lock.

        Args:
            virtual_path: Virtual file path.
        """
        lock_info = self._get_lock_info(virtual_path)

        with lock_info.lock:
            lock_info.read_count -= 1
            if lock_info.read_count == 0:
                pass

        self._cleanup_lock(virtual_path)

    def acquire_write(self, virtual_path: str, timeout: Optional[float] = None) -> bool:
        """
        Acquire write lock.

        Args:
            virtual_path: Virtual file path.
            timeout: Lock acquisition timeout.

        Returns:
            True if lock acquired.
        """
        lock_info = self._get_lock_info(virtual_path)

        with lock_info.lock:
            lock_info.write_waiting += 1

        acquired = lock_info.write_lock.acquire(timeout=timeout if timeout else -1)

        with lock_info.lock:
            lock_info.write_waiting -= 1

        return acquired

    def release_write(self, virtual_path: str):
        """
        Release write lock.

        Args:
            virtual_path: Virtual file path.
        """
        lock_info = self._get_lock_info(virtual_path)
        lock_info.write_lock.release()

        self._cleanup_lock(virtual_path)

    def upgrade_to_write(
        self,
        virtual_path: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Upgrade from read lock to write lock.

        Args:
            virtual_path: Virtual file path.
            timeout: Lock acquisition timeout.

        Returns:
            True if upgrade successful.
        """
        self.release_read(virtual_path)
        return self.acquire_write(virtual_path, timeout)

    def downgrade_to_read(self, virtual_path: str):
        """
        Downgrade from write lock to read lock.

        Args:
            virtual_path: Virtual file path.
        """
        lock_info = self._get_lock_info(virtual_path)

        with lock_info.lock:
            lock_info.read_count += 1

        self.release_write(virtual_path)

    def clear(self):
        """Clear all locks."""
        with self._lock:
            self._locks.clear()

    def get_lock_count(self) -> int:
        """Get number of active locks."""
        with self._lock:
            return len(self._locks)
