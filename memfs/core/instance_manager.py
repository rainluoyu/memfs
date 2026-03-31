"""
Multi-instance manager for MemFS.
Supports multiple independent instances with reference counting.
"""

import threading
from typing import Dict, Optional
from pathlib import Path

from ..core.filesystem import MemFileSystem


class InstanceManager:
    """
    Manages multiple MemFileSystem instances.

    Features:
    - Named instances based on persist_path
    - Reference counting for automatic lifecycle management
    - Thread-safe instance creation and retrieval
    """

    def __init__(self):
        """Initialize the InstanceManager."""
        self._lock = threading.Lock()
        self._instances: Dict[str, MemFileSystem] = {}
        self._ref_counts: Dict[str, int] = {}

    def _normalize_path(self, path: str) -> str:
        """
        Normalize persist_path to use as instance key.

        Args:
            path: Input path.

        Returns:
            Absolute normalized path string.
        """
        return str(Path(path).expanduser().resolve())

    def get_or_create_instance(
        self,
        persist_path: str = "./memfs_data",
        memory_limit: float = 0.8,
        storage_mode: str = "temp",
        worker_threads: int = 4,
        enable_logging: bool = True,
        log_path: Optional[str] = None,
        priority_boost_threshold: int = 10,
    ) -> MemFileSystem:
        """
        Get or create a MemFileSystem instance for the given persist_path.

        If an instance with the same persist_path already exists, returns it
        and increments the reference count. Otherwise, creates a new instance.

        Args:
            persist_path: Root path for real files (used as instance key).
            memory_limit: Memory usage limit (0-1).
            storage_mode: Storage mode - "temp" or "persist".
            worker_threads: Number of background worker threads.
            enable_logging: Whether to enable operation logging.
            log_path: Path for operation log file.
            priority_boost_threshold: Access count to boost priority.

        Returns:
            MemFileSystem instance.
        """
        instance_key = self._normalize_path(persist_path)

        with self._lock:
            if instance_key in self._instances:
                self._ref_counts[instance_key] += 1
                return self._instances[instance_key]

            config = {
                "memory_limit": memory_limit,
                "persist_path": persist_path,
                "storage_mode": storage_mode,
                "worker_threads": worker_threads,
                "enable_logging": enable_logging,
                "log_path": log_path,
                "priority_boost_threshold": priority_boost_threshold,
            }

            fs = MemFileSystem(**config)
            self._instances[instance_key] = fs
            self._ref_counts[instance_key] = 1

            return fs

    def release_instance(self, fs: MemFileSystem) -> bool:
        """
        Release a reference to an instance.

        Decrements the reference count. When count reaches zero,
        shuts down and removes the instance.

        Args:
            fs: MemFileSystem instance to release.

        Returns:
            True if instance was shut down and removed, False if still in use.
        """
        instance_key = None

        with self._lock:
            for key, instance in self._instances.items():
                if instance is fs:
                    instance_key = key
                    break

            if instance_key is None:
                return False

            self._ref_counts[instance_key] -= 1

            if self._ref_counts[instance_key] <= 0:
                fs.shutdown(wait=True)
                del self._instances[instance_key]
                del self._ref_counts[instance_key]
                return True

            return False

    def get_instance_count(self) -> int:
        """
        Get the number of active instances.

        Returns:
            Number of instances being managed.
        """
        with self._lock:
            return len(self._instances)

    def get_instance_stats(self) -> dict:
        """
        Get statistics for all managed instances.

        Returns:
            Dictionary with instance keys and their stats.
        """
        stats = {}

        with self._lock:
            for key, fs in self._instances.items():
                stats[key] = {
                    "ref_count": self._ref_counts[key],
                    "storage_mode": fs._storage_mode,
                    "persist_path": fs._persist_path,
                }

        return stats

    def close_all(self, wait: bool = True):
        """
        Close all managed instances.

        Args:
            wait: Wait for shutdown to complete.
        """
        with self._lock:
            for fs in list(self._instances.values()):
                fs.shutdown(wait=wait)

            self._instances.clear()
            self._ref_counts.clear()

    def has_instance(self, persist_path: str) -> bool:
        """
        Check if an instance exists for the given persist_path.

        Args:
            persist_path: Path to check.

        Returns:
            True if instance exists.
        """
        instance_key = self._normalize_path(persist_path)
        with self._lock:
            return instance_key in self._instances


_global_instance_manager: Optional[InstanceManager] = None
_instance_manager_lock = threading.Lock()


def get_global_instance_manager() -> InstanceManager:
    """
    Get the global InstanceManager singleton.

    Returns:
        Global InstanceManager instance.
    """
    global _global_instance_manager

    with _instance_manager_lock:
        if _global_instance_manager is None:
            _global_instance_manager = InstanceManager()

        return _global_instance_manager


def reset_global_instance_manager():
    """
    Reset the global InstanceManager (for testing purposes).

    This closes all instances and creates a new manager.
    """
    global _global_instance_manager

    with _instance_manager_lock:
        if _global_instance_manager is not None:
            _global_instance_manager.close_all(wait=True)
            _global_instance_manager = None
