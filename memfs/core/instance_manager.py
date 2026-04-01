"""
Multi-instance manager for MemFS.
Supports multiple independent instances with reference counting.

Instance Conflict Rules:
- Temp mode: Each instance must use a unique persist_path. Cannot overlap with existing temp or persist instances.
- Persist mode: Multiple instances can share the same persist_path (reference counted).
- Temp mode cannot overlap with persist mode on the same path.
"""

import tempfile
import threading
from typing import Dict, Optional
from pathlib import Path

from ..core.filesystem import MemFileSystem


class InstanceConflictError(Exception):
    """
    Raised when attempting to create conflicting instances.

    This occurs when:
    - Trying to create a temp mode instance with a path already used by another temp instance
    - Trying to create a temp mode instance with a path used by a persist instance
    - Trying to create a persist mode instance with a path used by a temp instance
    """

    pass


class InstanceManager:
    """
    Manages multiple MemFileSystem instances.

    Features:
    - Named instances based on persist_path
    - Reference counting for automatic lifecycle management
    - Thread-safe instance creation and retrieval
    - Temp mode path conflict detection
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

    @staticmethod
    def get_unique_temp_path() -> str:
        """
        Generate a unique temporary path for temp mode instances.

        Creates a temporary directory under system temp folder with unique name.
        The directory is NOT created immediately - this just returns a unique path.

        Returns:
            Unique temporary path string.

        Example:
            >>> path = InstanceManager.get_unique_temp_path()
            >>> # Returns something like: /tmp/memfs_a7b3c9d2e1f4
        """
        import uuid

        temp_base = tempfile.gettempdir()
        unique_name = f"memfs_{uuid.uuid4().hex[:12]}"
        return str(Path(temp_base) / unique_name)

    def get_or_create_instance(
        self,
        persist_path: Optional[str] = None,
        memory_limit_bytes: Optional[int] = None,
        memory_limit_percent: float = 0.8,
        storage_mode: str = "temp",
        worker_threads: int = 4,
        enable_logging: bool = True,
        log_path: Optional[str] = None,
        priority_boost_threshold: int = 10,
    ) -> MemFileSystem:
        """
        Get or create a MemFileSystem instance for the given persist_path.

        Instance Conflict Rules:
        - Temp mode: Must use unique path. Cannot overlap with any existing instance (temp or persist).
        - Persist mode: Can share path with other persist instances (reference counted).
        - Temp mode cannot overlap with persist mode on the same path.

        If persist_path is None and storage_mode is "temp", automatically generates
        a unique temporary path using get_unique_temp_path().

        Args:
            persist_path: Root path for real files. If None and storage_mode="temp", auto-generates unique path.
            memory_limit_bytes: Memory usage limit in bytes. If provided, takes precedence over memory_limit_percent.
            memory_limit_percent: Memory usage limit as fraction of total (0-1). Default is 0.8 (80%).
            storage_mode: Storage mode - "temp" or "persist".
            worker_threads: Number of background worker threads.
            enable_logging: Whether to enable operation logging.
            log_path: Path for operation log file.
            priority_boost_threshold: Access count to boost priority.

        Returns:
            MemFileSystem instance.

        Raises:
            InstanceConflictError: If trying to create conflicting temp mode instance.

        Example:
            >>> # Temp mode with auto-generated unique path
            >>> fs1 = manager.get_or_create_instance(storage_mode="temp")
            >>> fs2 = manager.get_or_create_instance(storage_mode="temp")  # Different path

            >>> # Persist mode can share path
            >>> fs3 = manager.get_or_create_instance(persist_path="./data", storage_mode="persist")
            >>> fs4 = manager.get_or_create_instance(persist_path="./data", storage_mode="persist")  # Same instance

            >>> # Temp mode conflict - raises InstanceConflictError
            >>> fs5 = manager.get_or_create_instance(persist_path="./data", storage_mode="temp")
            Traceback (most recent call last):
                ...
            InstanceConflictError: ...
        """
        # Auto-generate unique path for temp mode if not specified
        if persist_path is None:
            if storage_mode == "temp":
                persist_path = self.get_unique_temp_path()
            else:
                persist_path = "./memfs_data"

        instance_key = self._normalize_path(persist_path)

        with self._lock:
            if instance_key in self._instances:
                existing_fs = self._instances[instance_key]
                existing_mode = existing_fs._storage_mode

                # Temp mode conflicts with any existing instance on same path
                if storage_mode == "temp":
                    raise InstanceConflictError(
                        f"Cannot create temp mode instance with persist_path '{persist_path}' "
                        f"because it conflicts with existing {existing_mode} mode instance. "
                        f"Temp mode instances must use unique paths. "
                        f"Use get_unique_temp_path() to generate a unique path or use persist mode."
                    )

                # Persist mode can share with other persist instances
                if existing_mode == "temp":
                    raise InstanceConflictError(
                        f"Cannot create persist mode instance with persist_path '{persist_path}' "
                        f"because it conflicts with existing temp mode instance. "
                        f"Temp mode instances require exclusive use of the path."
                    )

                # Same persist mode - increment ref count and return existing
                self._ref_counts[instance_key] += 1
                return existing_fs

            config = {
                "memory_limit_bytes": memory_limit_bytes,
                "memory_limit_percent": memory_limit_percent,
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


def get_unique_temp_path() -> str:
    """
    Generate a unique temporary path for temp mode instances.

    Convenience function that delegates to InstanceManager.get_unique_temp_path().

    Returns:
        Unique temporary path string.

    Example:
        >>> import memfs
        >>> path = memfs.get_unique_temp_path()
        >>> fs = memfs.init(persist_path=path, storage_mode="temp")
    """
    return InstanceManager.get_unique_temp_path()


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

    Note: This will close all active instances regardless of mode.
    Use with caution in tests.
    """
    global _global_instance_manager

    with _instance_manager_lock:
        if _global_instance_manager is not None:
            _global_instance_manager.close_all(wait=True)
            _global_instance_manager = None
