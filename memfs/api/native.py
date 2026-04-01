"""
Native-style API for MemFS.
Provides open(), listdir(), and other familiar functions.
"""

import os as _os
from typing import List, Optional, Union

from ..core.filesystem import MemFileSystem
from ..core.instance_manager import (
    get_global_instance_manager,
    reset_global_instance_manager,
    InstanceConflictError,
    get_unique_temp_path,
)


_global_fs: Optional[MemFileSystem] = None
_global_fs_config: dict = {}


def init(
    memory_limit_bytes: Optional[int] = None,
    memory_limit_percent: float = 0.8,
    persist_path: Optional[str] = None,
    storage_mode: str = "temp",
    worker_threads: int = 4,
    enable_logging: bool = True,
    log_path: Optional[str] = None,
    priority_boost_threshold: int = 10,
) -> MemFileSystem:
    """
    Initialize or get a MemFileSystem instance.

    Instance Management Rules:
    - Temp mode (default): Each instance must use a unique persist_path.
      - If persist_path is None, automatically generates a unique temporary path.
      - Cannot overlap with any existing instance (temp or persist) on the same path.
    - Persist mode: Multiple instances can share the same persist_path (reference counted).
    - Reference counting: init() increments count, close() decrements.
    - Instance auto-shutdown when ref count reaches zero.

    Args:
        memory_limit_bytes: Memory usage limit in bytes. If provided, takes precedence over memory_limit_percent.
        memory_limit_percent: Memory usage limit as fraction of total (0-1). Default is 0.8 (80%).
        persist_path: Root path for real files.
                      If None and storage_mode="temp", auto-generates unique path.
                      Required for storage_mode="persist".
        storage_mode: Storage mode - "temp" (temporary, cleanup on shutdown) or "persist" (keep files after shutdown).
        worker_threads: Number of background worker threads.
        enable_logging: Whether to enable operation logging.
        log_path: Path for operation log file.
        priority_boost_threshold: Access count to boost priority.

    Returns:
        MemFileSystem instance.

    Raises:
        InstanceConflictError: If trying to create a temp mode instance with a path
                               that conflicts with an existing instance.

    Example:
        >>> import memfs

        >>> # Temp mode with auto-generated unique path (default)
        >>> fs1 = memfs.init()  # Auto-generates unique temp path
        >>> fs2 = memfs.init()  # Different unique path

        >>> # Temp mode with explicit path (must be unique)
        >>> fs3 = memfs.init(persist_path="./data_a", storage_mode="temp")
        >>> fs4 = memfs.init(persist_path="./data_b", storage_mode="temp")

        >>> # Persist mode can share path
        >>> fs5 = memfs.init(persist_path="./shared", storage_mode="persist")
        >>> fs6 = memfs.init(persist_path="./shared", storage_mode="persist")  # Same instance

        >>> # Temp mode conflict - raises InstanceConflictError
        >>> fs7 = memfs.init(persist_path="./shared", storage_mode="temp")
        Traceback (most recent call last):
            ...
        InstanceConflictError: ...
    """
    global _global_fs, _global_fs_config

    instance_manager = get_global_instance_manager()
    fs = instance_manager.get_or_create_instance(
        persist_path=persist_path,
        memory_limit_bytes=memory_limit_bytes,
        memory_limit_percent=memory_limit_percent,
        storage_mode=storage_mode,
        worker_threads=worker_threads,
        enable_logging=enable_logging,
        log_path=log_path,
        priority_boost_threshold=priority_boost_threshold,
    )
    _global_fs = fs
    _global_fs_config = {
        "memory_limit_bytes": memory_limit_bytes,
        "memory_limit_percent": memory_limit_percent,
        "persist_path": persist_path,
        "storage_mode": storage_mode,
        "worker_threads": worker_threads,
        "enable_logging": enable_logging,
        "log_path": log_path,
        "priority_boost_threshold": priority_boost_threshold,
    }
    return fs


def _get_fs() -> MemFileSystem:
    """Get or create global filesystem instance."""
    global _global_fs

    if _global_fs is None:
        _global_fs = MemFileSystem()

    return _global_fs


def set_global_fs(fs: MemFileSystem):
    """
    Set global filesystem instance.

    Args:
        fs: MemFileSystem instance.
    """
    global _global_fs
    _global_fs = fs


def get_global_fs() -> MemFileSystem:
    """
    Get global filesystem instance.

    Returns:
        MemFileSystem instance.
    """
    return _get_fs()


def open(path: str, mode: str = "rb", priority: int = 5):
    """
    Open a file in the virtual filesystem.

    Args:
        path: File path (e.g., '/data.txt').
        mode: File mode ('r', 'w', 'a', 'rb', 'wb', 'ab').
        priority: File priority (0-10, higher = keep in memory longer).

    Returns:
        VirtualFile instance.

    Example:
        >>> with open('/data.txt', 'w') as f:
        ...     f.write(b'hello')
        >>> with open('/data.txt', 'r') as f:
        ...     data = f.read()
    """
    return _get_fs().open(path, mode, priority)


def read(path: str, priority: Optional[int] = None) -> bytes:
    """
    Read entire file.

    Args:
        path: File path.
        priority: Optional priority for this access.

    Returns:
        File contents.

    Example:
        >>> data = read('/data.txt')
    """
    return _get_fs().read(path, priority)


def write(path: str, data: Union[bytes, str], priority: int = 5) -> int:
    """
    Write entire file.

    Args:
        path: File path.
        data: Data to write.
        priority: File priority.

    Returns:
        Number of bytes written.

    Example:
        >>> write('/data.txt', b'hello')
        5
    """
    return _get_fs().write(path, data, priority)


def exists(path: str) -> bool:
    """
    Check if file exists.

    Args:
        path: File path.

    Returns:
        True if exists.
    """
    return _get_fs().exists(path)


def delete(path: str) -> bool:
    """
    Delete a file.

    Args:
        path: File path.

    Returns:
        True if deleted.
    """
    return _get_fs().delete(path)


def mkdir(path: str) -> bool:
    """
    Create directory.

    Args:
        path: Directory path.

    Returns:
        True if created.
    """
    return _get_fs().mkdir(path)


def rmdir(path: str) -> bool:
    """
    Remove directory.

    Args:
        path: Directory path.

    Returns:
        True if removed.
    """
    return _get_fs().rmdir(path)


def listdir(path: str = "/") -> List[str]:
    """
    List directory contents.

    Args:
        path: Directory path.

    Returns:
        List of names.
    """
    return _get_fs().listdir(path)


def glob(pattern: str) -> List[str]:
    """
    Match paths using glob pattern.

    Args:
        pattern: Glob pattern (e.g., '*.txt', '**/*.py').

    Returns:
        List of matching paths.
    """
    return _get_fs().glob(pattern)


def set_priority(path: str, priority: int) -> bool:
    """
    Set file priority.

    Args:
        path: File path.
        priority: Priority level (0-10).

    Returns:
        True if set.
    """
    return _get_fs().set_priority(path, priority)


def get_priority(path: str) -> Optional[int]:
    """
    Get file priority.

    Args:
        path: File path.

    Returns:
        Priority or None.
    """
    return _get_fs().get_priority(path)


def preload(path: str, priority: int = 5) -> str:
    """
    Preload file into memory.

    Args:
        path: File path.
        priority: File priority.

    Returns:
        Task ID.
    """
    return _get_fs().preload(path, priority)


def gc(target_usage: float = 0.5) -> int:
    """
    Trigger garbage collection.

    Args:
        target_usage: Target memory usage (0-1).

    Returns:
        Number of files swapped out.
    """
    return _get_fs().gc(target_usage)


def get_stats() -> dict:
    """
    Get filesystem statistics.

    Returns:
        Statistics dictionary.
    """
    return _get_fs().get_stats()


def get_file_info(path: str) -> Optional[dict]:
    """
    Get file information.

    Args:
        path: File path.

    Returns:
        File info or None.
    """
    return _get_fs().get_file_info(path)


def get_memory_map() -> dict:
    """
    Get memory map showing all cached files and their locations.

    Returns:
        Dictionary mapping file paths to their info (location, size, priority).

    Example:
        >>> import memfs
        >>> memfs.write('/data.txt', b'hello')
        >>> memory_map = memfs.get_memory_map()
        >>> print(memory_map)
        {
            "/data.txt": {"location": "memory", "size": 5, "priority": 5, "in_memory": True},
        }
    """
    return _get_fs().get_memory_map()


def clear_persist() -> bool:
    """
    Clear persistent storage directory.

    This deletes all files in the persist_path directory.
    Use with caution - this is irreversible!

    Returns:
        True if cleared, False if directory does not exist.

    Example:
        >>> import memfs
        >>> memfs.clear_persist()  # Delete all persistent data
    """
    import shutil
    from pathlib import Path

    fs = _get_fs()
    persist_path = fs._persist_path if hasattr(fs, "_persist_path") else "./memfs_data"

    persist_dir = Path(persist_path)
    if persist_dir.exists():
        shutil.rmtree(persist_dir)
        return True
    return False


def close(fs: Optional[MemFileSystem] = None) -> bool:
    """
    Close a MemFileSystem instance.

    Decrements reference count for the instance.
    Instance is shut down and removed when ref count reaches zero.
    If fs is None, closes the global instance.

    Args:
        fs: Optional MemFileSystem instance to close. If None, closes global instance.

    Returns:
        True if instance was shut down and removed, False if still in use.

    Example:
        >>> import memfs
        >>> fs = memfs.init(persist_path="./data")
        >>> memfs.close(fs)  # Decrement ref count
        >>> memfs.close()  # Close global instance
    """
    global _global_fs

    instance_manager = get_global_instance_manager()

    if fs is None:
        fs = _global_fs

    if fs is not None:
        result = instance_manager.release_instance(fs)
        if result:
            _global_fs = None
        return result
    return False


def close_instance(persist_path: str) -> bool:
    """
    Close an instance by persist_path.

    Decrements reference count for the instance associated with the given path.
    Instance is shut down when ref count reaches zero.

    Args:
        persist_path: The persist_path of the instance to close.

    Returns:
        True if instance was shut down and removed, False if still in use or not found.

    Example:
        >>> import memfs
        >>> fs = memfs.init(persist_path="./data")
        >>> memfs.close_instance("./data")  # Close instance by path
    """
    instance_manager = get_global_instance_manager()

    fs = None
    for key, instance in instance_manager._instances.items():
        if instance._persist_path == str(
            __import__("pathlib").Path(persist_path).expanduser().resolve()
        ):
            fs = instance
            break

    if fs is not None:
        global _global_fs
        result = instance_manager.release_instance(fs)
        if result and _global_fs is fs:
            _global_fs = None
        return result
    return False


def get_instance_stats() -> dict:
    """
    Get statistics for all managed instances.

    Returns:
        Dictionary with instance keys and their stats (ref_count, storage_mode, persist_path).

    Example:
        >>> import memfs
        >>> fs1 = memfs.init(persist_path="./data_a")
        >>> fs2 = memfs.init(persist_path="./data_b")
        >>> stats = memfs.get_instance_stats()
        >>> print(stats)
        {
            "/absolute/path/to/data_a": {"ref_count": 1, "storage_mode": "temp", "persist_path": "..."},
            "/absolute/path/to/data_b": {"ref_count": 1, "storage_mode": "temp", "persist_path": "..."},
        }
    """
    instance_manager = get_global_instance_manager()
    return instance_manager.get_instance_stats()


def get_instance_count() -> int:
    """
    Get the number of active instances.

    Returns:
        Number of instances being managed.

    Example:
        >>> import memfs
        >>> fs1 = memfs.init(persist_path="./data_a")
        >>> fs2 = memfs.init(persist_path="./data_b")
        >>> count = memfs.get_instance_count()
        >>> print(count)  # Output: 2
    """
    instance_manager = get_global_instance_manager()
    return instance_manager.get_instance_count()


def has_instance(persist_path: str) -> bool:
    """
    Check if an instance exists for the given persist_path.

    Args:
        persist_path: The persist_path to check.

    Returns:
        True if instance exists.

    Example:
        >>> import memfs
        >>> fs = memfs.init(persist_path="./data")
        >>> memfs.has_instance("./data")  # True
        >>> memfs.has_instance("./other")  # False
    """
    instance_manager = get_global_instance_manager()
    return instance_manager.has_instance(persist_path)


def close_all_instances() -> None:
    """
    Close all managed instances.

    This shuts down all instances regardless of reference count.
    Use with caution - this will close all active instances.

    Example:
        >>> import memfs
        >>> fs1 = memfs.init(persist_path="./data_a")
        >>> fs2 = memfs.init(persist_path="./data_b")
        >>> memfs.close_all_instances()  # Close everything
    """
    instance_manager = get_global_instance_manager()
    instance_manager.close_all(wait=True)
    global _global_fs
    _global_fs = None
