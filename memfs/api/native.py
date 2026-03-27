"""
Native-style API for MemFS.
Provides open(), listdir(), and other familiar functions.
"""

import os as _os
from typing import List, Optional, Union

from ..core.filesystem import MemFileSystem


_global_fs: Optional[MemFileSystem] = None
_global_fs_config: dict = {}


def init(
    memory_limit: float = 0.8,
    persist_path: str = "./memfs_data",
    persist_mode: bool = False,
    temp_mode: bool = True,
    compress_memory: bool = True,
    worker_threads: int = 4,
    enable_logging: bool = True,
    log_path: Optional[str] = None,
    priority_boost_threshold: int = 10,
) -> MemFileSystem:
    """
    Initialize global MemFileSystem instance with custom configuration.

    Args:
        memory_limit: Memory usage limit (0-1).
        persist_path: Root path for real files.
        persist_mode: If True, keep files after shutdown (persistent mode).
        temp_mode: If True, use temp directory and cleanup on shutdown.
        compress_memory: If True, compress data in memory.
        worker_threads: Number of background worker threads.
        enable_logging: Whether to enable operation logging.
        log_path: Path for operation log file.
        priority_boost_threshold: Access count to boost priority.

    Returns:
        MemFileSystem instance.

    Example:
        >>> import memfs
        >>> fs = memfs.init(persist_mode=True, persist_path="./my_data")
    """
    global _global_fs, _global_fs_config

    if _global_fs is not None:
        _global_fs.shutdown(wait=True)

    _global_fs_config = {
        "memory_limit": memory_limit,
        "persist_path": persist_path,
        "persist_mode": persist_mode,
        "temp_mode": temp_mode,
        "compress_memory": compress_memory,
        "worker_threads": worker_threads,
        "enable_logging": enable_logging,
        "log_path": log_path,
        "priority_boost_threshold": priority_boost_threshold,
    }

    _global_fs = MemFileSystem(**_global_fs_config)
    return _global_fs


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
