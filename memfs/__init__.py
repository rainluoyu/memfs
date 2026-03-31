"""
MemFS - Memory-First File System

A file system that prioritizes in-memory storage with automatic
disk tiering, access tracking, and garbage collection.
"""

from .api.native import (
    open,
    read,
    write,
    exists,
    delete,
    mkdir,
    rmdir,
    listdir,
    glob,
    set_priority,
    get_priority,
    preload,
    gc,
    get_stats,
    get_file_info,
    get_memory_map,
    init,
    clear_persist,
    set_global_fs,
    get_global_fs,
)
from .api.object import MemFileSystem
from .core.file import VirtualFile
from .core.directory import VirtualDirectory, DirectoryManager
from .utils.stats import Statistics
from .utils.logger import OperationLogger, OperationType

__version__ = "0.1.0"
__all__ = [
    "__version__",
    "MemFileSystem",
    "VirtualFile",
    "VirtualDirectory",
    "DirectoryManager",
    "Statistics",
    "OperationLogger",
    "OperationType",
    "open",
    "read",
    "write",
    "exists",
    "delete",
    "mkdir",
    "rmdir",
    "listdir",
    "glob",
    "set_priority",
    "get_priority",
    "preload",
    "gc",
    "get_stats",
    "get_file_info",
    "get_memory_map",
    "init",
    "clear_persist",
    "set_global_fs",
    "get_global_fs",
]
