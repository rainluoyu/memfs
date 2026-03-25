"""API module for MemFS."""

from .native import (
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
    set_global_fs,
    get_global_fs,
)
from .object import MemFileSystem

__all__ = [
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
    "set_global_fs",
    "get_global_fs",
    "MemFileSystem",
]
