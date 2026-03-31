"""Core module for MemFS."""

from .filesystem import MemFileSystem
from .file import VirtualFile
from .directory import VirtualDirectory, DirectoryManager
from .instance_manager import (
    InstanceManager,
    InstanceConflictError,
    get_global_instance_manager,
    reset_global_instance_manager,
    get_unique_temp_path,
)

__all__ = [
    "MemFileSystem",
    "VirtualFile",
    "VirtualDirectory",
    "DirectoryManager",
    "InstanceManager",
    "InstanceConflictError",
    "get_global_instance_manager",
    "reset_global_instance_manager",
    "get_unique_temp_path",
]
