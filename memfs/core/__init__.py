"""Core module for MemFS."""

from .filesystem import MemFileSystem
from .file import VirtualFile
from .directory import VirtualDirectory, DirectoryManager
from .instance_manager import (
    InstanceManager,
    get_global_instance_manager,
    reset_global_instance_manager,
)

__all__ = [
    "MemFileSystem",
    "VirtualFile",
    "VirtualDirectory",
    "DirectoryManager",
    "InstanceManager",
    "get_global_instance_manager",
    "reset_global_instance_manager",
]
