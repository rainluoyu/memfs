"""Core module for MemFS."""

from .filesystem import MemFileSystem
from .file import VirtualFile
from .directory import VirtualDirectory, DirectoryManager

__all__ = ["MemFileSystem", "VirtualFile", "VirtualDirectory", "DirectoryManager"]
