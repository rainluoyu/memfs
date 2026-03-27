"""
Virtual directory structure for MemFS.
Provides directory tree management and glob matching.
"""

import fnmatch
import threading
from pathlib import PurePosixPath
from typing import Dict, List, Optional, Set


class VirtualDirectory:
    """
    Virtual directory structure.

    Manages hierarchical directory tree with glob support.
    """

    def __init__(self, name: str, parent: Optional["VirtualDirectory"] = None):
        """
        Initialize directory.

        Args:
            name: Directory name.
            parent: Parent directory.
        """
        self.name = name
        self.parent = parent

        self._children: Dict[str, "VirtualDirectory"] = {}
        self._files: Set[str] = set()
        self._lock = threading.RLock()

    @property
    def path(self) -> str:
        """Get full path."""
        if self.parent is None:
            return "/"

        parts = []
        current = self
        while current.parent is not None:
            parts.append(current.name)
            current = current.parent
        return "/" + "/".join(reversed(parts))

    def add_file(self, filename: str) -> bool:
        """
        Add file to directory.

        Args:
            filename: File name.

        Returns:
            True if added.
        """
        with self._lock:
            if filename in self._files:
                return False
            self._files.add(filename)
            return True

    def remove_file(self, filename: str) -> bool:
        """
        Remove file from directory.

        Args:
            filename: File name.

        Returns:
            True if removed.
        """
        with self._lock:
            if filename not in self._files:
                return False
            self._files.remove(filename)
            return True

    def has_file(self, filename: str) -> bool:
        """Check if file exists in directory."""
        with self._lock:
            return filename in self._files

    def add_subdirectory(self, name: str) -> "VirtualDirectory":
        """
        Add subdirectory.

        Args:
            name: Directory name.

        Returns:
            New or existing directory.
        """
        with self._lock:
            if name not in self._children:
                self._children[name] = VirtualDirectory(name, self)
            return self._children[name]

    def get_subdirectory(self, name: str) -> Optional["VirtualDirectory"]:
        """
        Get subdirectory.

        Args:
            name: Directory name.

        Returns:
            Directory or None.
        """
        with self._lock:
            return self._children.get(name)

    def remove_subdirectory(self, name: str) -> bool:
        """
        Remove subdirectory.

        Args:
            name: Directory name.

        Returns:
            True if removed.
        """
        with self._lock:
            if name not in self._children:
                return False
            del self._children[name]
            return True

    def list_files(self) -> List[str]:
        """List all files in directory."""
        with self._lock:
            return list(self._files)

    def list_directories(self) -> List[str]:
        """List all subdirectories."""
        with self._lock:
            return list(self._children.keys())

    def list_all(self) -> List[str]:
        """List all children (files and directories)."""
        with self._lock:
            return list(self._files) + list(self._children.keys())

    def get_all_files(self, recursive: bool = False) -> List[str]:
        """
        Get all files.

        Args:
            recursive: Include subdirectories.

        Returns:
            List of file paths.
        """
        result = []

        with self._lock:
            for filename in self._files:
                if self.parent:
                    result.append(f"{self.path}/{filename}")
                else:
                    result.append(f"/{filename}")

            if recursive:
                for child in self._children.values():
                    result.extend(child.get_all_files(recursive=True))

        return result


class DirectoryManager:
    """
    Manages virtual directory tree.

    Provides path resolution and glob matching.
    """

    def __init__(self):
        """Initialize directory manager."""
        self._lock = threading.RLock()
        self._root = VirtualDirectory("")
        self._path_cache: Dict[str, VirtualDirectory] = {"/": self._root}

    def get_or_create_directory(self, path: str) -> VirtualDirectory:
        """
        Get or create directory at path.

        Args:
            path: Directory path.

        Returns:
            Directory instance.
        """
        with self._lock:
            if path in self._path_cache:
                return self._path_cache[path]

            parts = self._split_path(path)
            current = self._root

            for part in parts:
                if not part:
                    continue
                current = current.add_subdirectory(part)
                dir_path = "/" + "/".join(parts[: parts.index(part) + 1])
                self._path_cache[dir_path] = current

            self._path_cache[path] = current
            return current

    def get_directory(self, path: str) -> Optional[VirtualDirectory]:
        """
        Get directory at path.

        Args:
            path: Directory path.

        Returns:
            Directory or None.
        """
        with self._lock:
            return self._path_cache.get(path)

    def resolve_path(self, path: str) -> tuple:
        """
        Resolve path to (directory, filename).

        Args:
            path: File path.

        Returns:
            Tuple of (VirtualDirectory, filename).
        """
        pure_path = PurePosixPath(path)

        dir_path = str(pure_path.parent)
        if dir_path == ".":
            dir_path = "/"

        filename = pure_path.name

        directory = self.get_or_create_directory(dir_path)

        return directory, filename

    def exists(self, path: str) -> bool:
        """
        Check if path exists.

        Args:
            path: File or directory path.

        Returns:
            True if exists.
        """
        # 先检查是否是目录
        directory = self.get_directory(path)
        if directory is not None:
            return True
        
        # 再检查是否是文件
        pure_path = PurePosixPath(path)
        dir_path = str(pure_path.parent)
        if dir_path == ".":
            dir_path = "/"
        
        parent_dir = self.get_directory(dir_path)
        if parent_dir:
            return parent_dir.has_file(pure_path.name)
        
        return False

    def mkdir(self, path: str) -> bool:
        """
        Create directory.

        Args:
            path: Directory path.

        Returns:
            True if created.
        """
        with self._lock:
            self.get_or_create_directory(path)
            return True

    def rmdir(self, path: str) -> bool:
        """
        Remove directory.

        Args:
            path: Directory path.

        Returns:
            True if removed.
        """
        with self._lock:
            directory = self.get_directory(path)

            if not directory:
                return False

            if directory._files or directory._children:
                return False

            if directory.parent:
                directory.parent.remove_subdirectory(directory.name)

                if path in self._path_cache:
                    del self._path_cache[path]

                return True

            return False

    def listdir(self, path: str) -> List[str]:
        """
        List directory contents.

        Args:
            path: Directory path.

        Returns:
            List of names.
        """
        directory = self.get_directory(path)

        if not directory:
            raise FileNotFoundError(f"Directory not found: {path}")

        return directory.list_all()

    def glob(self, pattern: str) -> List[str]:
        """
        Match paths using glob pattern.

        Args:
            pattern: Glob pattern (e.g., '*.txt', '**/*.py').

        Returns:
            List of matching paths.
        """
        with self._lock:
            all_files = self._root.get_all_files(recursive=True)

        pattern_parts = pattern.split("/")

        if "**" in pattern_parts:
            return self._glob_recursive(all_files, pattern)
        else:
            return [f for f in all_files if fnmatch.fnmatch(f, pattern)]

    def _glob_recursive(self, files: List[str], pattern: str) -> List[str]:
        """Handle ** glob patterns."""
        result = []

        pattern_parts = pattern.split("/")

        for filepath in files:
            file_parts = filepath.split("/")[1:]

            if self._match_parts(file_parts, pattern_parts):
                result.append(filepath)

        return result

    def _match_parts(self, file_parts: List[str], pattern_parts: List[str]) -> bool:
        """Match file parts against pattern parts."""
        if not pattern_parts:
            return not file_parts

        if pattern_parts[0] == "**":
            if len(pattern_parts) == 1:
                return True

            for i in range(len(file_parts) + 1):
                if self._match_parts(file_parts[i:], pattern_parts[1:]):
                    return True
            return False

        if not file_parts:
            return False

        if fnmatch.fnmatch(file_parts[0], pattern_parts[0]):
            return self._match_parts(file_parts[1:], pattern_parts[1:])

        return False

    @staticmethod
    def _split_path(path: str) -> List[str]:
        """Split path into parts."""
        return [p for p in path.split("/") if p]

    def get_all_paths(self) -> List[str]:
        """Get all directory paths."""
        with self._lock:
            return list(self._path_cache.keys())

    def clear(self):
        """Clear all directories."""
        with self._lock:
            self._root = VirtualDirectory("")
            self._path_cache = {"/": self._root}
