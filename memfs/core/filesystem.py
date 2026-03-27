"""
Core filesystem implementation for MemFS.
Main entry point for file system operations.
"""

import os
import threading
import time
import tempfile
import shutil
from typing import Any, BinaryIO, Dict, List, Optional, Union
from pathlib import Path

from ..core.file import VirtualFile
from ..core.directory import DirectoryManager
from ..storage.hybrid import HybridStorage, ExternalModificationError
from ..utils.logger import OperationLogger, OperationType
from ..utils.stats import Statistics


class MemFileSystem:
    """
    Main file system class for MemFS.

    Provides both high-level and low-level file operations
    with automatic memory/real-path tiering.
    """

    def __init__(
        self,
        memory_limit: float = 0.8,
        persist_path: str = "./memfs_data",
        persist_mode: bool = False,
        temp_mode: bool = True,
        compress_memory: bool = True,
        worker_threads: int = 4,
        enable_logging: bool = True,
        log_path: Optional[str] = None,
        priority_boost_threshold: int = 10,
    ):
        """
        Initialize MemFileSystem.

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
        """
        self._lock = threading.Lock()
        self._closed = False
        self._persist_mode = persist_mode
        self._temp_dir: Optional[Path] = None

        if persist_mode:
            real_root = Path(persist_path).expanduser().resolve()
        else:
            if temp_mode:
                self._temp_dir = Path(tempfile.mkdtemp(prefix="memfs_"))
                real_root = self._temp_dir
            else:
                real_root = Path(persist_path).expanduser().resolve()

        self._persist_path = str(real_root)

        self.storage = HybridStorage(
            memory_limit=memory_limit,
            persist_path=str(real_root),
            persist_mode=persist_mode,
            temp_mode=temp_mode and not persist_mode,
            compress_memory=compress_memory,
            worker_threads=worker_threads,
        )

        self.directories = DirectoryManager()

        self.priority_boost_threshold = priority_boost_threshold
        self._file_priorities: Dict[str, int] = {}

        if enable_logging:
            self.logger = OperationLogger(log_path=log_path)
        else:
            self.logger = None

        self._access_counts: Dict[str, int] = {}

    def open(self, path: str, mode: str = "rb", priority: int = 5) -> VirtualFile:
        """
        Open a file.

        Args:
            path: File path (e.g., '虚拟/data.txt').
            mode: File mode ('r', 'w', 'a', 'rb', 'wb', 'ab').
            priority: File priority (0-10).

        Returns:
            VirtualFile instance.
        """
        if self._closed:
            raise RuntimeError("File system is closed")

        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        directory, filename = self.directories.resolve_path(path)

        data = b""

        if "r" in mode or "+" in mode:
            try:
                data = self.storage.get(path, priority=priority) or b""
            except ExternalModificationError:
                pass

        file = VirtualFile(key=path, data=data, mode=mode, filesystem=self)

        if "w" in mode or "a" in mode:
            self._set_priority_internal(path, priority)

        self._log_operation(
            OperationType.READ if "r" in mode else OperationType.WRITE,
            path,
        )

        return file

    def read(
        self,
        path: str,
        priority: Optional[int] = None,
        check_external: bool = True,
    ) -> bytes:
        """
        Read entire file.

        Args:
            path: File path.
            priority: Optional priority for access.
            check_external: If True, check for external modifications.

        Returns:
            File contents.

        Raises:
            FileNotFoundError: If file doesn't exist.
            ExternalModificationError: If file was modified externally.
        """
        if self._closed:
            raise RuntimeError("File system is closed")

        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        start_time = time.time()

        data = self.storage.get(path, priority=priority, check_external=check_external)

        if data is None:
            raise FileNotFoundError(f"File not found: {path}")

        duration_ms = (time.time() - start_time) * 1000

        if self.logger:
            self.logger.log(
                OperationType.READ,
                path,
                size=len(data),
                duration_ms=duration_ms,
            )

        self._track_access(path)

        return data

    def write(self, path: str, data: Union[bytes, str], priority: int = 5) -> int:
        """
        Write entire file.

        Args:
            path: File path.
            data: Data to write.
            priority: File priority.

        Returns:
            Number of bytes written.
        """
        if self._closed:
            raise RuntimeError("File system is closed")

        if isinstance(data, str):
            data = data.encode("utf-8")

        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        start_time = time.time()

        self.directories.get_or_create_directory(os.path.dirname(path))

        success = self.storage.put(path, data, priority)

        self._set_priority_internal(path, priority)

        duration_ms = (time.time() - start_time) * 1000

        if self.logger:
            self.logger.log(
                OperationType.WRITE,
                path,
                size=len(data),
                priority=priority,
                duration_ms=duration_ms,
                success=success,
            )

        return len(data)

    def delete(self, path: str) -> bool:
        """
        Delete a file.

        Args:
            path: File path.

        Returns:
            True if deleted.
        """
        if self._closed:
            raise RuntimeError("File system is closed")

        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        result = self.storage.remove(path)

        if self.logger:
            self.logger.log(OperationType.DELETE, path, success=result)

        if result:
            directory, filename = self.directories.resolve_path(path)
            if directory:
                directory.remove_file(filename)

        return result

    def exists(self, path: str) -> bool:
        """
        Check if file exists.

        Args:
            path: File path.

        Returns:
            True if exists.
        """
        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        return self.storage.contains(path)

    def mkdir(self, path: str) -> bool:
        """
        Create directory.

        Args:
            path: Directory path.

        Returns:
            True if created.
        """
        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        result = self.directories.mkdir(path)

        if self.logger:
            self.logger.log(OperationType.MKDIR, path)

        return result

    def rmdir(self, path: str) -> bool:
        """
        Remove directory.

        Args:
            path: Directory path.

        Returns:
            True if removed.
        """
        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        result = self.directories.rmdir(path)

        if self.logger:
            self.logger.log(OperationType.RMDIR, path, success=result)

        return result

    def listdir(self, path: str = "虚拟/") -> List[str]:
        """
        List directory contents.

        Args:
            path: Directory path.

        Returns:
            List of names.
        """
        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        return self.directories.listdir(path)

    def glob(self, pattern: str) -> List[str]:
        """
        Match paths using glob pattern.

        Args:
            pattern: Glob pattern.

        Returns:
            List of matching paths.
        """
        if not pattern.startswith("虚拟/"):
            pattern = "虚拟/" + pattern

        return self.directories.glob(pattern)

    def set_priority(self, path: str, priority: int) -> bool:
        """
        Set file priority.

        Args:
            path: File path.
            priority: Priority level (0-10).

        Returns:
            True if set.
        """
        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        result = self._set_priority_internal(path, priority)

        if self.logger:
            self.logger.log(
                OperationType.SET_PRIORITY,
                path,
                priority=priority,
                success=result,
            )

        return result

    def _set_priority_internal(self, path: str, priority: int) -> bool:
        """Internal priority setter."""
        with self._lock:
            if not self.storage.contains(path):
                return False

            self._file_priorities[path] = priority
            return self.storage.set_priority(path, priority)

    def get_priority(self, path: str) -> Optional[int]:
        """
        Get file priority.

        Args:
            path: File path.

        Returns:
            Priority or None.
        """
        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        with self._lock:
            return self._file_priorities.get(path)

    def preload(self, path: str, priority: int = 5) -> str:
        """
        Preload file into memory.

        Args:
            path: File path.
            priority: File priority.

        Returns:
            Task ID.
        """
        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        if self.logger:
            self.logger.log(OperationType.PRELOAD, path, priority=priority)

        return self.storage.preload(path, priority)

    def gc(self, target_usage: float = 0.5) -> int:
        """
        Trigger garbage collection.

        Args:
            target_usage: Target memory usage (0-1).

        Returns:
            Number of files swapped out.
        """
        if self.logger:
            self.logger.log(
                OperationType.GC,
                "system",
                metadata={"target_usage": target_usage},
            )

        return self.storage.gc(target_usage)

    def get_stats(self) -> dict:
        """
        Get file system statistics.

        Returns:
            Statistics dictionary.
        """
        return self.storage.get_stats()

    def get_file_info(self, path: str) -> Optional[dict]:
        """
        Get file information.

        Args:
            path: File path.

        Returns:
            File info or None.
        """
        if not path.startswith("虚拟/"):
            path = "虚拟/" + path

        location = self.storage.get_file_location(path)

        if location == "unknown":
            return None

        info = {
            "path": path,
            "location": location,
            "priority": self.get_priority(path),
        }

        if location == "memory":
            mem_info = self.storage.memory.get_file_info(path)
            if mem_info:
                info.update(mem_info)
        elif location == "real":
            real_info = self.storage.real_storage.get_file_info(path)
            if real_info:
                info.update(real_info)

        return info

    def _track_access(self, path: str):
        """Track file access for priority boosting."""
        with self._lock:
            count = self._access_counts.get(path, 0) + 1
            self._access_counts[path] = count

            if count >= self.priority_boost_threshold:
                current_priority = self._file_priorities.get(path, 5)
                if current_priority < 8:
                    new_priority = min(current_priority + 1, 8)
                    self._file_priorities[path] = new_priority
                    self.storage.set_priority(path, new_priority)

    def _log_operation(self, operation: OperationType, path: str, **kwargs):
        """Log an operation."""
        if self.logger:
            self.logger.log(operation, path, **kwargs)

    def clear(self):
        """Clear all files."""
        self.storage.clear()
        self.directories.clear()

        with self._lock:
            self._file_priorities.clear()
            self._access_counts.clear()

        if self.logger:
            self.logger.clear()

    def shutdown(self, wait: bool = True):
        """
        Shut down file system.

        Args:
            wait: Wait for pending operations.
        """
        self._closed = True
        self.storage.shutdown(wait=wait)

        if self._temp_dir and self._temp_dir.exists():
            try:
                shutil.rmtree(self._temp_dir)
            except Exception:
                pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()

    def __del__(self):
        """Destructor."""
        if not self._closed:
            self.shutdown(wait=False)
