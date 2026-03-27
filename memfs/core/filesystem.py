"""
Core filesystem implementation for MemFS.
Main entry point for file system operations.
"""

import os
import threading
import time
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
        storage_mode: str = "temp",
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
            storage_mode: Storage mode - "temp" (temporary, cleanup on shutdown) or "persist" (keep files after shutdown).
            worker_threads: Number of background worker threads.
            enable_logging: Whether to enable operation logging.
            log_path: Path for operation log file.
            priority_boost_threshold: Access count to boost priority.
        """
        self._lock = threading.Lock()
        self._closed = False
        self._storage_mode = storage_mode
        self._persist_mode = storage_mode == "persist"

        real_root = Path(persist_path).expanduser().resolve()
        self._persist_path = str(real_root)

        self.directories = DirectoryManager()

        self.storage = HybridStorage(
            memory_limit=memory_limit,
            persist_path=str(real_root),
            storage_mode=storage_mode,
            worker_threads=worker_threads,
            directory_manager=self.directories,
        )

        self.priority_boost_threshold = priority_boost_threshold
        self._file_priorities: Dict[str, int] = {}

        if enable_logging:
            self.logger = OperationLogger(log_path=log_path)
        else:
            self.logger = None

        self._access_counts: Dict[str, int] = {}

    @staticmethod
    def _normalize_path(path: str) -> str:
        """
        Normalize path to use forward slashes (/).

        Converts Windows-style backslashes to forward slashes for internal consistency.

        Args:
            path: Input path (may contain \ or /).

        Returns:
            Normalized path with forward slashes only.
        """
        return path.replace("\\", "/")

    def open(self, path: str, mode: str = "rb", priority: int = 5) -> VirtualFile:
        """
        Open a file.

        Args:
            path: File path (e.g., '/data.txt' or '\\data.txt').
            mode: File mode ('r', 'w', 'a', 'rb', 'wb', 'ab').
            priority: File priority (0-10).

        Returns:
            VirtualFile instance.
        """
        if self._closed:
            raise RuntimeError("File system is closed")

        normalized_path = self._normalize_path(path)
        directory, filename = self.directories.resolve_path(normalized_path)

        data = b""

        if "r" in mode or "+" in mode:
            try:
                data = self.storage.get(normalized_path, priority=priority) or b""
            except ExternalModificationError:
                pass

        file = VirtualFile(key=normalized_path, data=data, mode=mode, filesystem=self)

        if "w" in mode or "a" in mode:
            self._set_priority_internal(normalized_path, priority)

        self._log_operation(
            OperationType.READ if "r" in mode else OperationType.WRITE,
            normalized_path,
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

        start_time = time.time()

        normalized_path = self._normalize_path(path)
        data = self.storage.get(
            normalized_path, priority=priority, check_external=check_external
        )

        if data is None:
            raise FileNotFoundError(f"File not found: {path}")

        duration_ms = (time.time() - start_time) * 1000

        if self.logger:
            self.logger.log(
                OperationType.READ,
                normalized_path,
                size=len(data),
                duration_ms=duration_ms,
            )

        self._track_access(normalized_path)

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

        start_time = time.time()

        normalized_path = self._normalize_path(path)
        directory = self.directories.get_or_create_directory(
            os.path.dirname(normalized_path)
        )
        directory, filename = self.directories.resolve_path(normalized_path)
        directory.add_file(filename)

        success = self.storage.put(normalized_path, data, priority)

        self._set_priority_internal(normalized_path, priority)

        duration_ms = (time.time() - start_time) * 1000

        if self.logger:
            self.logger.log(
                OperationType.WRITE,
                normalized_path,
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

        normalized_path = self._normalize_path(path)
        result = self.storage.remove(normalized_path)

        if self.logger:
            self.logger.log(OperationType.DELETE, normalized_path, success=result)

        if result:
            directory, filename = self.directories.resolve_path(normalized_path)
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

        normalized_path = self._normalize_path(path)
        return self.storage.contains(normalized_path)

    def mkdir(self, path: str) -> bool:
        """
        Create directory.

        Args:
            path: Directory path.

        Returns:
            True if created.
        """
        normalized_path = self._normalize_path(path)
        result = self.directories.mkdir(normalized_path)

        if self.logger:
            self.logger.log(OperationType.MKDIR, normalized_path)

        return result

    def rmdir(self, path: str) -> bool:
        """
        Remove directory.

        Args:
            path: Directory path.

        Returns:
            True if removed.
        """
        normalized_path = self._normalize_path(path)
        result = self.directories.rmdir(normalized_path)

        if self.logger:
            self.logger.log(OperationType.RMDIR, normalized_path, success=result)

        return result

    def listdir(self, path: str = "/") -> List[str]:
        """
        List directory contents.

        Args:
            path: Directory path.

        Returns:
            List of names.
        """
        normalized_path = self._normalize_path(path)
        return self.directories.listdir(normalized_path)

    def glob(self, pattern: str) -> List[str]:
        """
        Match paths using glob pattern.

        Args:
            pattern: Glob pattern.

        Returns:
            List of matching paths.
        """
        # Normalize pattern - ensure it starts with / and uses /
        normalized_pattern = self._normalize_path(pattern)
        if not normalized_pattern.startswith("/"):
            normalized_pattern = "/" + normalized_pattern

        return self.directories.glob(normalized_pattern)

    def set_priority(self, path: str, priority: int) -> bool:
        """
        Set file priority.

        Args:
            path: File path.
            priority: Priority level (0-10).

        Returns:
            True if set.
        """
        normalized_path = self._normalize_path(path)
        result = self._set_priority_internal(normalized_path, priority)

        if self.logger:
            self.logger.log(
                OperationType.SET_PRIORITY,
                normalized_path,
                priority=priority,
                success=result,
            )

        return result

    def _set_priority_internal(self, path: str, priority: int) -> bool:
        """Internal priority setter."""
        normalized_path = self._normalize_path(path)
        with self._lock:
            if not self.storage.contains(normalized_path):
                return False

            self._file_priorities[normalized_path] = priority
            return self.storage.set_priority(normalized_path, priority)

    def get_priority(self, path: str) -> Optional[int]:
        """
        Get file priority.

        Args:
            path: File path.

        Returns:
            Priority or None.
        """
        normalized_path = self._normalize_path(path)
        with self._lock:
            return self._file_priorities.get(normalized_path)

    def preload(self, path: str, priority: int = 5) -> str:
        """
        Preload file into memory.

        Args:
            path: File path.
            priority: File priority.

        Returns:
            Task ID.
        """
        normalized_path = self._normalize_path(path)
        if self.logger:
            self.logger.log(OperationType.PRELOAD, normalized_path, priority=priority)

        return self.storage.preload(normalized_path, priority)

    def get_file_info(self, path: str) -> Optional[dict]:
        """
        Get file information.

        Args:
            path: File path.

        Returns:
            File info or None.
        """
        normalized_path = self._normalize_path(path)
        location = self.storage.get_file_location(normalized_path)

        if location == "unknown":
            return None

        info = {
            "path": path,
            "location": location,
            "priority": self.get_priority(normalized_path),
        }

        if location == "memory":
            mem_info = self.storage.memory.get_file_info(normalized_path)
            if mem_info:
                info.update(mem_info)
        elif location == "real":
            real_info = self.storage.real_storage.get_file_info(normalized_path)
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
                  If False, returns immediately and operations complete in background.
        """
        self._closed = True
        pending = self.storage.shutdown(wait=wait)

        if not wait and pending > 0:
            # Background shutdown - tasks will complete asynchronously
            pass

        return pending

    def shutdown_async(self) -> int:
        """
        Initiate asynchronous shutdown.

        Returns immediately. Background tasks continue executing.

        Returns:
            Number of pending operations that will complete in background.
        """
        return self.shutdown(wait=False)

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
