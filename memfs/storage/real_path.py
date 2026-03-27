"""
Real path storage for MemFS.
Manages 1:1 mapping between virtual paths and real filesystem paths.
"""

import os
import shutil
import threading
import atexit
from pathlib import Path
from typing import Optional, Dict, Tuple
from datetime import datetime


class RealPathStorage:
    """
    Real path storage manager.

    Provides 1:1 mapping between virtual paths and real filesystem paths.
    Supports both persistent mode and temporary mode.
    """

    def __init__(
        self,
        real_root: str,
        temp_mode: bool = True,
    ):
        """
        Initialize real path storage.

        Args:
            real_root: Root directory for real files.
            temp_mode: If True, cleanup directory on shutdown.
        """
        self.real_root = Path(real_root).expanduser().resolve()
        self.temp_mode = temp_mode

        self._lock = threading.Lock()
        self._file_info: Dict[str, dict] = {}  # Cache file info (mtime, size)

        if temp_mode:
            self._ensure_directory()
            self._register_cleanup()
        else:
            self._ensure_directory()
            self._load_file_info()

    def _ensure_directory(self):
        """Ensure root directory exists."""
        self.real_root.mkdir(parents=True, exist_ok=True)

    def _register_cleanup(self):
        """Register cleanup on program exit."""
        atexit.register(self._cleanup)

    def _cleanup(self):
        """Cleanup temporary directory."""
        if self.temp_mode and self.real_root.exists():
            try:
                shutil.rmtree(self.real_root)
            except Exception:
                pass

    def _load_file_info(self):
        """Load file info from disk for persistent mode."""
        if not self.real_root.exists():
            return

        for file_path in self.real_root.rglob("*"):
            if file_path.is_file():
                try:
                    stat = file_path.stat()
                    virtual_path = self._real_to_virtual(file_path)
                    self._file_info[virtual_path] = {
                        "mtime": stat.st_mtime,
                        "size": stat.st_size,
                    }
                except (OSError, IOError):
                    continue

    def _virtual_to_real(self, virtual_path: str) -> Path:
        """
        Convert virtual path to real path.

        Args:
            virtual_path: Virtual path (e.g., "/test.txt")

        Returns:
            Real filesystem path.
        """
        if virtual_path.startswith("/"):
            relative_path = virtual_path[3:]
        else:
            relative_path = virtual_path

        return self.real_root / relative_path

    def _real_to_virtual(self, real_path: Path) -> str:
        """
        Convert real path to virtual path.

        Args:
            real_path: Real filesystem path.

        Returns:
            Virtual path.
        """
        try:
            relative_path = real_path.relative_to(self.real_root)
            return "/" + str(relative_path).replace("\\", "/")
        except ValueError:
            return str(real_path)

    def get_real_path(self, virtual_path: str) -> Path:
        """
        Get real path for virtual path.

        Args:
            virtual_path: Virtual path.

        Returns:
            Real filesystem path.
        """
        return self._virtual_to_real(virtual_path)

    def write_sync(self, virtual_path: str, data: bytes) -> bool:
        """
        Synchronously write data to real path.

        Args:
            virtual_path: Virtual path.
            data: Data to write.

        Returns:
            True if successful.
        """
        try:
            real_path = self._virtual_to_real(virtual_path)

            with self._lock:
                real_path.parent.mkdir(parents=True, exist_ok=True)

                with open(real_path, "wb") as f:
                    f.write(data)

                stat = real_path.stat()
                self._file_info[virtual_path] = {
                    "mtime": stat.st_mtime,
                    "size": stat.st_size,
                }

                return True

        except (OSError, IOError):
            return False

    def read_sync(self, virtual_path: str) -> Optional[bytes]:
        """
        Synchronously read data from real path.

        Args:
            virtual_path: Virtual path.

        Returns:
            File data or None if not found.
        """
        try:
            real_path = self._virtual_to_real(virtual_path)

            if not real_path.exists():
                return None

            with open(real_path, "rb") as f:
                data = f.read()

            with self._lock:
                stat = real_path.stat()
                self._file_info[virtual_path] = {
                    "mtime": stat.st_mtime,
                    "size": stat.st_size,
                }

            return data

        except (OSError, IOError):
            return None

    def delete_sync(self, virtual_path: str) -> bool:
        """
        Synchronously delete file from real path.

        Args:
            virtual_path: Virtual path.

        Returns:
            True if deleted.
        """
        try:
            real_path = self._virtual_to_real(virtual_path)

            with self._lock:
                if real_path.exists():
                    real_path.unlink()

                    if virtual_path in self._file_info:
                        del self._file_info[virtual_path]

                    self._cleanup_empty_parents(real_path.parent)
                    return True

                return False

        except (OSError, IOError):
            return False

    def _cleanup_empty_parents(self, directory: Path):
        """Remove empty parent directories."""
        try:
            while directory != self.real_root:
                if directory.exists() and not any(directory.iterdir()):
                    directory.rmdir()
                    directory = directory.parent
                else:
                    break
        except (OSError, IOError):
            pass

    def exists(self, virtual_path: str) -> bool:
        """
        Check if file exists on real path.

        Args:
            virtual_path: Virtual path.

        Returns:
            True if exists.
        """
        real_path = self._virtual_to_real(virtual_path)
        return real_path.exists()

    def get_file_info(self, virtual_path: str) -> Optional[dict]:
        """
        Get file info (mtime, size).

        Args:
            virtual_path: Virtual path.

        Returns:
            File info dict or None.
        """
        with self._lock:
            return self._file_info.get(virtual_path)

    def check_external_modified(
        self,
        virtual_path: str,
        cached_mtime: float,
        cached_size: int,
    ) -> bool:
        """
        Check if file was modified externally.

        Args:
            virtual_path: Virtual path.
            cached_mtime: Cached modification time.
            cached_size: Cached file size.

        Returns:
            True if externally modified.
        """
        try:
            real_path = self._virtual_to_real(virtual_path)

            if not real_path.exists():
                return False

            stat = real_path.stat()

            return stat.st_mtime != cached_mtime or stat.st_size != cached_size

        except (OSError, IOError):
            return False

    def reload_if_modified(
        self,
        virtual_path: str,
        cached_mtime: float,
        cached_size: int,
    ) -> Tuple[bool, Optional[bytes]]:
        """
        Reload file if externally modified.

        Args:
            virtual_path: Virtual path.
            cached_mtime: Cached modification time.
            cached_size: Cached file size.

        Returns:
            (modified, new_data) - modified indicates if file changed,
                                   new_data is the reloaded data or None.
        """
        try:
            real_path = self._virtual_to_real(virtual_path)

            if not real_path.exists():
                return False, None

            stat = real_path.stat()

            if stat.st_mtime == cached_mtime and stat.st_size == cached_size:
                return False, None

            with open(real_path, "rb") as f:
                new_data = f.read()

            with self._lock:
                self._file_info[virtual_path] = {
                    "mtime": stat.st_mtime,
                    "size": stat.st_size,
                }

            return True, new_data

        except (OSError, IOError):
            return False, None

    def update_file_info(self, virtual_path: str, mtime: float, size: int):
        """
        Update cached file info.

        Args:
            virtual_path: Virtual path.
            mtime: Modification time.
            size: File size.
        """
        with self._lock:
            self._file_info[virtual_path] = {
                "mtime": mtime,
                "size": size,
            }

    def clear_file_info(self, virtual_path: str):
        """
        Clear cached file info.

        Args:
            virtual_path: Virtual path.
        """
        with self._lock:
            if virtual_path in self._file_info:
                del self._file_info[virtual_path]

    def get_all_files(self) -> list[str]:
        """
        Get all files in real root.

        Returns:
            List of virtual paths.
        """
        files = []

        if not self.real_root.exists():
            return files

        for file_path in self.real_root.rglob("*"):
            if file_path.is_file():
                files.append(self._real_to_virtual(file_path))

        return files

    def shutdown(self):
        """Shutdown storage."""
        pass

    def clear(self):
        """Clear all files."""
        with self._lock:
            if self.real_root.exists():
                for file_path in self.real_root.rglob("*"):
                    if file_path.is_file():
                        try:
                            file_path.unlink()
                        except (OSError, IOError):
                            pass

                self._cleanup_empty_parents(self.real_root)

            self._file_info.clear()
