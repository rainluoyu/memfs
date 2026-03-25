"""
Disk storage for MemFS.
Handles persistent storage of files on disk.
"""

import os
import shutil
import threading
import hashlib
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime


class DiskStorage:
    """
    Disk-based persistent storage.

    Stores files in a configurable directory with metadata.
    """

    def __init__(
        self,
        storage_path: str,
        create: bool = True,
        compression: str = "gzip",
        compression_level: int = 6,
    ):
        """
        Initialize disk storage.

        Args:
            storage_path: Base path for storage.
            create: Whether to create directory if not exists.
            compression: Compression algorithm ('none', 'gzip', 'lz4', 'zstd').
            compression_level: Compression level (1-9).
        """
        self.storage_path = Path(storage_path).expanduser().resolve()
        self.compression = compression
        self.compression_level = compression_level

        self._lock = threading.Lock()
        self._metadata: Dict[str, dict] = {}

        if create:
            self._ensure_directory()

        self._load_metadata()

    def _ensure_directory(self):
        """Ensure storage directory exists."""
        self.storage_path.mkdir(parents=True, exist_ok=True)
        (self.storage_path / "data").mkdir(exist_ok=True)
        (self.storage_path / "meta").mkdir(exist_ok=True)

    def _get_data_path(self, key: str) -> Path:
        """Get path for data file."""
        file_hash = hashlib.md5(key.encode()).hexdigest()
        return self.storage_path / "data" / f"{file_hash}.dat"

    def _get_meta_path(self, key: str) -> Path:
        """Get path for metadata file."""
        file_hash = hashlib.md5(key.encode()).hexdigest()
        return self.storage_path / "meta" / f"{file_hash}.meta"

    def _load_metadata(self):
        """Load metadata from disk."""
        meta_dir = self.storage_path / "meta"

        if not meta_dir.exists():
            return

        import json

        for meta_file in meta_dir.glob("*.meta"):
            try:
                with open(meta_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                    key = metadata.get("key")
                    if key:
                        self._metadata[key] = metadata
            except Exception:
                continue

    def _save_metadata(self, key: str, metadata: dict):
        """Save metadata to disk."""
        import json

        meta_path = self._get_meta_path(key)

        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

    def _remove_metadata(self, key: str):
        """Remove metadata file."""
        meta_path = self._get_meta_path(key)

        if meta_path.exists():
            meta_path.unlink()

    def put(
        self,
        key: str,
        data: bytes,
        priority: int = 5,
        compressed: Optional[bool] = None,
    ) -> int:
        """
        Store file on disk.

        Args:
            key: File key.
            data: File data.
            priority: File priority.
            compressed: Whether to compress (None = use default).

        Returns:
            Size in bytes.
        """
        with self._lock:
            should_compress = (
                compressed if compressed is not None else self.compression != "none"
            )

            stored_data = data

            if should_compress and len(data) > 0:
                from ..utils.compress import CompressionFactory

                try:
                    compressor = CompressionFactory.create(
                        self.compression, self.compression_level
                    )
                    stored_data = compressor.compress(data)
                except Exception:
                    stored_data = data
                    should_compress = False

            data_path = self._get_data_path(key)

            with open(data_path, "wb") as f:
                f.write(stored_data)

            metadata = {
                "key": key,
                "original_size": len(data),
                "stored_size": len(stored_data),
                "compressed": should_compress,
                "priority": priority,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }

            self._save_metadata(key, metadata)
            self._metadata[key] = metadata

            return len(stored_data)

    def get(self, key: str) -> Optional[bytes]:
        """
        Retrieve file from disk.

        Args:
            key: File key.

        Returns:
            File data or None if not found.
        """
        with self._lock:
            if key not in self._metadata:
                return None

            data_path = self._get_data_path(key)

            if not data_path.exists():
                return None

            with open(data_path, "rb") as f:
                stored_data = f.read()

            metadata = self._metadata[key]

            if metadata.get("compressed", False):
                from ..utils.compress import CompressionFactory

                try:
                    compressor = CompressionFactory.create(self.compression)
                    data = compressor.decompress(stored_data)
                except Exception:
                    return None
            else:
                data = stored_data

            return data

    def contains(self, key: str) -> bool:
        """Check if file exists on disk."""
        with self._lock:
            if key not in self._metadata:
                return False

            data_path = self._get_data_path(key)
            return data_path.exists()

    def remove(self, key: str) -> bool:
        """
        Remove file from disk.

        Args:
            key: File key.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            if key not in self._metadata:
                return False

            data_path = self._get_data_path(key)
            meta_path = self._get_meta_path(key)

            if data_path.exists():
                data_path.unlink()

            if meta_path.exists():
                meta_path.unlink()

            del self._metadata[key]
            return True

    def update_priority(self, key: str, priority: int) -> bool:
        """
        Update file priority.

        Args:
            key: File key.
            priority: New priority.

        Returns:
            True if updated, False if not found.
        """
        with self._lock:
            if key not in self._metadata:
                return False

            self._metadata[key]["priority"] = priority
            self._metadata[key]["updated_at"] = datetime.now().isoformat()

            self._save_metadata(key, self._metadata[key])
            return True

    def get_metadata(self, key: str) -> Optional[dict]:
        """
        Get file metadata.

        Args:
            key: File key.

        Returns:
            Metadata dict or None.
        """
        with self._lock:
            return self._metadata.get(key)

    def get_usage(self) -> dict:
        """Get disk usage statistics."""
        with self._lock:
            total_size = sum(m.get("stored_size", 0) for m in self._metadata.values())

            data_dir = self.storage_path / "data"
            disk_total = 0

            if data_dir.exists():
                try:
                    import shutil

                    total, used, free = shutil.disk_usage(str(data_dir))
                    disk_total = total
                except Exception:
                    pass

            return {
                "path": str(self.storage_path),
                "file_count": len(self._metadata),
                "total_size": total_size,
                "total_size_mb": total_size / (1024 * 1024),
                "disk_total": disk_total,
                "disk_total_mb": disk_total / (1024 * 1024),
            }

    def get_all_keys(self) -> list[str]:
        """Get all file keys on disk."""
        with self._lock:
            return list(self._metadata.keys())

    def clear(self):
        """Clear all files from disk storage."""
        with self._lock:
            data_dir = self.storage_path / "data"
            meta_dir = self.storage_path / "meta"

            if data_dir.exists():
                shutil.rmtree(data_dir)

            if meta_dir.exists():
                shutil.rmtree(meta_dir)

            self._metadata.clear()
            self._ensure_directory()

    def get_storage_path(self) -> str:
        """Get storage path."""
        return str(self.storage_path)
