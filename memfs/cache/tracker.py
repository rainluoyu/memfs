"""
Access tracker for MemFS.
Tracks file access patterns for cache eviction decisions.
"""

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class FileAccessRecord:
    """Record of file access patterns."""

    path: str
    access_count: int = 0
    last_access_time: float = field(default_factory=time.time)
    last_modified_time: float = field(default_factory=time.time)
    first_access_time: float = field(default_factory=time.time)
    read_count: int = 0
    write_count: int = 0
    last_size: int = 0

    def record_access(self, is_write: bool = False, size: int = 0):
        """Record an access event."""
        self.access_count += 1
        self.last_access_time = time.time()

        if is_write:
            self.write_count += 1
            self.last_modified_time = self.last_access_time
            self.last_size = size
        else:
            self.read_count += 1

        if size > 0:
            self.last_size = size

    @property
    def access_frequency(self) -> float:
        """Get access frequency (accesses per second since first access)."""
        elapsed = time.time() - self.first_access_time
        if elapsed <= 0:
            return 0.0
        return self.access_count / elapsed

    @property
    def recency_score(self) -> float:
        """Get recency score (higher is more recent)."""
        return 1.0 / (1.0 + (time.time() - self.last_access_time))

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "path": self.path,
            "access_count": self.access_count,
            "last_access_time": self.last_access_time,
            "last_modified_time": self.last_modified_time,
            "first_access_time": self.first_access_time,
            "read_count": self.read_count,
            "write_count": self.write_count,
            "last_size": self.last_size,
            "access_frequency": self.access_frequency,
            "recency_score": self.recency_score,
        }


class AccessTracker:
    """Tracks file access patterns for all files in the system."""

    def __init__(self):
        """Initialize access tracker."""
        self._lock = threading.Lock()
        self._records: Dict[str, FileAccessRecord] = {}

    def record_access(self, path: str, is_write: bool = False, size: int = 0):
        """
        Record a file access.

        Args:
            path: File path.
            is_write: True if write operation, False if read.
            size: File size in bytes.
        """
        with self._lock:
            if path not in self._records:
                self._records[path] = FileAccessRecord(path=path)

            self._records[path].record_access(is_write=is_write, size=size)

    def get_record(self, path: str) -> Optional[FileAccessRecord]:
        """
        Get access record for a file.

        Args:
            path: File path.

        Returns:
            FileAccessRecord or None if not found.
        """
        with self._lock:
            return self._records.get(path)

    def get_all_records(self) -> Dict[str, FileAccessRecord]:
        """Get all access records."""
        with self._lock:
            return self._records.copy()

    def remove_record(self, path: str):
        """Remove record for a file."""
        with self._lock:
            if path in self._records:
                del self._records[path]

    def get_hottest_files(self, limit: int = 10) -> list[FileAccessRecord]:
        """
        Get the most frequently accessed files.

        Args:
            limit: Maximum number of files to return.

        Returns:
            List of FileAccessRecord sorted by access count.
        """
        with self._lock:
            records = list(self._records.values())

        records.sort(key=lambda r: r.access_count, reverse=True)
        return records[:limit]

    def get_coldest_files(self, limit: int = 10) -> list[FileAccessRecord]:
        """
        Get the least frequently accessed files.

        Args:
            limit: Maximum number of files to return.

        Returns:
            List of FileAccessRecord sorted by access count.
        """
        with self._lock:
            records = list(self._records.values())

        records.sort(key=lambda r: r.access_count)
        return records[:limit]

    def get_stale_files(self, max_age_seconds: float) -> list[FileAccessRecord]:
        """
        Get files that haven't been accessed recently.

        Args:
            max_age_seconds: Maximum age in seconds.

        Returns:
            List of FileAccessRecord for stale files.
        """
        current_time = time.time()
        threshold = current_time - max_age_seconds

        with self._lock:
            stale = [
                r for r in self._records.values() if r.last_access_time < threshold
            ]

        stale.sort(key=lambda r: r.last_access_time)
        return stale

    def get_stats(self) -> dict:
        """Get tracker statistics."""
        with self._lock:
            total_files = len(self._records)
            total_accesses = sum(r.access_count for r in self._records.values())
            total_reads = sum(r.read_count for r in self._records.values())
            total_writes = sum(r.write_count for r in self._records.values())

        return {
            "total_files": total_files,
            "total_accesses": total_accesses,
            "total_reads": total_reads,
            "total_writes": total_writes,
        }

    def clear(self):
        """Clear all records."""
        with self._lock:
            self._records.clear()
