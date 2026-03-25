"""
Statistics module for MemFS.
Provides usage statistics for memory, disk, and file operations.
"""

import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MemoryStats:
    """Memory usage statistics."""

    current_usage: int = 0
    peak_usage: int = 0
    limit: int = 0
    file_count: int = 0
    total_size: int = 0

    @property
    def usage_percent(self) -> float:
        """Get current usage as percentage of limit."""
        if self.limit == 0:
            return 0.0
        return (self.current_usage / self.limit) * 100

    def to_dict(self) -> dict:
        return {
            "current_usage": self.current_usage,
            "current_usage_mb": self.current_usage / (1024 * 1024),
            "peak_usage": self.peak_usage,
            "peak_usage_mb": self.peak_usage / (1024 * 1024),
            "limit": self.limit,
            "limit_mb": self.limit / (1024 * 1024),
            "usage_percent": self.usage_percent,
            "file_count": self.file_count,
            "total_size": self.total_size,
            "total_size_mb": self.total_size / (1024 * 1024),
        }


@dataclass
class DiskStats:
    """Disk usage statistics."""

    path: str = ""
    current_usage: int = 0
    total_capacity: int = 0
    file_count: int = 0

    @property
    def usage_percent(self) -> float:
        """Get current usage as percentage of capacity."""
        if self.total_capacity == 0:
            return 0.0
        return (self.current_usage / self.total_capacity) * 100

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "current_usage": self.current_usage,
            "current_usage_mb": self.current_usage / (1024 * 1024),
            "total_capacity": self.total_capacity,
            "total_capacity_mb": self.total_capacity / (1024 * 1024),
            "usage_percent": self.usage_percent,
            "file_count": self.file_count,
        }


@dataclass
class CacheStats:
    """Cache performance statistics."""

    hits: int = 0
    misses: int = 0
    swaps_in: int = 0
    swaps_out: int = 0
    preloads: int = 0
    evictions: int = 0

    @property
    def hit_rate(self) -> float:
        """Get cache hit rate."""
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return (self.hits / total) * 100

    def to_dict(self) -> dict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hit_rate,
            "swaps_in": self.swaps_in,
            "swaps_out": self.swaps_out,
            "preloads": self.preloads,
            "evictions": self.evictions,
        }


@dataclass
class OperationStats:
    """Operation performance statistics."""

    total_operations: int = 0
    read_count: int = 0
    write_count: int = 0
    delete_count: int = 0
    avg_read_time_ms: float = 0.0
    avg_write_time_ms: float = 0.0

    _read_times: list[float] = field(default_factory=list)
    _write_times: list[float] = field(default_factory=list)

    def record_read(self, duration_ms: float):
        """Record a read operation."""
        self.read_count += 1
        self.total_operations += 1
        self._read_times.append(duration_ms)
        if len(self._read_times) > 1000:
            self._read_times = self._read_times[-1000:]
        self.avg_read_time_ms = sum(self._read_times) / len(self._read_times)

    def record_write(self, duration_ms: float):
        """Record a write operation."""
        self.write_count += 1
        self.total_operations += 1
        self._write_times.append(duration_ms)
        if len(self._write_times) > 1000:
            self._write_times = self._write_times[-1000:]
        self.avg_write_time_ms = sum(self._write_times) / len(self._write_times)

    def to_dict(self) -> dict:
        return {
            "total_operations": self.total_operations,
            "read_count": self.read_count,
            "write_count": self.write_count,
            "delete_count": self.delete_count,
            "avg_read_time_ms": self.avg_read_time_ms,
            "avg_write_time_ms": self.avg_write_time_ms,
        }


class Statistics:
    """Centralized statistics collection for MemFS."""

    def __init__(self):
        """Initialize statistics collector."""
        self._lock = threading.Lock()
        self.start_time = time.time()

        self.memory = MemoryStats()
        self.disk = DiskStats()
        self.cache = CacheStats()
        self.operations = OperationStats()

    def update_memory(
        self,
        current_usage: Optional[int] = None,
        file_count: Optional[int] = None,
        total_size: Optional[int] = None,
    ):
        """Update memory statistics."""
        with self._lock:
            if current_usage is not None:
                self.memory.current_usage = current_usage
                if current_usage > self.memory.peak_usage:
                    self.memory.peak_usage = current_usage

            if file_count is not None:
                self.memory.file_count = file_count

            if total_size is not None:
                self.memory.total_size = total_size

    def set_memory_limit(self, limit: int):
        """Set memory limit."""
        with self._lock:
            self.memory.limit = limit

    def update_disk(
        self,
        path: str,
        current_usage: Optional[int] = None,
        file_count: Optional[int] = None,
    ):
        """Update disk statistics."""
        with self._lock:
            self.disk.path = path

            if current_usage is not None:
                self.disk.current_usage = current_usage

            if file_count is not None:
                self.disk.file_count = file_count

            try:
                if os.path.exists(path):
                    stat = os.statvfs(path)
                    self.disk.total_capacity = stat.f_blocks * stat.f_frsize
            except (OSError, AttributeError):
                try:
                    import psutil

                    usage = psutil.disk_usage(path)
                    self.disk.total_capacity = usage.total
                except (ImportError, OSError):
                    pass

    def record_cache_hit(self):
        """Record a cache hit."""
        with self._lock:
            self.cache.hits += 1

    def record_cache_miss(self):
        """Record a cache miss."""
        with self._lock:
            self.cache.misses += 1

    def record_swap_in(self):
        """Record a swap-in operation."""
        with self._lock:
            self.cache.swaps_in += 1

    def record_swap_out(self):
        """Record a swap-out operation."""
        with self._lock:
            self.cache.swaps_out += 1
            self.cache.evictions += 1

    def record_preload(self):
        """Record a preload operation."""
        with self._lock:
            self.cache.preloads += 1

    def to_dict(self) -> dict:
        """Get all statistics as dictionary."""
        with self._lock:
            return {
                "uptime_seconds": time.time() - self.start_time,
                "memory": self.memory.to_dict(),
                "disk": self.disk.to_dict(),
                "cache": self.cache.to_dict(),
                "operations": self.operations.to_dict(),
            }

    def reset(self):
        """Reset all statistics."""
        with self._lock:
            self.start_time = time.time()
            self.memory = MemoryStats()
            self.disk = DiskStats()
            self.cache = CacheStats()
            self.operations = OperationStats()
