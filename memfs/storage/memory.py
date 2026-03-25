"""
Memory manager for MemFS.
Manages in-memory file storage and triggers eviction when needed.
"""

import io
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Callable
from collections import OrderedDict


@dataclass
class MemoryFile:
    """Represents a file stored in memory."""

    key: str
    data: bytes
    size: int
    priority: int = 5
    created_at: float = None
    last_accessed: float = None
    access_count: int = 0

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()
        if self.last_accessed is None:
            self.last_accessed = self.created_at

    def access(self):
        """Record an access."""
        self.last_accessed = time.time()
        self.access_count += 1

    def to_bytes(self) -> bytes:
        """Get file data as bytes."""
        return self.data


class MemoryManager:
    """
    Manages in-memory file storage.

    Tracks memory usage and triggers eviction when limit is reached.
    """

    def __init__(
        self,
        memory_limit: float = 0.8,
        on_eviction: Optional[Callable[[str], None]] = None,
    ):
        """
        Initialize memory manager.

        Args:
            memory_limit: Memory usage limit as fraction of total (0-1).
            on_eviction: Callback when eviction is needed.
                        Signature: (file_key) -> None
        """
        self.memory_limit = memory_limit
        self._on_eviction = on_eviction

        self._lock = threading.Lock()
        self._files: Dict[str, MemoryFile] = OrderedDict()
        self._current_usage = 0
        self._peak_usage = 0

        self._total_system_memory = self._get_system_memory()
        self._max_bytes = int(self._total_system_memory * memory_limit)

    @staticmethod
    def _get_system_memory() -> int:
        """Get total system memory in bytes."""
        try:
            import psutil

            return psutil.virtual_memory().total
        except ImportError:
            try:
                import os

                if hasattr(os, "sysconf"):
                    return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
            except Exception:
                pass

            import struct
            import ctypes

            try:
                kernel32 = ctypes.windll.kernel32
                c_ulonglong = ctypes.c_ulonglong

                class MEMORYSTATUSEX(ctypes.Structure):
                    _fields_ = [
                        ("dwLength", ctypes.c_ulong),
                        ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", c_ulonglong),
                        ("ullAvailPhys", c_ulonglong),
                        ("ullTotalPageFile", c_ulonglong),
                        ("ullAvailPageFile", c_ulonglong),
                        ("ullTotalVirtual", c_ulonglong),
                        ("ullAvailVirtual", c_ulonglong),
                        ("ullAvailExtendedVirtual", c_ulonglong),
                    ]

                status = MEMORYSTATUSEX()
                status.dwLength = ctypes.sizeof(status)
                kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
                return status.ullTotalPhys
            except Exception:
                return 8 * 1024 * 1024 * 1024

    def put(self, key: str, data: bytes, priority: int = 5) -> list[str]:
        """
        Store file in memory.

        Args:
            key: File key.
            data: File data.
            priority: File priority (0-10).

        Returns:
            List of evicted file keys.
        """
        evicted = []
        size = len(data)

        with self._lock:
            if key in self._files:
                old_file = self._files[key]
                self._current_usage -= old_file.size

            mem_file = MemoryFile(
                key=key,
                data=data,
                size=size,
                priority=priority,
            )

            self._files[key] = mem_file
            self._current_usage += size

            if self._current_usage > self._peak_usage:
                self._peak_usage = self._current_usage

            while self._current_usage > self._max_bytes:
                victim = self._select_victim()
                if not victim:
                    break

                evicted.append(victim)

                if victim in self._files:
                    del self._files[victim]
                    victim_size = size
                    self._current_usage -= victim_size

            if evicted and self._on_eviction:
                for victim_key in evicted:
                    try:
                        self._on_eviction(victim_key)
                    except Exception:
                        pass

        return evicted

    def get(self, key: str) -> Optional[bytes]:
        """
        Get file from memory.

        Args:
            key: File key.

        Returns:
            File data or None if not found.
        """
        with self._lock:
            if key not in self._files:
                return None

            file = self._files[key]
            file.access()

            self._files.move_to_end(key)

            return file.to_bytes()

    def contains(self, key: str) -> bool:
        """Check if file is in memory."""
        with self._lock:
            return key in self._files

    def remove(self, key: str) -> bool:
        """
        Remove file from memory.

        Args:
            key: File key.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            if key not in self._files:
                return False

            file = self._files[key]
            self._current_usage -= file.size
            del self._files[key]
            return True

    def update_priority(self, key: str, priority: int) -> bool:
        """
        Update file priority.

        Args:
            key: File key.
            priority: New priority (0-10).

        Returns:
            True if updated, False if not found.
        """
        with self._lock:
            if key not in self._files:
                return False

            self._files[key].priority = priority
            return True

    def get_file_info(self, key: str) -> Optional[dict]:
        """
        Get file information.

        Args:
            key: File key.

        Returns:
            File info dict or None.
        """
        with self._lock:
            if key not in self._files:
                return None

            file = self._files[key]
            return {
                "key": file.key,
                "size": file.size,
                "priority": file.priority,
                "created_at": file.created_at,
                "last_accessed": file.last_accessed,
                "access_count": file.access_count,
            }

    def _select_victim(self) -> Optional[str]:
        """Select a file for eviction (must hold lock)."""
        if not self._files:
            return None

        candidates = []
        for key, file in self._files.items():
            if file.priority >= 9:
                continue

            score = self._eviction_score(file)
            candidates.append((key, score))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[1])
        return candidates[0][0]

    @staticmethod
    def _eviction_score(file: MemoryFile) -> float:
        """Calculate eviction score (lower = evict first)."""
        priority_weight = {
            0: 0.1,
            1: 0.2,
            2: 0.3,
            3: 0.5,
            4: 0.7,
            5: 1.0,
            6: 1.5,
            7: 2.0,
            8: 3.0,
            9: 5.0,
            10: 10.0,
        }.get(file.priority, 1.0)

        recency = time.time() - file.last_accessed
        freq_factor = 1.0 / (file.access_count + 1)

        score = (1.0 / priority_weight) * freq_factor + (recency / 1000)

        return score

    def set_memory_limit(self, limit: float):
        """
        Set new memory limit.

        Args:
            limit: New limit as fraction of total (0-1).
        """
        self.memory_limit = limit
        self._max_bytes = int(self._total_system_memory * limit)

    def get_usage(self) -> dict:
        """Get memory usage statistics."""
        with self._lock:
            return {
                "current_usage": self._current_usage,
                "current_usage_mb": self._current_usage / (1024 * 1024),
                "peak_usage": self._peak_usage,
                "peak_usage_mb": self._peak_usage / (1024 * 1024),
                "max_bytes": self._max_bytes,
                "max_bytes_mb": self._max_bytes / (1024 * 1024),
                "usage_percent": (self._current_usage / self._max_bytes * 100)
                if self._max_bytes > 0
                else 0,
                "file_count": len(self._files),
                "memory_limit": self.memory_limit,
            }

    def clear(self):
        """Clear all files from memory."""
        with self._lock:
            self._files.clear()
            self._current_usage = 0

    def get_all_keys(self) -> list[str]:
        """Get all file keys in memory."""
        with self._lock:
            return list(self._files.keys())
