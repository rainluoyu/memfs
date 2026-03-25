"""
Operation logger for MemFS.
Records all file operations for auditing and debugging.
"""

import logging
import os
import threading
from datetime import datetime
from enum import Enum
from typing import Optional


class OperationType(Enum):
    """Types of file operations."""

    READ = "READ"
    WRITE = "WRITE"
    DELETE = "DELETE"
    MKDIR = "MKDIR"
    RMDIR = "RMDIR"
    RENAME = "RENAME"
    SWAP_OUT = "SWAP_OUT"
    SWAP_IN = "SWAP_IN"
    GC = "GC"
    PRELOAD = "PRELOAD"
    SET_PRIORITY = "SET_PRIORITY"


class LogEntry:
    """Represents a single log entry."""

    def __init__(
        self,
        operation: OperationType,
        path: str,
        timestamp: datetime,
        size: Optional[int] = None,
        priority: Optional[int] = None,
        duration_ms: Optional[float] = None,
        success: bool = True,
        error: Optional[str] = None,
        metadata: Optional[dict] = None,
    ):
        self.operation = operation
        self.path = path
        self.timestamp = timestamp
        self.size = size
        self.priority = priority
        self.duration_ms = duration_ms
        self.success = success
        self.error = error
        self.metadata = metadata or {}

    def to_dict(self) -> dict:
        """Convert entry to dictionary."""
        return {
            "operation": self.operation.value,
            "path": self.path,
            "timestamp": self.timestamp.isoformat(),
            "size": self.size,
            "priority": self.priority,
            "duration_ms": self.duration_ms,
            "success": self.success,
            "error": self.error,
            "metadata": self.metadata,
        }

    def __str__(self) -> str:
        status = "OK" if self.success else f"FAILED: {self.error}"
        return (
            f"[{self.timestamp.isoformat()}] {self.operation.value} "
            f"'{self.path}' - {status}"
        )


class OperationLogger:
    """Logger for file system operations."""

    def __init__(
        self,
        log_path: Optional[str] = None,
        enable_file_logging: bool = True,
        level: int = logging.INFO,
        max_entries: int = 10000,
    ):
        """
        Initialize operation logger.

        Args:
            log_path: Path to log file. If None, uses memory only.
            enable_file_logging: Whether to write logs to file.
            level: Logging level.
            max_entries: Maximum entries to keep in memory.
        """
        self.enable_file_logging = enable_file_logging
        self.max_entries = max_entries
        self._lock = threading.Lock()
        self._entries: list[LogEntry] = []
        self._logger: Optional[logging.Logger] = None

        if enable_file_logging and log_path:
            self._setup_file_logger(log_path, level)

    def _setup_file_logger(self, log_path: str, level: int):
        """Set up file-based logging."""
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)

        self._logger = logging.getLogger("memfs_operations")
        self._logger.setLevel(level)
        self._logger.handlers.clear()

        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
        )
        self._logger.addHandler(handler)

    def log(
        self,
        operation: OperationType,
        path: str,
        size: Optional[int] = None,
        priority: Optional[int] = None,
        duration_ms: Optional[float] = None,
        success: bool = True,
        error: Optional[str] = None,
        metadata: Optional[dict] = None,
    ):
        """
        Log an operation.

        Args:
            operation: Type of operation.
            path: File/directory path.
            size: Size in bytes (if applicable).
            priority: File priority (if applicable).
            duration_ms: Operation duration in milliseconds.
            success: Whether operation succeeded.
            error: Error message if failed.
            metadata: Additional metadata.
        """
        entry = LogEntry(
            operation=operation,
            path=path,
            timestamp=datetime.now(),
            size=size,
            priority=priority,
            duration_ms=duration_ms,
            success=success,
            error=error,
            metadata=metadata,
        )

        with self._lock:
            self._entries.append(entry)

            if len(self._entries) > self.max_entries:
                self._entries = self._entries[-self.max_entries :]

        if self._logger:
            log_msg = str(entry)
            if success:
                self._logger.info(log_msg)
            else:
                self._logger.error(log_msg)

    def get_entries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        operation_type: Optional[OperationType] = None,
        path_pattern: Optional[str] = None,
        limit: int = 100,
    ) -> list[LogEntry]:
        """
        Query log entries with filters.

        Args:
            start_time: Filter entries after this time.
            end_time: Filter entries before this time.
            operation_type: Filter by operation type.
            path_pattern: Filter by path pattern (substring match).
            limit: Maximum entries to return.

        Returns:
            List of matching log entries.
        """
        with self._lock:
            entries = self._entries.copy()

        if start_time:
            entries = [e for e in entries if e.timestamp >= start_time]

        if end_time:
            entries = [e for e in entries if e.timestamp <= end_time]

        if operation_type:
            entries = [e for e in entries if e.operation == operation_type]

        if path_pattern:
            entries = [e for e in entries if path_pattern in e.path]

        return entries[-limit:]

    def get_stats(self) -> dict:
        """Get logging statistics."""
        with self._lock:
            total = len(self._entries)
            success = sum(1 for e in self._entries if e.success)
            failed = total - success

            by_operation = {}
            for op in OperationType:
                count = sum(1 for e in self._entries if e.operation == op)
                if count > 0:
                    by_operation[op.value] = count

        return {
            "total_entries": total,
            "successful": success,
            "failed": failed,
            "by_operation": by_operation,
        }

    def clear(self):
        """Clear all in-memory log entries."""
        with self._lock:
            self._entries.clear()

    def export_json(self, filepath: str):
        """Export logs to JSON file."""
        import json

        with self._lock:
            entries = [e.to_dict() for e in self._entries]

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2)
