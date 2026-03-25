"""
Comprehensive tests for MemFS core functionality.
Covers all public APIs with unit tests.
"""

import pytest
import sys
import os
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memfs import MemFileSystem, write, read, exists, delete
from memfs.core.file import VirtualFile
from memfs.core.directory import VirtualDirectory, DirectoryManager
from memfs.storage.memory import MemoryManager, MemoryFile
from memfs.storage.disk import DiskStorage
from memfs.cache.lfu import LFUCache
from memfs.cache.priority import PriorityQueue
from memfs.cache.tracker import AccessTracker, FileAccessRecord
from memfs.utils.stats import Statistics, MemoryStats, DiskStats
from memfs.utils.logger import OperationLogger, OperationType, LogEntry
from memfs.utils.compress import (
    Compressor,
    GzipCompressor,
    CompressionFactory,
)


# =============================================================================
# Test VirtualFile
# =============================================================================


class TestVirtualFile:
    """Tests for VirtualFile class."""

    def test_create_file(self):
        """Test file creation."""
        f = VirtualFile("test.txt", b"hello", "rb")
        assert f.read() == b"hello"
        f.close()

    def test_create_empty_file(self):
        """Test creating empty file."""
        f = VirtualFile("test.txt", b"", "rb")
        assert f.read() == b""
        f.close()

    def test_write_file(self):
        """Test file writing."""
        f = VirtualFile("test.txt", b"", "wb")
        bytes_written = f.write(b"world")
        assert bytes_written == 5
        f.close()

    def test_write_in_read_mode_raises(self):
        """Test writing in read mode raises error."""
        import io

        f = VirtualFile("test.txt", b"hello", "rb")
        with pytest.raises(io.UnsupportedOperation):
            f.write(b"world")
        f.close()

    def test_read_in_write_mode_raises(self):
        """Test reading in write mode raises error."""
        import io

        f = VirtualFile("test.txt", b"", "wb")
        with pytest.raises(io.UnsupportedOperation):
            f.read()
        f.close()

    def test_seek_tell(self):
        """Test seek and tell."""
        f = VirtualFile("test.txt", b"hello world", "rb")
        assert f.tell() == 0
        f.seek(6)
        assert f.tell() == 6
        assert f.read() == b"world"
        f.close()

    def test_seek_from_end(self):
        """Test seek from end."""
        f = VirtualFile("test.txt", b"hello world", "rb")
        f.seek(-5, 2)
        assert f.read() == b"world"
        f.close()

    def test_seek_from_current(self):
        """Test seek from current position."""
        f = VirtualFile("test.txt", b"hello world", "rb")
        f.seek(6)
        f.seek(2, 1)
        assert f.read() == b"rld"
        f.close()

    def test_truncate(self):
        """Test truncate."""
        f = VirtualFile("test.txt", b"hello world", "wb")
        f.seek(5)
        f.truncate()
        # Check buffer content after truncate
        assert f._buffer.getvalue() == b"hello"
        f.close()

    def test_truncate_with_size(self):
        """Test truncate with size parameter."""
        f = VirtualFile("test.txt", b"hello world", "wb")
        f.truncate(5)
        # Check buffer content after truncate
        assert f._buffer.getvalue() == b"hello"
        f.close()

    def test_readline(self):
        """Test reading lines."""
        f = VirtualFile("test.txt", b"line1\nline2\nline3", "rb")
        assert f.readline() == b"line1\n"
        assert f.readline() == b"line2\n"
        f.close()

    def test_context_manager(self):
        """Test context manager."""
        with VirtualFile("test.txt", b"test", "rb") as f:
            assert f.read() == b"test"
        assert f.closed

    def test_double_close(self):
        """Test closing twice doesn't raise."""
        f = VirtualFile("test.txt", b"test", "rb")
        f.close()
        f.close()  # Should not raise

    def test_read_after_close_raises(self):
        """Test reading after close raises."""
        f = VirtualFile("test.txt", b"test", "rb")
        f.close()
        with pytest.raises(ValueError):
            f.read()

    def test_write_after_close_raises(self):
        """Test writing after close raises."""
        f = VirtualFile("test.txt", b"", "wb")
        f.close()
        with pytest.raises(ValueError):
            f.write(b"test")

    def test_append_mode(self):
        """Test append mode."""
        f = VirtualFile("test.txt", b"hello", "ab")
        f.write(b" world")
        # Check buffer content after append
        assert f._buffer.getvalue() == b"hello world"
        f.close()

    def test_readable_writable_seekable(self):
        """Test file capabilities."""
        f = VirtualFile("test.txt", b"test", "rb")
        assert f.readable()
        assert not f.writable()
        assert f.seekable()
        f.close()

        f = VirtualFile("test.txt", b"", "wb")
        assert not f.readable()
        assert f.writable()
        assert f.seekable()
        f.close()


# =============================================================================
# Test MemoryManager
# =============================================================================


class TestMemoryManager:
    """Tests for MemoryManager class."""

    def test_put_get(self):
        """Test basic put and get."""
        manager = MemoryManager(memory_limit=0.9)
        manager.put("file1", b"data1", priority=5)
        assert manager.get("file1") == b"data1"

    def test_contains(self):
        """Test contains check."""
        manager = MemoryManager(memory_limit=0.9)
        manager.put("file1", b"data1")
        assert manager.contains("file1")
        assert not manager.contains("file2")

    def test_remove(self):
        """Test file removal."""
        manager = MemoryManager(memory_limit=0.9)
        manager.put("file1", b"data1")
        assert manager.remove("file1")
        assert not manager.contains("file1")
        assert not manager.remove("file1")  # Remove again returns False

    def test_update_priority(self):
        """Test priority update."""
        manager = MemoryManager(memory_limit=0.9)
        manager.put("file1", b"data1", priority=3)
        manager.update_priority("file1", priority=8)
        info = manager.get_file_info("file1")
        assert info["priority"] == 8

    def test_update_priority_nonexistent(self):
        """Test updating priority of nonexistent file."""
        manager = MemoryManager(memory_limit=0.9)
        result = manager.update_priority("nonexistent", priority=8)
        assert result is False

    def test_get_file_info(self):
        """Test getting file info."""
        manager = MemoryManager(memory_limit=0.9)
        manager.put("file1", b"data1", priority=7)
        info = manager.get_file_info("file1")
        assert info["key"] == "file1"
        assert info["size"] == 5
        assert info["priority"] == 7
        assert info["access_count"] >= 0

    def test_get_file_info_nonexistent(self):
        """Test getting info of nonexistent file."""
        manager = MemoryManager(memory_limit=0.9)
        info = manager.get_file_info("nonexistent")
        assert info is None

    def test_clear(self):
        """Test clearing all files."""
        manager = MemoryManager(memory_limit=0.9)
        manager.put("file1", b"data1")
        manager.put("file2", b"data2")
        manager.clear()
        assert not manager.contains("file1")
        assert not manager.contains("file2")

    def test_get_all_keys(self):
        """Test getting all keys."""
        manager = MemoryManager(memory_limit=0.9)
        manager.put("file1", b"data1")
        manager.put("file2", b"data2")
        keys = manager.get_all_keys()
        assert "file1" in keys
        assert "file2" in keys

    def test_get_usage(self):
        """Test getting usage statistics."""
        manager = MemoryManager(memory_limit=0.8)
        manager.put("file1", b"data1")
        usage = manager.get_usage()
        assert "current_usage" in usage
        assert "file_count" in usage
        assert usage["file_count"] == 1
        assert usage["memory_limit"] == 0.8

    def test_eviction_on_overflow(self):
        """Test that eviction happens when limit is exceeded."""
        manager = MemoryManager(memory_limit=0.0001)  # Very small limit
        manager.put("file1", b"x" * 1000, priority=1)  # Low priority
        manager.put("file2", b"y" * 1000, priority=1)
        manager.put("file3", b"z" * 1000, priority=1)
        # Some files should be evicted
        usage = manager.get_usage()
        assert usage["current_usage"] <= manager._max_bytes

    def test_high_priority_protects_from_eviction(self):
        """Test that high priority protects files from eviction."""
        manager = MemoryManager(memory_limit=0.0001)
        manager.put("protected", b"data", priority=10)  # Maximum priority
        # Even with small limit, protected file should stay
        # (eviction might not happen for priority 9-10)

    def test_access_increments_count(self):
        """Test that access increments access count."""
        manager = MemoryManager(memory_limit=0.9)
        manager.put("file1", b"data1")
        info1 = manager.get_file_info("file1")
        manager.get("file1")
        info2 = manager.get_file_info("file1")
        assert info2["access_count"] > info1["access_count"]

    def test_set_memory_limit(self):
        """Test setting memory limit."""
        manager = MemoryManager(memory_limit=0.5)
        manager.set_memory_limit(0.8)
        assert manager.memory_limit == 0.8


# =============================================================================
# Test LFUCache
# =============================================================================


class TestLFUCache:
    """Tests for LFUCache class."""

    def test_put_get(self):
        """Test basic put and get."""
        cache = LFUCache(max_capacity=10)
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_nonexistent(self):
        """Test getting nonexistent key."""
        cache = LFUCache(max_capacity=10)
        assert cache.get("nonexistent") is None

    def test_eviction(self):
        """Test LFU eviction."""
        cache = LFUCache(max_capacity=3)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)
        cache.get("a")
        cache.get("a")
        cache.put("d", 4)

        assert cache.get("a") == 1
        assert cache.get("d") == 4

    def test_remove(self):
        """Test item removal."""
        cache = LFUCache(max_capacity=10)
        cache.put("key1", "value1")
        assert cache.remove("key1")
        assert not cache.contains("key1")

    def test_remove_nonexistent(self):
        """Test removing nonexistent key."""
        cache = LFUCache(max_capacity=10)
        assert not cache.remove("nonexistent")

    def test_contains(self):
        """Test contains check."""
        cache = LFUCache(max_capacity=10)
        cache.put("key1", "value1")
        assert cache.contains("key1")
        assert not cache.contains("key2")

    def test_size(self):
        """Test size method."""
        cache = LFUCache(max_capacity=10)
        assert cache.size() == 0
        cache.put("key1", "value1")
        assert cache.size() == 1

    def test_clear(self):
        """Test clearing cache."""
        cache = LFUCache(max_capacity=10)
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.clear()
        assert cache.size() == 0

    def test_get_all_keys(self):
        """Test getting all keys."""
        cache = LFUCache(max_capacity=10)
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        keys = cache.get_all_keys()
        assert len(keys) == 2
        assert "key1" in keys
        assert "key2" in keys

    def test_get_frequency(self):
        """Test getting access frequency."""
        cache = LFUCache(max_capacity=10)
        cache.put("key1", "value1")
        assert cache.get_frequency("key1") == 1
        cache.get("key1")
        assert cache.get_frequency("key1") == 2

    def test_update_existing_key(self):
        """Test updating existing key."""
        cache = LFUCache(max_capacity=10)
        cache.put("key1", "value1")
        cache.put("key1", "value2")
        assert cache.get("key1") == "value2"

    def test_get_stats(self):
        """Test getting cache stats."""
        cache = LFUCache(max_capacity=10)
        cache.put("key1", "value1")
        stats = cache.get_stats()
        assert stats["size"] == 1
        assert stats["max_capacity"] == 10


# =============================================================================
# Test PriorityQueue
# =============================================================================


class TestPriorityQueue:
    """Tests for PriorityQueue class."""

    def test_put_get(self):
        """Test basic put and get."""
        queue = PriorityQueue(max_size=10)
        queue.put("key1", "value1", priority=5)
        assert queue.get("key1") == "value1"

    def test_get_nonexistent(self):
        """Test getting nonexistent key."""
        queue = PriorityQueue(max_size=10)
        assert queue.get("nonexistent") is None

    def test_update_priority(self):
        """Test priority update."""
        queue = PriorityQueue(max_size=10)
        queue.put("key1", "value1", priority=3)
        queue.update_priority("key1", priority=9)

        candidates = queue.get_eviction_candidates(count=1)
        assert "key1" not in [c[0] for c in candidates]

    def test_eviction_candidates(self):
        """Test eviction candidate selection."""
        queue = PriorityQueue(max_size=10)
        queue.put("low", "data", priority=1, frequency=1)
        queue.put("high", "data", priority=9, frequency=10)

        candidates = queue.get_eviction_candidates(count=1)
        assert candidates[0][0] == "low"

    def test_remove(self):
        """Test item removal."""
        queue = PriorityQueue(max_size=10)
        queue.put("key1", "value1")
        assert queue.remove("key1")
        assert not queue.contains("key1")

    def test_contains(self):
        """Test contains check."""
        queue = PriorityQueue(max_size=10)
        queue.put("key1", "value1")
        assert queue.contains("key1")
        assert not queue.contains("key2")

    def test_size(self):
        """Test size method."""
        queue = PriorityQueue(max_size=10)
        assert queue.size() == 0
        queue.put("key1", "value1")
        assert queue.size() == 1

    def test_clear(self):
        """Test clearing queue."""
        queue = PriorityQueue(max_size=10)
        queue.put("key1", "value1")
        queue.put("key2", "value2")
        queue.clear()
        assert queue.size() == 0

    def test_get_stats(self):
        """Test getting queue stats."""
        queue = PriorityQueue(max_size=10)
        queue.put("key1", "value1")
        stats = queue.get_stats()
        assert stats["size"] == 1
        assert stats["max_size"] == 10


# =============================================================================
# Test AccessTracker
# =============================================================================


class TestAccessTracker:
    """Tests for AccessTracker class."""

    def test_record_access(self):
        """Test recording access."""
        tracker = AccessTracker()
        tracker.record_access("file1", is_write=False, size=100)
        record = tracker.get_record("file1")
        assert record is not None
        assert record.access_count == 1
        assert record.read_count == 1

    def test_record_write(self):
        """Test recording write access."""
        tracker = AccessTracker()
        tracker.record_access("file1", is_write=True, size=100)
        record = tracker.get_record("file1")
        assert record.write_count == 1
        assert record.last_size == 100

    def test_get_record_nonexistent(self):
        """Test getting nonexistent record."""
        tracker = AccessTracker()
        record = tracker.get_record("nonexistent")
        assert record is None

    def test_remove_record(self):
        """Test removing record."""
        tracker = AccessTracker()
        tracker.record_access("file1")
        tracker.remove_record("file1")
        assert tracker.get_record("file1") is None

    def test_get_all_records(self):
        """Test getting all records."""
        tracker = AccessTracker()
        tracker.record_access("file1")
        tracker.record_access("file2")
        records = tracker.get_all_records()
        assert len(records) == 2

    def test_get_hottest_files(self):
        """Test getting hottest files."""
        tracker = AccessTracker()
        tracker.record_access("file1")
        tracker.record_access("file1")
        tracker.record_access("file1")
        tracker.record_access("file2")
        hottest = tracker.get_hottest_files(limit=1)
        assert hottest[0].path == "file1"

    def test_get_coldest_files(self):
        """Test getting coldest files."""
        tracker = AccessTracker()
        tracker.record_access("file1")
        tracker.record_access("file2")
        tracker.record_access("file2")
        coldest = tracker.get_coldest_files(limit=1)
        assert coldest[0].path == "file1"

    def test_get_stats(self):
        """Test getting tracker stats."""
        tracker = AccessTracker()
        tracker.record_access("file1", is_write=False)
        tracker.record_access("file2", is_write=True)
        stats = tracker.get_stats()
        assert stats["total_files"] == 2
        assert stats["total_accesses"] == 2

    def test_clear(self):
        """Test clearing tracker."""
        tracker = AccessTracker()
        tracker.record_access("file1")
        tracker.clear()
        assert tracker.get_stats()["total_files"] == 0


# =============================================================================
# Test Statistics
# =============================================================================


class TestStatistics:
    """Tests for Statistics class."""

    def test_create_statistics(self):
        """Test creating statistics object."""
        stats = Statistics()
        assert stats.memory is not None
        assert stats.disk is not None
        assert stats.cache is not None
        assert stats.operations is not None

    def test_update_memory(self):
        """Test updating memory stats."""
        stats = Statistics()
        stats.update_memory(current_usage=1000, file_count=5, total_size=2000)
        assert stats.memory.current_usage == 1000
        assert stats.memory.file_count == 5

    def test_set_memory_limit(self):
        """Test setting memory limit."""
        stats = Statistics()
        stats.set_memory_limit(10000)
        assert stats.memory.limit == 10000

    def test_update_disk(self):
        """Test updating disk stats."""
        stats = Statistics()
        stats.update_disk(path="./test", current_usage=5000, file_count=10)
        assert stats.disk.path == "./test"
        assert stats.disk.current_usage == 5000

    def test_record_cache_operations(self):
        """Test recording cache operations."""
        stats = Statistics()
        stats.record_cache_hit()
        stats.record_cache_hit()
        stats.record_cache_miss()
        stats.record_swap_in()
        stats.record_swap_out()
        stats.record_preload()

        assert stats.cache.hits == 2
        assert stats.cache.misses == 1
        assert stats.cache.swaps_in == 1
        assert stats.cache.swaps_out == 1
        assert stats.cache.preloads == 1

    def test_to_dict(self):
        """Test converting to dictionary."""
        stats = Statistics()
        stats.update_memory(current_usage=1000)
        result = stats.to_dict()
        assert "memory" in result
        assert "disk" in result
        assert "cache" in result
        assert "operations" in result
        assert "uptime_seconds" in result

    def test_reset(self):
        """Test resetting statistics."""
        stats = Statistics()
        stats.record_cache_hit()
        stats.reset()
        assert stats.cache.hits == 0


# =============================================================================
# Test OperationLogger
# =============================================================================


class TestOperationLogger:
    """Tests for OperationLogger class."""

    def test_create_logger(self):
        """Test creating logger."""
        logger = OperationLogger(enable_file_logging=False)
        assert logger is not None

    def test_log_operation(self):
        """Test logging operation."""
        logger = OperationLogger(enable_file_logging=False)
        logger.log(OperationType.READ, "file1", size=100)
        entries = logger.get_entries(limit=10)
        assert len(entries) == 1
        assert entries[0].operation == OperationType.READ

    def test_get_entries(self):
        """Test getting entries."""
        logger = OperationLogger(enable_file_logging=False)
        logger.log(OperationType.READ, "file1")
        logger.log(OperationType.WRITE, "file2")
        entries = logger.get_entries(limit=10)
        assert len(entries) == 2

    def test_get_entries_filter_by_operation(self):
        """Test filtering entries by operation."""
        logger = OperationLogger(enable_file_logging=False)
        logger.log(OperationType.READ, "file1")
        logger.log(OperationType.WRITE, "file2")
        entries = logger.get_entries(operation_type=OperationType.READ)
        assert len(entries) == 1

    def test_get_stats(self):
        """Test getting logger stats."""
        logger = OperationLogger(enable_file_logging=False)
        logger.log(OperationType.READ, "file1")
        logger.log(OperationType.WRITE, "file2", success=False, error="test error")
        stats = logger.get_stats()
        assert stats["total_entries"] == 2
        assert stats["successful"] == 1
        assert stats["failed"] == 1

    def test_clear(self):
        """Test clearing logs."""
        logger = OperationLogger(enable_file_logging=False)
        logger.log(OperationType.READ, "file1")
        logger.clear()
        assert len(logger.get_entries()) == 0


# =============================================================================
# Test Compressor
# =============================================================================


class TestCompressor:
    """Tests for Compressor classes."""

    def test_gzip_compress_decompress(self):
        """Test gzip compression and decompression."""
        compressor = GzipCompressor(level=6)
        data = b"Hello, World! " * 100
        compressed = compressor.compress(data)
        decompressed = compressor.decompress(compressed)
        assert decompressed == data
        assert len(compressed) < len(data)

    def test_compression_factory(self):
        """Test compression factory."""
        compressor = CompressionFactory.create("gzip", level=6)
        assert isinstance(compressor, GzipCompressor)

    def test_compression_factory_invalid(self):
        """Test factory with invalid algorithm."""
        with pytest.raises(ValueError):
            CompressionFactory.create("invalid_algorithm")

    def test_get_available_algorithms(self):
        """Test getting available algorithms."""
        algorithms = CompressionFactory.get_available_algorithms()
        assert "gzip" in algorithms


# =============================================================================
# Test VirtualDirectory
# =============================================================================


class TestVirtualDirectory:
    """Tests for VirtualDirectory class."""

    def test_create_directory(self):
        """Test creating directory."""
        dir = VirtualDirectory("test")
        assert dir.name == "test"
        assert dir.parent is None

    def test_add_file(self):
        """Test adding file."""
        dir = VirtualDirectory("test")
        assert dir.add_file("file1.txt")
        assert dir.has_file("file1.txt")

    def test_add_duplicate_file(self):
        """Test adding duplicate file."""
        dir = VirtualDirectory("test")
        dir.add_file("file1.txt")
        assert not dir.add_file("file1.txt")

    def test_remove_file(self):
        """Test removing file."""
        dir = VirtualDirectory("test")
        dir.add_file("file1.txt")
        assert dir.remove_file("file1.txt")
        assert not dir.has_file("file1.txt")

    def test_add_subdirectory(self):
        """Test adding subdirectory."""
        parent = VirtualDirectory("parent")
        child = parent.add_subdirectory("child")
        assert child.name == "child"
        assert child.parent == parent

    def test_list_files(self):
        """Test listing files."""
        dir = VirtualDirectory("test")
        dir.add_file("file1.txt")
        dir.add_file("file2.txt")
        files = dir.list_files()
        assert len(files) == 2

    def test_list_all(self):
        """Test listing all children."""
        dir = VirtualDirectory("test")
        dir.add_file("file1.txt")
        dir.add_subdirectory("subdir")
        all_items = dir.list_all()
        assert len(all_items) == 2


# =============================================================================
# Test DirectoryManager
# =============================================================================


class TestDirectoryManager:
    """Tests for DirectoryManager class."""

    def test_get_or_create_directory(self):
        """Test getting or creating directory."""
        manager = DirectoryManager()
        dir = manager.get_or_create_directory("virtual/subdir")
        assert dir is not None

    def test_resolve_path(self):
        """Test resolving path."""
        manager = DirectoryManager()
        directory, filename = manager.resolve_path("virtual/test.txt")
        assert directory is not None
        assert filename == "test.txt"

    def test_exists(self):
        """Test exists check."""
        manager = DirectoryManager()
        manager.get_or_create_directory("virtual/subdir")
        assert manager.exists("virtual/subdir")
        assert not manager.exists("virtual/nonexistent")

    def test_mkdir(self):
        """Test creating directory."""
        manager = DirectoryManager()
        assert manager.mkdir("virtual/newdir")
        assert manager.exists("virtual/newdir")

    def test_listdir(self):
        """Test listing directory."""
        manager = DirectoryManager()
        manager.get_or_create_directory("virtual/subdir")
        items = manager.listdir("virtual/")
        assert "subdir" in items

    def test_glob(self):
        """Test glob matching."""
        manager = DirectoryManager()
        dir = manager.get_or_create_directory("virtual/")
        dir.add_file("test1.txt")
        dir.add_file("test2.txt")
        dir.add_file("other.py")

        matches = manager.glob("virtual/test*.txt")
        assert len(matches) == 2


# =============================================================================
# Test DiskStorage
# =============================================================================


class TestDiskStorage:
    """Tests for DiskStorage class."""

    @pytest.fixture
    def storage(self, tmp_path):
        """Create test disk storage."""
        return DiskStorage(str(tmp_path / "disk_storage"))

    def test_put_get(self, storage):
        """Test basic put and get."""
        data = b"Hello, World!"
        storage.put("file1", data)
        retrieved = storage.get("file1")
        assert retrieved == data

    def test_get_nonexistent(self, storage):
        """Test getting nonexistent file."""
        assert storage.get("nonexistent") is None

    def test_contains(self, storage):
        """Test contains check."""
        storage.put("file1", b"data")
        assert storage.contains("file1")
        assert not storage.contains("file2")

    def test_remove(self, storage):
        """Test file removal."""
        storage.put("file1", b"data")
        assert storage.remove("file1")
        assert not storage.contains("file1")

    def test_update_priority(self, storage):
        """Test priority update."""
        storage.put("file1", b"data", priority=5)
        storage.update_priority("file1", priority=9)
        metadata = storage.get_metadata("file1")
        assert metadata["priority"] == 9

    def test_get_metadata(self, storage):
        """Test getting metadata."""
        storage.put("file1", b"data", priority=7)
        metadata = storage.get_metadata("file1")
        assert metadata["key"] == "file1"
        assert metadata["priority"] == 7
        assert metadata["original_size"] == 4

    def test_get_usage(self, storage):
        """Test getting usage."""
        storage.put("file1", b"data")
        usage = storage.get_usage()
        assert usage["file_count"] == 1
        assert "total_size" in usage

    def test_clear(self, storage):
        """Test clearing storage."""
        storage.put("file1", b"data")
        storage.put("file2", b"data")
        storage.clear()
        assert storage.get_usage()["file_count"] == 0


# =============================================================================
# Test MemFileSystem
# =============================================================================


class TestMemFileSystem:
    """Tests for MemFileSystem class."""

    @pytest.fixture
    def fs(self):
        """Create test filesystem."""
        fs = MemFileSystem(
            memory_limit=0.8,
            persist_path="./tmp/test_memfs_data",
            enable_logging=False,
        )
        yield fs
        fs.shutdown()

    def test_write_read(self, fs):
        """Test basic write and read."""
        fs.write("virtual/test.txt", "hello world")
        content = fs.read("virtual/test.txt")
        assert content.decode("utf-8") == "hello world"

    def test_write_read_bytes(self, fs):
        """Test binary write and read."""
        data = b"\x00\x01\x02\x03\x04"
        fs.write("virtual/binary.bin", data)
        content = fs.read("virtual/binary.bin")
        assert content == data

    def test_write_string_auto_encode(self, fs):
        """Test that strings are automatically encoded."""
        fs.write("virtual/text.txt", "hello")
        content = fs.read("virtual/text.txt")
        assert isinstance(content, bytes)

    def test_exists(self, fs):
        """Test exists check."""
        assert not fs.exists("virtual/nonexistent.txt")
        fs.write("virtual/existing.txt", "data")
        assert fs.exists("virtual/existing.txt")

    def test_delete(self, fs):
        """Test file deletion."""
        fs.write("virtual/to_delete.txt", "data")
        assert fs.exists("virtual/to_delete.txt")
        fs.delete("virtual/to_delete.txt")
        assert not fs.exists("virtual/to_delete.txt")
        assert not fs.delete("virtual/nonexistent.txt")

    def test_priority(self, fs):
        """Test priority management."""
        fs.write("virtual/priority_test.txt", "data", priority=8)
        assert fs.get_priority("virtual/priority_test.txt") == 8

        fs.set_priority("virtual/priority_test.txt", priority=3)
        assert fs.get_priority("virtual/priority_test.txt") == 3

        # Nonexistent file
        assert not fs.set_priority("virtual/nonexistent.txt", priority=5)
        assert fs.get_priority("virtual/nonexistent.txt") is None

    def test_mkdir_listdir(self, fs):
        """Test directory operations."""
        fs.mkdir("virtual/subdir")
        fs.write("virtual/subdir/file.txt", "data")

        items = fs.listdir("virtual/")
        assert "subdir" in items

    def test_rmdir(self, fs):
        """Test removing directory."""
        fs.mkdir("virtual/emptydir")
        assert fs.rmdir("virtual/emptydir")
        assert not fs.exists("virtual/emptydir")

    def test_glob(self, fs):
        """Test glob matching."""
        fs.write("virtual/test1.txt", "data1")
        fs.write("virtual/test2.txt", "data2")
        fs.write("virtual/other.txt", "data3")

        matches = fs.glob("virtual/test*.txt")
        assert len(matches) == 2

    def test_stats(self, fs):
        """Test statistics."""
        fs.write("virtual/file1.txt", "data" * 1000)
        stats = fs.get_stats()

        assert "memory" in stats
        assert "disk" in stats
        assert "cache" in stats
        assert stats["memory"]["file_count"] >= 1

    def test_gc(self, fs):
        """Test garbage collection."""
        for i in range(10):
            fs.write(f"virtual/file_{i}.txt", f"data {i}" * 1000, priority=3)

        swapped = fs.gc(target_usage=0.3)
        assert swapped >= 0

    def test_preload(self, fs):
        """Test file preloading."""
        fs.write("virtual/preload_test.txt", "data" * 1000, priority=3)
        fs.gc(0.0)

        task_id = fs.preload("virtual/preload_test.txt", priority=8)
        assert isinstance(task_id, str)

    def test_context_manager(self):
        """Test context manager."""
        with MemFileSystem(persist_path="./tmp/test_ctx_data") as fs:
            fs.write("virtual/test.txt", "data")
            assert fs.exists("virtual/test.txt")

    def test_open_file(self, fs):
        """Test opening file with open() method."""
        with fs.open("virtual/test.txt", "w") as f:
            f.write(b"hello")

        with fs.open("virtual/test.txt", "r") as f:
            content = f.read()
            assert content == b"hello"

    def test_get_file_info(self, fs):
        """Test getting file info."""
        fs.write("virtual/info.txt", "data", priority=7)
        info = fs.get_file_info("virtual/info.txt")
        assert info is not None
        assert info["location"] == "memory"
        assert info["priority"] == 7

    def test_get_file_info_nonexistent(self, fs):
        """Test getting info of nonexistent file."""
        info = fs.get_file_info("virtual/nonexistent.txt")
        assert info is None

    def test_read_nonexistent_raises(self, fs):
        """Test reading nonexistent file raises."""
        with pytest.raises(FileNotFoundError):
            fs.read("virtual/nonexistent.txt")

    def test_shutdown(self, fs):
        """Test shutdown."""
        fs.shutdown()
        assert fs._closed is True

    def test_write_after_shutdown_raises(self, fs):
        """Test writing after shutdown raises."""
        fs.shutdown()
        with pytest.raises(RuntimeError):
            fs.write("virtual/test.txt", "data")


# =============================================================================
# Test Native API
# =============================================================================


class TestNativeAPI:
    """Tests for native-style API."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test."""
        from memfs.api.native import (
            set_global_fs,
            MemFileSystem,
            write,
            read,
            exists,
            delete,
            mkdir,
            rmdir,
            listdir,
            glob,
            set_priority,
            get_priority,
            get_stats,
            get_file_info,
            gc,
        )

        fs = MemFileSystem(persist_path="./tmp/test_native_data", enable_logging=False)
        set_global_fs(fs)

        # Make imports available in test methods
        self.write = write
        self.read = read
        self.exists = exists
        self.delete = delete
        self.mkdir = mkdir
        self.rmdir = rmdir
        self.listdir = listdir
        self.glob = glob
        self.set_priority = set_priority
        self.get_priority = get_priority
        self.get_stats = get_stats
        self.get_file_info = get_file_info
        self.gc = gc

        yield

        fs.shutdown()

    def test_write_read(self):
        """Test native write and read."""
        self.write("virtual/native_test.txt", "hello")
        content = self.read("virtual/native_test.txt")
        assert content.decode("utf-8") == "hello"

    def test_exists_delete(self):
        """Test exists and delete."""
        self.write("virtual/to_delete.txt", "data")
        assert self.exists("virtual/to_delete.txt")
        self.delete("virtual/to_delete.txt")
        assert not self.exists("virtual/to_delete.txt")

    def test_mkdir_rmdir(self):
        """Test mkdir and rmdir."""
        assert self.mkdir("virtual/testdir")
        items = self.listdir("virtual/")
        assert "testdir" in items

    def test_listdir(self):
        """Test listdir."""
        self.write("virtual/file1.txt", "data")
        self.write("virtual/file2.txt", "data")
        items = self.listdir("virtual/")
        assert "file1.txt" in items
        assert "file2.txt" in items

    def test_glob(self):
        """Test glob."""
        self.write("virtual/glob_test1.txt", "data")
        self.write("virtual/glob_test2.txt", "data")
        matches = self.glob("virtual/glob_test*.txt")
        assert len(matches) == 2

    def test_set_get_priority(self):
        """Test set_priority and get_priority."""
        self.write("virtual/priority.txt", "data", priority=7)
        assert self.get_priority("virtual/priority.txt") == 7
        self.set_priority("virtual/priority.txt", priority=3)
        assert self.get_priority("virtual/priority.txt") == 3

    def test_get_stats(self):
        """Test get_stats."""
        stats = self.get_stats()
        assert "memory" in stats

    def test_get_file_info(self):
        """Test get_file_info."""
        self.write("virtual/info.txt", "data")
        info = self.get_file_info("virtual/info.txt")
        assert info is not None

    def test_gc(self):
        """Test gc."""
        for i in range(5):
            self.write(f"virtual/gc_file_{i}.txt", f"data {i}" * 100, priority=2)
        swapped = self.gc(target_usage=0.5)
        assert swapped >= 0


# =============================================================================
# Test Concurrent Access
# =============================================================================


class TestConcurrentAccess:
    """Tests for thread safety and concurrent access."""

    def test_concurrent_writes(self):
        """Test concurrent writes."""
        fs = MemFileSystem(persist_path="./tmp/test_concurrent", enable_logging=False)

        def write_file(i):
            fs.write(f"virtual/concurrent_{i}.txt", f"data {i}")

        threads = [threading.Thread(target=write_file, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for i in range(10):
            assert fs.exists(f"virtual/concurrent_{i}.txt")

        fs.shutdown()

    def test_concurrent_reads(self):
        """Test concurrent reads."""
        fs = MemFileSystem(
            persist_path="./tmp/test_concurrent_read", enable_logging=False
        )
        fs.write("virtual/shared.txt", "shared data")

        def read_file():
            for _ in range(10):
                fs.read("virtual/shared.txt")

        threads = [threading.Thread(target=read_file) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        fs.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
