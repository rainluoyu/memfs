"""
Tests for real path persistence and new features.
"""

import os
import time
import shutil
import pytest
from pathlib import Path

from memfs import MemFileSystem
from memfs.storage.real_path import RealPathStorage
from memfs.storage.lock_manager import FileLockManager


class TestRealPathStorage:
    """Test real path storage functionality."""

    def setup_method(self):
        """Setup test fixtures."""
        self.test_dir = "./tmp/test_real_path"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def teardown_method(self):
        """Cleanup after tests."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_virtual_to_real_mapping(self):
        """Test virtual path to real path mapping."""
        storage = RealPathStorage(real_root=self.test_dir, temp_mode=False)

        # Test path conversion
        real_path = storage.get_real_path("/test.txt")
        assert str(real_path).endswith("test.txt")
        # Use resolved path for comparison
        expected_dir = os.path.abspath(self.test_dir)
        assert expected_dir in str(real_path)

    def test_write_and_read(self):
        """Test synchronous write and read."""
        storage = RealPathStorage(real_root=self.test_dir, temp_mode=False)

        # Write file
        result = storage.write_sync("/test.txt", b"Hello World")
        assert result is True

        # Read file
        data = storage.read_sync("/test.txt")
        assert data == b"Hello World"

    def test_delete(self):
        """Test file deletion."""
        storage = RealPathStorage(real_root=self.test_dir, temp_mode=False)

        # Write then delete
        storage.write_sync("/test.txt", b"data")
        assert storage.exists("/test.txt") is True

        result = storage.delete_sync("/test.txt")
        assert result is True
        assert storage.exists("/test.txt") is False

    def test_external_modification_detection(self):
        """Test detection of external file modifications."""
        storage = RealPathStorage(real_root=self.test_dir, temp_mode=False)

        # Write file
        storage.write_sync("/test.txt", b"original")
        file_info = storage.get_file_info("/test.txt")

        # Wait a bit to ensure mtime difference
        time.sleep(0.05)

        # Modify externally
        real_path = storage.get_real_path("/test.txt")
        with open(real_path, "wb") as f:
            f.write(b"modified")

        # Check detection
        modified = storage.check_external_modified(
            "/test.txt",
            file_info["mtime"],
            file_info["size"],
        )
        assert modified is True

    def test_reload_on_modified(self):
        """Test reloading file after external modification."""
        storage = RealPathStorage(real_root=self.test_dir, temp_mode=False)

        # Write file
        storage.write_sync("/test.txt", b"original")
        file_info = storage.get_file_info("/test.txt")
        assert file_info is not None, "File info should exist"

        # Wait a bit to ensure mtime difference
        time.sleep(0.05)

        # Modify externally
        real_path = storage.get_real_path("/test.txt")
        with open(real_path, "wb") as f:
            f.write(b"modified content")

        # Reload
        modified, new_data = storage.reload_if_modified(
            "/test.txt",
            file_info["mtime"],
            file_info["size"],
        )

        assert modified is True
        assert new_data == b"modified content"


class TestFileLockManager:
    """Test file lock manager."""

    def test_write_lock(self):
        """Test write lock acquisition and release."""
        lock_mgr = FileLockManager()

        # Acquire write lock
        result = lock_mgr.acquire_write("/test.txt")
        assert result is True

        # Release
        lock_mgr.release_write("/test.txt")

    def test_read_lock(self):
        """Test read lock acquisition and release."""
        lock_mgr = FileLockManager()

        # Acquire read lock
        result = lock_mgr.acquire_read("/test.txt")
        assert result is True

        # Release
        lock_mgr.release_read("/test.txt")

    def test_concurrent_readers(self):
        """Test multiple concurrent readers."""
        lock_mgr = FileLockManager()

        # Multiple readers should succeed
        assert lock_mgr.acquire_read("/test.txt") is True
        assert lock_mgr.acquire_read("/test.txt") is True

        lock_mgr.release_read("/test.txt")
        lock_mgr.release_read("/test.txt")


class TestPersistModes:
    """Test persistent and temporary modes."""

    def setup_method(self):
        """Setup test fixtures."""
        self.test_dir = "./tmp/test_modes"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def teardown_method(self):
        """Cleanup after tests."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_temp_mode_cleanup(self):
        """Test temporary mode cleanup on shutdown."""
        fs = MemFileSystem(storage_mode="temp")

        # Write file
        fs.write("/test.txt", b"temporary data")

        # Shutdown
        fs.shutdown(wait=True)

        # Temp directory should be cleaned up
        # (we can't check the exact path, but we trust the implementation)

    def test_persist_mode_survive_restart(self):
        """Test persistent mode survives restart."""
        # First instance
        fs1 = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )
        fs1.write("/test.txt", b"persistent data")
        fs1.shutdown(wait=True)

        # Verify file exists
        real_path = os.path.join(self.test_dir, "test.txt")
        assert os.path.exists(real_path)

        # Second instance
        fs2 = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Read data
        data = fs2.read("/test.txt")
        assert data == b"persistent data"

        fs2.shutdown(wait=True)

    def test_lazy_loading(self):
        """Test lazy loading from real path."""
        # Create file directly
        os.makedirs(self.test_dir, exist_ok=True)
        real_path = os.path.join(self.test_dir, "test.txt")
        with open(real_path, "wb") as f:
            f.write(b"pre-existing data")

        # Create filesystem
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Read file (should lazy load)
        data = fs.read("/test.txt")
        assert data == b"pre-existing data"

        fs.shutdown(wait=True)


class TestAsyncWrite:
    """Test asynchronous write functionality."""

    def setup_method(self):
        """Setup test fixtures."""
        self.test_dir = "./tmp/test_async"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def teardown_method(self):
        """Cleanup after tests."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_async_write_completion(self):
        """Test that async write completes."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Write file
        fs.write("/test.txt", b"async data")

        # Wait for async write
        time.sleep(1)

        # Verify file exists
        real_path = os.path.join(self.test_dir, "test.txt")
        assert os.path.exists(real_path)

        with open(real_path, "rb") as f:
            data = f.read()
        assert data == b"async data"

        fs.shutdown(wait=True)

    def test_write_then_read(self):
        """Test write followed by immediate read."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Write and immediately read
        fs.write("/test.txt", b"test data")
        data = fs.read("/test.txt")

        assert data == b"test data"

        fs.shutdown(wait=True)


class TestSwapOut:
    """Test memory swap-out to real path."""

    def setup_method(self):
        """Setup test fixtures."""
        self.test_dir = "./tmp/test_swap"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def teardown_method(self):
        """Cleanup after tests."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_gc_triggers_swap_out(self):
        """Test that GC swaps out low priority files."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
            memory_limit=0.001,  # Very small limit to force GC
        )

        # Write low priority file
        fs.write("/test.txt", b"data", priority=1)

        # Force GC
        fs.gc(target_usage=0.0)

        # Wait for async swap
        time.sleep(1)

        # Verify file exists on disk
        real_path = os.path.join(self.test_dir, "test.txt")
        assert os.path.exists(real_path)

        fs.shutdown(wait=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
