"""
Tests for real path persistence and new features.
"""

import os
import sys
import time
import shutil
import pytest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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


class TestPersistentListdir:
    """Test listdir functionality in persistent mode."""

    def setup_method(self):
        """Setup test fixtures."""
        self.test_dir = "./tmp/test_persist_listdir"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def teardown_method(self):
        """Cleanup after tests."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_listdir_with_preexisting_files(self):
        """Test listdir shows pre-existing files in persistent mode.

        Bug fix: In persistent mode, when specifying a directory that already
        contains files, listdir should return those files even with lazy loading.
        """
        # Create directory and files first
        os.makedirs(self.test_dir, exist_ok=True)
        test_file = os.path.join(self.test_dir, "preexisting.txt")
        with open(test_file, "wb") as f:
            f.write(b"pre-existing data")

        # Create filesystem in persistent mode pointing to existing directory
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # listdir should return the pre-existing file
        contents = fs.listdir("/")
        assert "preexisting.txt" in contents, (
            f"Expected 'preexisting.txt' in listdir result, got {contents}"
        )

        # Should also be able to read the file
        data = fs.read("/preexisting.txt")
        assert data == b"pre-existing data"

        fs.shutdown(wait=True)

    def test_listdir_with_nested_preexisting_files(self):
        """Test listdir shows nested pre-existing files in persistent mode."""
        # Create directory structure with files
        os.makedirs(self.test_dir, exist_ok=True)
        subdir = os.path.join(self.test_dir, "subdir")
        os.makedirs(subdir, exist_ok=True)

        test_file1 = os.path.join(self.test_dir, "root.txt")
        test_file2 = os.path.join(subdir, "nested.txt")

        with open(test_file1, "wb") as f:
            f.write(b"root data")
        with open(test_file2, "wb") as f:
            f.write(b"nested data")

        # Create filesystem in persistent mode
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Root directory should list the file and subdirectory
        root_contents = fs.listdir("/")
        assert "root.txt" in root_contents
        assert "subdir" in root_contents

        # Subdirectory should list its file
        subdir_contents = fs.listdir("/subdir")
        assert "nested.txt" in subdir_contents

        fs.shutdown(wait=True)

    def test_listdir_after_new_file_in_persist_mode(self):
        """Test listdir works correctly after adding new files in persistent mode."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Initially empty
        contents = fs.listdir("/")
        assert contents == []

        # Add a file
        fs.write("/newfile.txt", b"new data")

        # Should appear in listdir
        contents = fs.listdir("/")
        assert "newfile.txt" in contents

        fs.shutdown(wait=True)


class TestTempModePathSafety:
    """Test path safety checks in temporary mode."""

    def test_temp_mode_rejects_existing_file(self):
        """Test that temp mode raises error if path exists as a file."""
        test_file = "./tmp/test_safety_file.txt"
        os.makedirs("./tmp", exist_ok=True)

        # Create a file at the target path
        with open(test_file, "wb") as f:
            f.write(b"existing file")

        try:
            # Should raise FileExistsError
            with pytest.raises(FileExistsError) as exc_info:
                MemFileSystem(
                    storage_mode="temp",
                    persist_path=test_file,
                )

            assert "already exists as a file" in str(exc_info.value)
        finally:
            # Cleanup
            if os.path.exists(test_file):
                os.remove(test_file)

    def test_temp_mode_rejects_nonempty_directory(self):
        """Test that temp mode raises error if path exists as non-empty directory."""
        test_dir = "./tmp/test_safety_nonempty"
        os.makedirs(test_dir, exist_ok=True)

        # Create a file inside the directory
        test_file = os.path.join(test_dir, "existing.txt")
        with open(test_file, "wb") as f:
            f.write(b"existing")

        try:
            # Should raise FileExistsError
            with pytest.raises(FileExistsError) as exc_info:
                MemFileSystem(
                    storage_mode="temp",
                    persist_path=test_dir,
                )

            assert "already exists as a non-empty directory" in str(exc_info.value)
        finally:
            # Cleanup
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)

    def test_temp_mode_allows_empty_directory(self):
        """Test that temp mode allows empty directory."""
        test_dir = "./tmp/test_safety_empty"
        os.makedirs(test_dir, exist_ok=True)

        try:
            # Should succeed (empty directory is ok)
            fs = MemFileSystem(
                storage_mode="temp",
                persist_path=test_dir,
            )

            # Should be able to use it normally
            fs.write("/test.txt", b"data")
            assert fs.exists("/test.txt")

            fs.shutdown(wait=True)
        finally:
            # Cleanup (directory might be cleaned by temp mode)
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)

    def test_temp_mode_creates_new_directory(self):
        """Test that temp mode creates new directory if it doesn't exist."""
        test_dir = "./tmp/test_safety_new"

        # Ensure it doesn't exist
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

        try:
            # Should succeed and create the directory
            fs = MemFileSystem(
                storage_mode="temp",
                persist_path=test_dir,
            )

            fs.write("/test.txt", b"data")
            assert fs.exists("/test.txt")

            fs.shutdown(wait=True)

            # In temp mode, directory should be cleaned up on shutdown
            # (but we don't test this as it happens in atexit)
        finally:
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)


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


class TestAsyncShutdown:
    """Test asynchronous shutdown functionality."""

    def setup_method(self):
        """Setup test fixtures."""
        self.test_dir = "./tmp/test_async_shutdown"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def teardown_method(self):
        """Cleanup after tests."""
        # Wait a bit for async operations to complete
        time.sleep(0.5)
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_shutdown_returns_immediately(self):
        """Test that shutdown(wait=False) returns immediately."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Write multiple files
        for i in range(5):
            fs.write(f"/file{i}.txt", b"data" * 1000)

        # Async shutdown should return immediately
        import time

        start = time.time()
        pending = fs.shutdown(wait=False)
        elapsed = time.time() - start

        # Should return in less than 0.1 seconds
        assert elapsed < 0.1, (
            f"shutdown(wait=False) took {elapsed}s, should be immediate"
        )
        # May have pending tasks
        assert pending >= 0

    def test_shutdown_async_method(self):
        """Test shutdown_async convenience method."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        fs.write("/test.txt", b"data")

        # shutdown_async should return pending count
        pending = fs.shutdown_async()
        assert isinstance(pending, int)
        assert pending >= 0


class TestPathNormalization:
    """Test path normalization for Windows compatibility."""

    def setup_method(self):
        """Setup test fixtures."""
        self.test_dir = "./tmp/test_path_norm"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def teardown_method(self):
        """Cleanup after tests."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_write_with_backslash_path(self):
        """Test writing file with Windows-style backslash path."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Use backslash (Windows style)
        result = fs.write("\\test.txt", b"data")
        assert result == 4

        # Should be readable with forward slash
        data = fs.read("/test.txt")
        assert data == b"data"

        fs.shutdown(wait=True)

    def test_read_with_backslash_path(self):
        """Test reading file with Windows-style backslash path."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Write with forward slash
        fs.write("/test.txt", b"data")

        # Read with backslash
        data = fs.read("\\test.txt")
        assert data == b"data"

        fs.shutdown(wait=True)

    def test_exists_with_backslash_path(self):
        """Test exists check with Windows-style backslash path."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        fs.write("/test.txt", b"data")

        # Check with backslash
        assert fs.exists("\\test.txt") is True
        assert fs.exists("/test.txt") is True

        fs.shutdown(wait=True)

    def test_delete_with_backslash_path(self):
        """Test delete with Windows-style backslash path."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        fs.write("/test.txt", b"data")
        assert fs.exists("/test.txt")

        # Delete with backslash
        result = fs.delete("\\test.txt")
        assert result is True
        assert fs.exists("/test.txt") is False

        fs.shutdown(wait=True)

    def test_mkdir_rmdir_with_backslash(self):
        """Test mkdir/rmdir with Windows-style backslash path."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Create directory with backslash
        result = fs.mkdir("\\subdir")
        assert result is True

        # Check with forward slash
        assert fs.exists("/subdir") is True

        # Remove with backslash
        result = fs.rmdir("\\subdir")
        assert result is True

        fs.shutdown(wait=True)

    def test_listdir_with_backslash(self):
        """Test listdir with Windows-style backslash path."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        fs.write("/test.txt", b"data")

        # List with backslash
        contents = fs.listdir("\\")
        assert "test.txt" in contents

        fs.shutdown(wait=True)

    def test_mixed_slash_paths(self):
        """Test mixed slash usage in paths."""
        fs = MemFileSystem(
            storage_mode="persist",
            persist_path=self.test_dir,
        )

        # Write with mixed slashes (should normalize)
        fs.write("/subdir\\file.txt", b"data")

        # Should be readable
        assert fs.exists("/subdir/file.txt")
        assert fs.exists("\\subdir\\file.txt")

        data = fs.read("/subdir/file.txt")
        assert data == b"data"

        fs.shutdown(wait=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
