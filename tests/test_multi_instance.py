"""
Unit tests for multi-instance support in MemFS.
Tests reference counting, instance isolation, and lifecycle management.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

import memfs
from memfs.core.instance_manager import (
    InstanceManager,
    get_global_instance_manager,
    reset_global_instance_manager,
)


class TestInstanceManager:
    """Test InstanceManager class."""

    def setup_method(self):
        """Reset instance manager before each test."""
        reset_global_instance_manager()

    def teardown_method(self):
        """Cleanup after each test."""
        reset_global_instance_manager()

    def test_create_instance(self, tmp_path):
        """Test creating a single instance."""
        persist_path = str(tmp_path / "data")
        manager = InstanceManager()

        fs = manager.get_or_create_instance(persist_path=persist_path)

        assert fs is not None
        assert fs._persist_path == str(Path(persist_path).resolve())
        assert manager.get_instance_count() == 1

    def test_same_path_returns_same_instance(self, tmp_path):
        """Test that same persist_path returns same instance."""
        persist_path = str(tmp_path / "data")
        manager = InstanceManager()

        fs1 = manager.get_or_create_instance(persist_path=persist_path)
        fs2 = manager.get_or_create_instance(persist_path=persist_path)

        assert fs1 is fs2
        assert manager.get_instance_count() == 1

    def test_different_paths_create_different_instances(self, tmp_path):
        """Test that different persist_paths create different instances."""
        path_a = str(tmp_path / "data_a")
        path_b = str(tmp_path / "data_b")
        manager = InstanceManager()

        fs1 = manager.get_or_create_instance(persist_path=path_a)
        fs2 = manager.get_or_create_instance(persist_path=path_b)

        assert fs1 is not fs2
        assert manager.get_instance_count() == 2
        assert fs1._persist_path != fs2._persist_path

    def test_reference_counting(self, tmp_path):
        """Test reference counting mechanism."""
        persist_path = str(tmp_path / "data")
        manager = InstanceManager()

        fs1 = manager.get_or_create_instance(persist_path=persist_path)
        fs2 = manager.get_or_create_instance(persist_path=persist_path)

        assert fs1 is fs2
        stats = manager.get_instance_stats()
        key = str(Path(persist_path).resolve())
        assert stats[key]["ref_count"] == 2

        # Release once - should still exist
        result = manager.release_instance(fs1)
        assert result is False  # Not shut down yet
        assert manager.get_instance_count() == 1

        # Release again - should be shut down
        result = manager.release_instance(fs2)
        assert result is True  # Shut down and removed
        assert manager.get_instance_count() == 0

    def test_has_instance(self, tmp_path):
        """Test checking instance existence."""
        path_a = str(tmp_path / "data_a")
        path_b = str(tmp_path / "data_b")
        manager = InstanceManager()

        assert manager.has_instance(path_a) is False

        manager.get_or_create_instance(persist_path=path_a)
        assert manager.has_instance(path_a) is True
        assert manager.has_instance(path_b) is False

    def test_close_all(self, tmp_path):
        """Test closing all instances."""
        path_a = str(tmp_path / "data_a")
        path_b = str(tmp_path / "data_b")
        manager = InstanceManager()

        fs1 = manager.get_or_create_instance(persist_path=path_a)
        fs2 = manager.get_or_create_instance(persist_path=path_b)

        manager.close_all()

        assert manager.get_instance_count() == 0
        assert fs1._closed is True
        assert fs2._closed is True


class TestMultiInstanceIsolation:
    """Test multi-instance isolation."""

    def setup_method(self):
        """Reset before each test."""
        reset_global_instance_manager()

    def teardown_method(self):
        """Cleanup after each test."""
        reset_global_instance_manager()

    def test_two_instances_file_isolation(self, tmp_path):
        """Test that files in different instances are isolated."""
        path_a = str(tmp_path / "data_a")
        path_b = str(tmp_path / "data_b")

        fs1 = memfs.init(persist_path=path_a)
        fs2 = memfs.init(persist_path=path_b)

        # Write to fs1 using fs1 directly
        fs1.write("/test.txt", b"data_from_fs1")

        # Write to fs2 using fs2 directly
        fs2.write("/test.txt", b"data_from_fs2")

        # Verify isolation - use specific instances
        assert fs1.read("/test.txt") == b"data_from_fs1"
        assert fs2.read("/test.txt") == b"data_from_fs2"

        # fs2 should not see fs1's file
        assert fs1.exists("/test.txt") is True
        assert fs2.exists("/test.txt") is True
        assert len(fs1.listdir("/")) == 1
        assert len(fs2.listdir("/")) == 1

        # Verify content isolation (the files have same name but different content)
        # Each instance has its own /test.txt with different data
        assert fs1.read("/test.txt") != fs2.read("/test.txt")

    def test_instance_persistence_isolation(self, tmp_path):
        """Test that persistent data is isolated between instances."""
        path_a = str(tmp_path / "data_a")
        path_b = str(tmp_path / "data_b")

        # Create instances with persist mode
        fs1 = memfs.init(persist_path=path_a, storage_mode="persist")
        fs2 = memfs.init(persist_path=path_b, storage_mode="persist")

        # Write data using each instance directly
        fs1.write("/file_a.txt", b"data_a")
        fs2.write("/file_b.txt", b"data_b")

        # Close instances
        memfs.close(fs1)
        memfs.close(fs2)

        # Verify persistence directories are separate
        dir_a = Path(path_a)
        dir_b = Path(path_b)

        assert dir_a.exists()
        assert dir_b.exists()
        assert (dir_a / "file_a.txt").exists()
        assert (dir_b / "file_b.txt").exists()
        assert not (dir_a / "file_b.txt").exists()
        assert not (dir_b / "file_a.txt").exists()

    def test_same_instance_retrieval(self, tmp_path):
        """Test that same persist_path returns same instance."""
        persist_path = str(tmp_path / "data")

        fs1 = memfs.init(persist_path=persist_path)
        fs2 = memfs.init(persist_path=persist_path)

        assert fs1 is fs2

        # Write with fs1
        memfs.write("/test.txt", b"hello")

        # Read with fs2 (should see same data)
        assert fs2.read("/test.txt") == b"hello"


class TestInstanceLifecycle:
    """Test instance lifecycle management."""

    def setup_method(self):
        """Reset before each test."""
        reset_global_instance_manager()

    def teardown_method(self):
        """Cleanup after each test."""
        reset_global_instance_manager()

    def test_close_function(self, tmp_path):
        """Test close() function."""
        persist_path = str(tmp_path / "data")

        fs = memfs.init(persist_path=persist_path)
        assert memfs.get_instance_count() == 1

        result = memfs.close(fs)
        assert result is True
        assert memfs.get_instance_count() == 0
        assert fs._closed is True

    def test_close_global_instance(self, tmp_path):
        """Test closing global instance."""
        persist_path = str(tmp_path / "data")

        memfs.init(persist_path=persist_path)
        assert memfs.get_instance_count() == 1

        memfs.close()  # Close global instance
        assert memfs.get_instance_count() == 0

    def test_close_instance_by_path(self, tmp_path):
        """Test close_instance() function."""
        persist_path = str(tmp_path / "data")

        fs = memfs.init(persist_path=persist_path)
        assert memfs.has_instance(persist_path) is True

        result = memfs.close_instance(persist_path)
        assert result is True
        assert memfs.has_instance(persist_path) is False

    def test_get_instance_stats(self, tmp_path):
        """Test get_instance_stats() function."""
        path_a = str(tmp_path / "data_a")
        path_b = str(tmp_path / "data_b")

        memfs.init(persist_path=path_a)
        memfs.init(persist_path=path_b)

        stats = memfs.get_instance_stats()

        assert len(stats) == 2

        key_a = str(Path(path_a).resolve())
        key_b = str(Path(path_b).resolve())

        assert key_a in stats
        assert key_b in stats
        assert stats[key_a]["ref_count"] == 1
        assert stats[key_b]["ref_count"] == 1
        assert stats[key_a]["storage_mode"] == "temp"

    def test_reference_count_with_multiple_inits(self, tmp_path):
        """Test reference count with multiple inits."""
        persist_path = str(tmp_path / "data")

        fs1 = memfs.init(persist_path=persist_path)
        fs2 = memfs.init(persist_path=persist_path)
        fs3 = memfs.init(persist_path=persist_path)

        assert fs1 is fs2 is fs3
        assert memfs.get_instance_count() == 1

        stats = memfs.get_instance_stats()
        key = str(Path(persist_path).resolve())
        assert stats[key]["ref_count"] == 3

        # Close once - should still exist
        memfs.close(fs1)
        assert memfs.get_instance_count() == 1

        # Close twice - should still exist
        memfs.close(fs2)
        assert memfs.get_instance_count() == 1

        # Close third time - should be removed
        memfs.close(fs3)
        assert memfs.get_instance_count() == 0

    def test_close_all_instances(self, tmp_path):
        """Test close_all_instances() function."""
        path_a = str(tmp_path / "data_a")
        path_b = str(tmp_path / "data_b")

        fs1 = memfs.init(persist_path=path_a)
        fs2 = memfs.init(persist_path=path_b)

        assert memfs.get_instance_count() == 2

        memfs.close_all_instances()

        assert memfs.get_instance_count() == 0
        assert fs1._closed is True
        assert fs2._closed is True


class TestLegacyMode:
    """Test legacy mode (named_instance=False)."""

    def setup_method(self):
        """Reset before each test."""
        reset_global_instance_manager()

    def teardown_method(self):
        """Cleanup after each test."""
        reset_global_instance_manager()

    def test_legacy_mode_always_creates_new(self, tmp_path):
        """Test that legacy mode always creates new instance."""
        persist_path = str(tmp_path / "data")

        fs1 = memfs.init(persist_path=persist_path, named_instance=False)
        fs2 = memfs.init(persist_path=persist_path, named_instance=False)

        # In legacy mode, fs1 should be closed and fs2 is new
        assert fs1._closed is True
        assert fs2._closed is False
        assert fs1 is not fs2

    def test_legacy_mode_close(self, tmp_path):
        """Test closing in legacy mode."""
        persist_path = str(tmp_path / "data")

        fs = memfs.init(persist_path=persist_path, named_instance=False)
        result = memfs.close(fs, named_instance=False)

        assert result is True
        assert fs._closed is True


class TestRealWorldScenario:
    """Test real-world scenarios like training with multiple datasets."""

    def setup_method(self):
        """Reset before each test."""
        reset_global_instance_manager()

    def teardown_method(self):
        """Cleanup after each test."""
        reset_global_instance_manager()

    def test_train_val_dataset_scenario(self, tmp_path):
        """Test train/val dataset scenario."""
        train_path = str(tmp_path / "train")
        val_path = str(tmp_path / "val")

        # Initialize train and val datasets
        train_fs = memfs.init(persist_path=train_path, storage_mode="persist")
        val_fs = memfs.init(persist_path=val_path, storage_mode="persist")

        # Write training data using train_fs
        train_fs.write("/train_data_1.bin", b"train_data_1")
        train_fs.write("/train_data_2.bin", b"train_data_2")

        # Write validation data using val_fs
        val_fs.write("/val_data_1.bin", b"val_data_1")
        val_fs.write("/val_data_2.bin", b"val_data_2")

        # Verify both datasets are accessible using their respective instances
        assert train_fs.read("/train_data_1.bin") == b"train_data_1"
        assert train_fs.read("/train_data_2.bin") == b"train_data_2"
        assert val_fs.read("/val_data_1.bin") == b"val_data_1"
        assert val_fs.read("/val_data_2.bin") == b"val_data_2"

        # List directory contents
        train_files = train_fs.listdir("/")
        val_files = val_fs.listdir("/")

        assert len(train_files) == 2
        assert len(val_files) == 2
        assert all("train" in f for f in train_files)
        assert all("val" in f for f in val_files)

        # Close train dataset
        memfs.close(train_fs)

        # Val dataset should still be accessible
        assert val_fs.read("/val_data_1.bin") == b"val_data_1"
        assert memfs.get_instance_count() == 1
