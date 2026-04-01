"""
Memory eviction tests for MemFS.
Tests for memory management and eviction logic.
"""

import pytest
import time
from memfs.storage.memory import MemoryManager


class TestMemoryEviction:
    """Tests for memory eviction logic."""

    def test_eviction_capacity_calculation(self):
        """Test 1: Verify memory usage calculation is correct after eviction."""
        memory_limit = 1 * 1024 * 1024  # 1MB
        manager = MemoryManager(memory_limit_bytes=memory_limit)

        file_size = 2 * 1024 * 1024  # 2MB per file
        evicted_keys = []

        for i in range(5):
            key = f"file_{i}"
            data = bytes([i % 256] * file_size)
            evicted = manager.put(key, data, priority=5)
            evicted_keys.extend(evicted)

        current_usage = manager.get_usage()["current_usage"]
        actual_memory_usage = sum(
            manager.get_file_info(k)["size"]
            for k in manager.get_all_keys()
            if manager.get_file_info(k)
        )

        assert current_usage == actual_memory_usage, (
            f"Memory usage mismatch: "
            f"_current_usage={current_usage}, "
            f"actual={actual_memory_usage}, "
            f"error={abs(current_usage - actual_memory_usage)} bytes"
        )

    def test_file_location_state_consistency(self):
        """Test 2: Verify _file_locations state consistency after eviction."""
        from memfs.storage.hybrid import HybridStorage
        import tempfile
        import shutil

        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 1 * 1024 * 1024  # 1MB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            file_size = 512 * 1024  # 512KB per file
            keys = []

            for i in range(5):
                key = f"/file_{i}.txt"
                data = bytes([i % 256] * file_size)
                fs.put(key, data, priority=5)
                keys.append(key)
                time.sleep(0.1)

            fs.worker.wait_all(timeout=10)
            time.sleep(0.5)

            for key in keys:
                location = fs.get_file_location(key)
                in_memory = fs.memory.contains(key)
                on_disk = fs.real_storage.exists(key)

                if location == "memory":
                    assert in_memory, f"{key}: location=memory but not in memory"
                    assert not on_disk, f"{key}: location=memory but on disk"
                elif location == "real":
                    assert not in_memory, f"{key}: location=real but in memory"
                    assert on_disk, f"{key}: location=real but not on disk"
                elif location == "both":
                    assert in_memory, f"{key}: location=both but not in memory"
                    assert on_disk, f"{key}: location=both but not on disk"

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_eviction_read_integrity(self):
        """Test 3: Verify evicted files can be read correctly."""
        from memfs.storage.hybrid import HybridStorage
        import tempfile
        import shutil

        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 1 * 1024 * 1024  # 1MB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            file_a_key = "/file_a.txt"
            file_a_data = bytes(range(256)) * (2 * 1024)  # 2MB pattern data

            fs.put(file_a_key, file_a_data, priority=5)

            for i in range(4):
                key = f"/file_bc_{i}.txt"
                data = bytes([i % 256] * (512 * 1024))
                fs.put(key, data, priority=5)
                time.sleep(0.1)

            fs.worker.wait_all(timeout=10)
            time.sleep(0.5)

            read_data = fs.get(file_a_key)

            assert read_data is not None, "Evicted file returned None"
            assert len(read_data) == len(file_a_data), (
                f"Size mismatch: expected={len(file_a_data)}, got={len(read_data)}"
            )
            assert read_data == file_a_data, "Content mismatch after eviction"

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_numpy_file_loading_scenario(self):
        """Test 4: Test .npy file loading scenario."""
        from memfs.storage.hybrid import HybridStorage
        import tempfile
        import shutil
        import io

        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 1 * 1024 * 1024  # 1MB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            try:
                import numpy as np

                npy_files = []
                for i in range(5):
                    key = f"/data_{i}.npy"
                    array = np.random.rand(100, 100).astype(np.float64)
                    buffer = io.BytesIO()
                    np.save(buffer, array)
                    data = buffer.getvalue()

                    fs.put(key, data, priority=5)
                    npy_files.append((key, data))
                    time.sleep(0.1)

                fs.worker.wait_all(timeout=10)
                time.sleep(0.5)

                for i, (key, original_data) in enumerate(npy_files):
                    data = fs.get(key)
                    assert data is not None, f"File {key} returned None"

                    buffer = io.BytesIO(data)
                    loaded_array = np.load(buffer)
                    assert loaded_array.shape == (100, 100), f"Shape mismatch for {key}"

                fs.shutdown(wait=True)
            except ImportError:
                pytest.skip("numpy not installed")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_concurrent_eviction_stress(self):
        """Test 5: Stress test for concurrent eviction."""
        from memfs.storage.hybrid import HybridStorage
        import tempfile
        import shutil
        import threading

        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 512 * 1024  # 512KB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=2,
            )

            errors = []
            num_threads = 4
            ops_per_thread = 10

            def worker(thread_id):
                try:
                    for i in range(ops_per_thread):
                        key = f"/thread_{thread_id}_file_{i}.txt"
                        data = bytes([i % 256] * (128 * 1024))
                        fs.put(key, data, priority=5)

                        read_data = fs.get(key)
                        if read_data is None:
                            errors.append(f"Thread {thread_id}: Got None for {key}")
                        elif read_data != data:
                            errors.append(
                                f"Thread {thread_id}: Data mismatch for {key}"
                            )
                except Exception as e:
                    errors.append(f"Thread {thread_id}: Exception: {e}")

            threads = []
            for t in range(num_threads):
                thread = threading.Thread(target=worker, args=(t,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join(timeout=30)

            fs.worker.wait_all(timeout=10)

            usage = fs.memory.get_usage()
            assert usage["current_usage"] >= 0, "Negative memory usage"

            assert len(errors) == 0, f"Errors occurred: {errors}"

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_eviction_callback_invocation(self):
        """Test 6: Verify eviction callback is invoked correctly."""
        memory_limit = 512 * 1024  # 512KB
        evicted_keys = []

        def on_eviction(key):
            evicted_keys.append(key)

        manager = MemoryManager(
            memory_limit_bytes=memory_limit, on_eviction=on_eviction
        )

        file_size = 256 * 1024  # 256KB
        for i in range(5):
            key = f"file_{i}"
            data = bytes([i % 256] * file_size)
            manager.put(key, data, priority=5)

        # After writing 5 files of 256KB each with 512KB limit,
        # only 2 files should remain in memory, 3 should be evicted
        assert len(evicted_keys) >= 1, "No eviction callbacks were invoked"

        # Verify evicted files are not in memory
        for key in evicted_keys:
            assert not manager.contains(key), f"Evicted key {key} still in memory"

    def test_priority_protection_from_eviction(self):
        """Test 7: Verify high-priority files are protected from eviction."""
        memory_limit = 512 * 1024  # 512KB
        manager = MemoryManager(memory_limit_bytes=memory_limit)

        high_priority_key = "high_priority_file"
        high_priority_data = bytes(256 * 1024)
        manager.put(high_priority_key, high_priority_data, priority=10)

        for i in range(5):
            key = f"low_priority_{i}"
            data = bytes([i % 256] * (256 * 1024))
            manager.put(key, data, priority=0)

        assert manager.contains(high_priority_key), "High-priority file was evicted"
