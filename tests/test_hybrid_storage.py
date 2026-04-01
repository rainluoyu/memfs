"""
Hybrid storage tests for MemFS.
Tests for hybrid storage management and file location tracking.
"""

import pytest
import time
import tempfile
import shutil
from memfs.storage.hybrid import HybridStorage


class TestHybridStorage:
    """Tests for hybrid storage logic."""

    def test_memory_usage_after_eviction(self):
        """Test memory usage calculation after multiple evictions."""
        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 1 * 1024 * 1024  # 1MB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            file_size = 2 * 1024 * 1024  # 2MB per file
            total_written = 0

            for i in range(5):
                key = f"/file_{i}.txt"
                data = bytes([i % 256] * file_size)
                fs.put(key, data, priority=5)
                total_written += file_size
                time.sleep(0.1)

            fs.worker.wait_all(timeout=10)
            time.sleep(0.5)

            usage = fs.memory.get_usage()
            current_usage = usage["current_usage"]
            actual_memory = sum(
                fs.memory.get_file_info(k)["size"]
                for k in fs.memory.get_all_keys()
                if fs.memory.get_file_info(k)
            )

            assert current_usage == actual_memory, (
                f"Memory usage mismatch: "
                f"_current_usage={current_usage}, "
                f"actual={actual_memory}, "
                f"error={abs(current_usage - actual_memory)} bytes"
            )

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_file_location_tracking_after_eviction(self):
        """Test _file_locations tracking consistency after eviction."""
        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 512 * 1024  # 512KB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            file_size = 256 * 1024  # 256KB
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
                elif location == "real":
                    assert on_disk, f"{key}: location=real but not on disk"
                elif location == "both":
                    assert in_memory and on_disk, (
                        f"{key}: location=both but not in both"
                    )

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_read_evicted_file完整性(self):
        """Test reading evicted files returns correct data."""
        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 512 * 1024  # 512KB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            file_a_key = "/file_a.txt"
            file_a_data = bytes(range(256)) * (2 * 1024)  # 2MB

            fs.put(file_a_key, file_a_data, priority=5)

            for i in range(4):
                key = f"/file_bc_{i}.txt"
                data = bytes([i % 256] * (256 * 1024))
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

    def test_async_write_completion(self):
        """Test async write operations complete correctly."""
        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 2 * 1024 * 1024  # 2MB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            key = "/async_test.txt"
            data = bytes(range(256)) * (1 * 1024)  # 1MB

            fs.put(key, data, priority=5, sync=False)

            fs.worker.wait_all(timeout=10)
            time.sleep(0.5)

            assert fs.real_storage.exists(key), "Async write did not complete"

            disk_data = fs.real_storage.read_sync(key)
            assert disk_data == data, "Disk data does not match"

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_swap_in_after_eviction(self):
        """Test swap-in operation after file was evicted."""
        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 256 * 1024  # 256KB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            file_a_key = "/file_a.txt"
            file_a_data = bytes(range(256)) * (512 * 1024)  # 512KB

            fs.put(file_a_key, file_a_data, priority=5)

            for i in range(3):
                key = f"/file_bc_{i}.txt"
                data = bytes([i % 256] * (256 * 1024))
                fs.put(key, data, priority=5)
                time.sleep(0.1)

            fs.worker.wait_all(timeout=10)
            time.sleep(0.5)

            read_data = fs.get(file_a_key)

            assert read_data is not None, "Swap-in returned None"
            assert read_data == file_a_data, "Swap-in data mismatch"

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_gc_target_usage(self):
        """Test garbage collection achieves target usage."""
        temp_dir = tempfile.mkdtemp()
        try:
            memory_limit = 1 * 1024 * 1024  # 1MB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            for i in range(10):
                key = f"/file_{i}.txt"
                data = bytes([i % 256] * (256 * 1024))
                fs.put(key, data, priority=3)
                time.sleep(0.1)

            fs.worker.wait_all(timeout=10)
            time.sleep(0.5)

            initial_usage = fs.memory.get_usage()["usage_percent"]

            swapped = fs.gc(target_usage=0.3)

            fs.worker.wait_all(timeout=10)
            time.sleep(0.5)

            final_usage = fs.memory.get_usage()["usage_percent"]

            assert final_usage <= 35 or swapped == 0, (
                f"GC did not achieve target: final_usage={final_usage}%"
            )

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_concurrent_put_get(self):
        """Test concurrent put and get operations."""
        temp_dir = tempfile.mkdtemp()
        try:
            import threading

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
                        data = bytes([i % 256] * (64 * 1024))
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

            assert len(errors) == 0, f"Errors occurred: {errors}"

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_file_location_both_state(self):
        """Test 'both' state is correctly set after async write."""
        temp_dir = tempfile.mkdtemp()
        try:
            # Set memory limit high enough to keep file in memory
            memory_limit = 4 * 1024 * 1024  # 4MB
            fs = HybridStorage(
                memory_limit_bytes=memory_limit,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            key = "/both_test.txt"
            # Create 1MB file (smaller than memory limit)
            data = bytes(range(256)) * (4 * 1024)  # 1MB

            fs.put(key, data, priority=5, sync=False)

            # Wait for async write to complete
            fs.worker.wait_all(timeout=10)
            time.sleep(0.5)

            location = fs.get_file_location(key)

            # File should be in both memory and disk after async write completes
            assert location == "both", f"Expected 'both' state, got '{location}'"

            fs.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_persist_mode_file_restoration(self):
        """Test persist mode restores files from disk on startup."""
        temp_dir = tempfile.mkdtemp()
        try:
            key = "/persist_test.txt"
            original_data = bytes(range(256)) * (512 * 1024)

            fs1 = HybridStorage(
                memory_limit_bytes=2 * 1024 * 1024,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            fs1.put(key, original_data, priority=5, sync=True)
            fs1.shutdown(wait=True)

            fs2 = HybridStorage(
                memory_limit_bytes=2 * 1024 * 1024,
                persist_path=temp_dir,
                storage_mode="persist",
                worker_threads=1,
            )

            assert fs2.contains(key), "Persisted file not found on restart"

            fs2.shutdown(wait=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
