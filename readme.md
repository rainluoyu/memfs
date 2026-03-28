# MemFS - Memory-First File System

> Personal use: This is a Python package built entirely by AI

**Memory-First Virtual File System**

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

[English](README.md) | [中文](readme_cn.md)

## 📖 Introduction

MemFS is a high-performance Python memory file system that provides a virtual directory structure, prioritizes storing data in memory, and supports automatic persistence to disk. It features advanced capabilities such as intelligent swap-in/swap-out, file tracking, priority management, and asynchronous operations.

### Core Features

- 🚀 **Memory-First** - Data is kept in memory first, providing microsecond-level access speeds
- 💾 **Auto Persistence** - Automatically swaps out to disk when memory is insufficient, supporting unlimited storage
- 🧠 **Intelligent Swap-Out** - LFU + Priority hybrid algorithm for intelligent file location management
- ⚡ **Async Operations** - Thread pool-based asynchronous swap-in/swap-out without blocking the main thread
- 📊 **File Tracking** - Records every access, supporting usage-based automatic optimization
- 🎯 **Priority System** - 0-10 numerical priority levels with manual and automatic adjustment support
- 🔌 **Dual Interface** - Provides both native-style and object-oriented interfaces simultaneously

## 📦 Installation

```bash
# Basic installation (standard library only)
pip install -e .

# Full installation (all compression algorithms)
pip install -e ".[full]"

# Development installation
pip install -e ".[dev]"
```

### Dependencies

| Dependency | Type | Description |
|------------|------|-------------|
| Python 3.8+ | Required | Minimum Python version |
| psutil | Optional | Memory and disk monitoring (recommended) |
| pytest | Development | Testing framework |

## 🚀 Quick Start

### 1. Native-Style API

```python
from memfs import write, read, exists, delete, open

# Write file
write('/hello.txt', 'Hello, World!')

# Read file
content = read('/hello.txt')
print(content.decode('utf-8'))  # Hello, World!

# Check existence
if exists('/hello.txt'):
    print("File exists")

# Using context manager
with open('/data.txt', 'w') as f:
    f.write(b'binary data')

with open('/data.txt', 'r') as f:
    data = f.read()
```

### 2. Object-Oriented API

```python
from memfs import MemFileSystem

# Create file system
fs = MemFileSystem(
    memory_limit=0.8,           # Memory limit 80%
    persist_path='./memfs_data', # Persistence path
    compression='gzip',         # Compression algorithm
    worker_threads=4            # Worker thread count
)

# File operations
fs.write('/file.txt', 'content', priority=5)
content = fs.read('/file.txt')

# Priority management
fs.set_priority('/important.txt', priority=9)

# Preload file
task_id = fs.preload('/next_file.txt')

# Manual garbage collection
swapped = fs.gc(target_usage=0.5)

# View statistics
stats = fs.get_stats()
print(f"Memory usage: {stats['memory']['usage_percent']:.1f}%")
print(f"Cache hit rate: {stats['cache']['hit_rate']:.1f}%")

# Shutdown
fs.shutdown()
```

## ⚙️ Configuration Options

```python
fs = MemFileSystem(
    memory_limit=0.8,              # Memory usage limit (0-1), default 0.8
    persist_path='./memfs_data',   # Persistent storage path, default './memfs_data'
    compression='gzip',            # Compression: none/gzip/lz4/zstd, default 'gzip'
    compression_level=6,           # Compression level (1-9), default 6
    worker_threads=4,              # Background worker threads, default 4
    enable_logging=True,           # Enable operation logging, default True
    log_path='logs/memfs.log',     # Log file path, default None
    priority_boost_threshold=10    # Access count for auto priority boost, default 10
)
```

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `memory_limit` | float | 0.8 | Memory usage limit, range 0-1, percentage of system memory |
| `persist_path` | str | './memfs_data' | Base path for disk persistent storage |
| `compression` | str | 'gzip' | Compression algorithm: `none`/`gzip`/`lz4`/`zstd` |
| `compression_level` | int | 6 | Compression level, range 1-9, higher means better compression but slower |
| `worker_threads` | int | 4 | Number of background worker threads, affects concurrent swap-in/out performance |
| `enable_logging` | bool | True | Whether to enable operation logging |
| `log_path` | str | None | Operation log file path, None means in-memory storage only |
| `priority_boost_threshold` | int | 10 | Auto priority boost after file access reaches this count |

## 🎯 Priority System

Priority range: **0-10**, higher values mean more important

| Priority | Level | Swap-Out Strategy | Use Cases |
|----------|-------|-------------------|-----------|
| 0-2 | Low Priority | Priority swap-out | Temporary files, cache data |
| 3-5 | Normal Priority | Normal swap-out | General files, configuration files |
| 6-8 | High Priority | Keep as much as possible | Important data, frequently accessed files |
| 9-10 | Locked | No swap-out unless manual GC | Core data, system files |

### Priority Usage Examples

```python
from memfs import MemFileSystem

fs = MemFileSystem()

# Set priority on write
fs.write('/temp.txt', 'temporary data', priority=1)      # Low priority
fs.write('/config.txt', 'config data', priority=5)       # Normal priority
fs.write('/core.db', b'important data', priority=9)      # High priority

# Dynamically adjust priority
fs.set_priority('/temp.txt', priority=3)  # Boost priority
fs.set_priority('/core.db', priority=10)  # Lock file

# Get priority
priority = fs.get_priority('/core.db')  # Returns 10
```

## 📚 API Reference

### File Operations

#### `open(path, mode='rb', priority=5)`
Open a virtual file and return a file object.

```python
# Write
with fs.open('/test.txt', 'w') as f:
    f.write('hello')

# Read
with fs.open('/test.txt', 'r') as f:
    content = f.read()

# Binary
with fs.open('/data.bin', 'wb') as f:
    f.write(b'\x00\x01\x02')
```

#### `read(path, priority=None)`
Read the entire file content.

```python
data = fs.read('/file.txt')  # Returns bytes
text = fs.read('/file.txt').decode('utf-8')
```

#### `write(path, data, priority=5)`
Write an entire file.

```python
# Write string
fs.write('/file.txt', 'hello world', priority=5)

# Write bytes
fs.write('/data.bin', b'\x00\x01\x02', priority=5)
```

#### `exists(path)`
Check if a file exists.

```python
if fs.exists('/file.txt'):
    print("File exists")
```

#### `delete(path)`
Delete a file.

```python
fs.delete('/file.txt')
```

### Directory Operations

#### `mkdir(path)`
Create a directory.

```python
fs.mkdir('/subdir')
fs.mkdir('/nested/deep/path')  # Auto-create parent directories
```

#### `rmdir(path)`
Delete an empty directory.

```python
fs.rmdir('/empty_dir')
```

#### `listdir(path='/')`
List directory contents.

```python
items = fs.listdir('/')
print(items)  # ['file1.txt', 'subdir', 'file2.txt']
```

#### `glob(pattern)`
Match files using glob patterns.

```python
# Match all txt files
txt_files = fs.glob('/*.txt')

# Recursive match
all_py = fs.glob('/**/*.py')

# Pattern match
data_files = fs.glob('/data_*.csv')
```

### Advanced Features

#### `set_priority(path, priority)`
Set file priority.

```python
fs.set_priority('/important.txt', priority=9)
```

#### `get_priority(path)`
Get file priority.

```python
priority = fs.get_priority('/file.txt')
```

#### `preload(path, priority=5)`
Preload a file into memory.

```python
# Async preload
task_id = fs.preload('/large_file.bin', priority=7)
```

#### `gc(target_usage=0.5)`
Trigger garbage collection to swap files out to disk.

```python
# Swap out files until memory usage drops to 50%
swapped_count = fs.gc(target_usage=0.5)
print(f"Swapped out {swapped_count} files")
```

#### `get_stats()`
Get file system statistics.

```python
stats = fs.get_stats()
print(f"Memory usage: {stats['memory']['usage_percent']:.1f}%")
print(f"File count: {stats['memory']['file_count']}")
print(f"Cache hits: {stats['cache']['hit_rate']:.1f}%")
print(f"Swaps in: {stats['cache']['swaps_in']}")
print(f"Swaps out: {stats['cache']['swaps_out']}")
```

#### `get_file_info(path)`
Get detailed file information.

```python
info = fs.get_file_info('/file.txt')
print(info)
# {
#     'path': '/file.txt',
#     'location': 'memory',  # or 'disk'
#     'priority': 5,
#     'size': 1024,
#     'access_count': 10,
#     ...
# }
```

## 📊 Statistics Structure

The statistics returned by `get_stats()` include the following sections:

```python
{
    'uptime_seconds': 3600.5,          # Uptime
    'memory': {
        'current_usage': 1048576,      # Current memory usage (bytes)
        'current_usage_mb': 1.0,       # Current memory usage (MB)
        'peak_usage': 2097152,         # Peak memory usage (bytes)
        'peak_usage_mb': 2.0,          # Peak memory usage (MB)
        'limit': 8589934592,           # Memory limit (bytes)
        'limit_mb': 8192.0,            # Memory limit (MB)
        'usage_percent': 12.5,         # Usage percentage
        'file_count': 100,             # Number of files in memory
        'total_size': 1048576          # Total file size
    },
    'disk': {
        'path': './memfs_data',        # Storage path
        'current_usage': 10485760,     # Disk usage (bytes)
        'current_usage_mb': 10.0,      # Disk usage (MB)
        'total_capacity': 1073741824,  # Total capacity (bytes)
        'total_capacity_mb': 1024.0,   # Total capacity (MB)
        'usage_percent': 1.0,          # Usage percentage
        'file_count': 500              # Number of files on disk
    },
    'cache': {
        'hits': 1000,                  # Cache hits
        'misses': 50,                  # Cache misses
        'hit_rate': 95.2,              # Hit rate (%)
        'swaps_in': 45,                # Swap-in count
        'swaps_out': 50,               # Swap-out count
        'preloads': 10,                # Preload count
        'evictions': 50                # Eviction count
    },
    'operations': {
        'total_operations': 5000,      # Total operations
        'read_count': 3000,            # Read count
        'write_count': 1500,           # Write count
        'delete_count': 500,           # Delete count
        'avg_read_time_ms': 0.5,       # Average read time (ms)
        'avg_write_time_ms': 1.2       # Average write time (ms)
    }
}
```

## 🔧 Use Cases

### 1. Temporary File Storage
```python
fs = MemFileSystem(memory_limit=0.3)

# Temporary data processing
fs.write('/temp/process_1.dat', large_data, priority=2)
# Auto swap-out when memory is insufficient
```

### 2. Cache Layer
```python
fs = MemFileSystem(memory_limit=0.5, compression='lz4')

# Cache hot data
def get_data(key):
    if fs.exists(f'/cache/{key}'):
        return fs.read(f'/cache/{key}')
    data = expensive_compute(key)
    fs.write(f'/cache/{key}', data, priority=7)
    return data
```

### 3. Large File Processing
```python
fs = MemFileSystem(memory_limit=0.8)

# Process large files with auto swap-out
for i in range(100):
    fs.write(f'/batch/file_{i}.bin', generate_data(i), priority=3)
    if i % 10 == 0:
        fs.gc(target_usage=0.6)  # Periodic GC
```

### 4. Data Preloading
```python
fs = MemFileSystem()

# Preload upcoming files
for next_file in upcoming_files:
    fs.preload(f'/data/{next_file}', priority=6)

# Load in background, already in memory when needed
```

## ⚠️ Notes

1. **Thread-Safe**: All operations are thread-safe and can be used in multi-threaded environments
2. **Resource Cleanup**: Call `shutdown()` or `close()` after use to release resources
3. **Path Format**: All paths use `/` as the root directory
4. **Memory Limit**: Set `memory_limit` appropriately to avoid consuming excessive system memory
5. **Compression Options**:
   - `gzip`: Balanced performance and compression ratio (recommended)
   - `lz4`: Extreme speed, lower compression ratio
   - `zstd`: High compression ratio, moderate speed

## 🧪 Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run tests with coverage
pytest tests/ --cov=memfs --cov-report=html

# Run specific test
pytest tests/test_core.py::TestMemFileSystem::test_write_read -v
```

## 📝 Example Code

See `examples.py` for more usage examples:

```bash
python examples.py
```

## 📄 Project Note

**This is a personal-use project, built and maintained entirely by AI.**

- **Maintenance**: This project is not actively maintained. Updates are made as needed for personal use.
- **Contributions**: Please do not submit issues or pull requests. If you find this useful, feel free to fork it and make modifications in your own repository.
- **Support**: No official support is provided. Use at your own risk.

## 📄 License

MIT License
