# MemFS - Memory-First File System

> **自用项目**：这是一个完全由 AI 构建和维护的 Python 包。不主动维护，如有需要欢迎 fork 后在自己的仓库中修改维护。

**内存优先的虚拟文件系统**

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

[English](README.md) | [中文](readme_cn.md)

## 📖 简介

MemFS 是一个高性能的 Python 内存文件系统，提供虚拟目录结构，将数据优先存储在内存中，同时支持自动持久化到磁盘。具备智能换入换出、文件追踪、优先级管理和异步操作等高级特性。

### 核心特性

- 🚀 **内存优先** - 数据优先保存在内存中，提供微秒级访问速度
- 💾 **自动持久化** - 内存不足时自动换出到磁盘，支持无限存储
- 🧠 **智能换出** - LFU + 优先级混合算法，智能管理文件位置
- ⚡ **异步操作** - 基于线程池的异步换入换出，不阻塞主线程
- 📊 **文件追踪** - 记录每次访问，支持基于使用率的自动优化
- 🎯 **优先级系统** - 0-10 数值优先级，支持手动和自动调整
- 🔌 **双接口** - 同时提供原生风格和面向对象接口

## 📦 安装

```bash
# 安装依赖
pip install -r requirements.txt

# 开发安装
pip install -e .
```

### 依赖说明

| 依赖 | 类型 | 说明 |
|------|------|------|
| Python 3.8+ | 必需 | 最低 Python 版本 |
| psutil | 可选 | 内存和磁盘监控（推荐） |
| pytest | 开发 | 测试框架 |

## 🚀 快速开始

### 1. 原生风格 API

```python
from memfs import write, read, exists, delete, open

# 写入文件
write('/hello.txt', 'Hello, World!')

# 读取文件
content = read('/hello.txt')
print(content.decode('utf-8'))  # Hello, World!

# 检查存在
if exists('/hello.txt'):
    print("文件存在")

# 使用上下文管理器
with open('/data.txt', 'w') as f:
    f.write(b'binary data')

with open('/data.txt', 'r') as f:
    data = f.read()
```

### 2. 面向对象 API

```python
from memfs import MemFileSystem

# 创建文件系统
fs = MemFileSystem(
    memory_limit=0.8,              # 内存限制 80%
    persist_path='./memfs_data',   # 持久化路径
    storage_mode='persist',        # 存储模式：'temp' 或 'persist'
    worker_threads=4               # 工作线程数
)

# 文件操作
fs.write('/file.txt', 'content', priority=5)
content = fs.read('/file.txt')

# 优先级管理
fs.set_priority('/important.txt', priority=9)

# 预热文件
task_id = fs.preload('/next_file.txt')

# 手动垃圾回收
swapped = fs.gc(target_usage=0.5)

# 查看统计
stats = fs.get_stats()
print(f"内存使用：{stats['memory']['usage_percent']:.1f}%")
print(f"缓存命中率：{stats['cache']['hit_rate']:.1f}%")

# 关闭
fs.shutdown()
```

## ⚙️ 配置选项

```python
fs = MemFileSystem(
    memory_limit=0.8,              # 内存使用限制 (0-1)，默认 0.8
    persist_path='./memfs_data',   # 持久化存储路径，默认 './memfs_data'
    storage_mode='temp',           # 存储模式：'temp' 或 'persist'，默认 'temp'
    worker_threads=4,              # 后台工作线程数，默认 4
    enable_logging=True,           # 启用操作日志，默认 True
    log_path='logs/memfs.log',     # 日志文件路径，默认 None
    priority_boost_threshold=10    # 自动提升优先级的访问次数，默认 10
)
```

### 配置参数详解

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `memory_limit` | float | 0.8 | 内存使用限制，范围 0-1，表示系统内存的百分比 |
| `persist_path` | str | './memfs_data' | 磁盘持久化存储的基础路径 |
| `storage_mode` | str | 'temp' | 存储模式：`'temp'`（临时，关闭时清理）或 `'persist'`（关闭后保留文件） |
| `worker_threads` | int | 4 | 后台工作线程数量，影响并发换入换出性能 |
| `enable_logging` | bool | True | 是否启用操作日志记录 |
| `log_path` | str | None | 操作日志文件路径，None 表示仅内存存储 |
| `priority_boost_threshold` | int | 10 | 文件访问达到此次数后自动提升优先级 |

## 🎯 优先级系统

优先级范围：**0-10**，数值越高越重要

| 优先级 | 级别 | 换出策略 | 适用场景 |
|--------|------|----------|----------|
| 0-2 | 低优先级 | 优先换出 | 临时文件、缓存数据 |
| 3-5 | 普通优先级 | 正常换出 | 一般文件、配置文件 |
| 6-8 | 高优先级 | 尽量保留 | 重要数据、频繁访问文件 |
| 9-10 | 锁定 | 除非手动 GC，否则不换出 | 核心数据、系统文件 |

### 优先级使用示例

```python
from memfs import MemFileSystem

fs = MemFileSystem()

# 写入时设置优先级
fs.write('/temp.txt', 'temporary data', priority=1)      # 低优先级
fs.write('/config.txt', 'config data', priority=5)       # 普通优先级
fs.write('/core.db', b'important data', priority=9)      # 高优先级

# 动态调整优先级
fs.set_priority('/temp.txt', priority=3)  # 提升优先级
fs.set_priority('/core.db', priority=10)  # 锁定文件

# 获取优先级
priority = fs.get_priority('/core.db')  # 返回 10
```

## 📚 API 参考

### 文件操作

#### `open(path, mode='rb', priority=5)`
打开虚拟文件，返回文件对象。

```python
# 写入
with fs.open('/test.txt', 'w') as f:
    f.write('hello')

# 读取
with fs.open('/test.txt', 'r') as f:
    content = f.read()

# 二进制
with fs.open('/data.bin', 'wb') as f:
    f.write(b'\x00\x01\x02')
```

#### `read(path, priority=None)`
读取整个文件内容。

```python
data = fs.read('/file.txt')  # 返回 bytes
text = fs.read('/file.txt').decode('utf-8')
```

#### `write(path, data, priority=5)`
写入整个文件。

```python
# 写入字符串
fs.write('/file.txt', 'hello world', priority=5)

# 写入字节
fs.write('/data.bin', b'\x00\x01\x02', priority=5)
```

#### `exists(path)`
检查文件是否存在。

```python
if fs.exists('/file.txt'):
    print("文件存在")
```

#### `delete(path)`
删除文件。

```python
fs.delete('/file.txt')
```

### 目录操作

#### `mkdir(path)`
创建目录。

```python
fs.mkdir('/subdir')
fs.mkdir('/nested/deep/path')  # 自动创建父目录
```

#### `rmdir(path)`
删除空目录。

```python
fs.rmdir('/empty_dir')
```

#### `listdir(path='/')`
列出目录内容。

```python
items = fs.listdir('/')
print(items)  # ['file1.txt', 'subdir', 'file2.txt']
```

#### `glob(pattern)`
使用 glob 模式匹配文件。

```python
# 匹配所有 txt 文件
txt_files = fs.glob('/*.txt')

# 递归匹配
all_py = fs.glob('/**/*.py')

# 模式匹配
data_files = fs.glob('/data_*.csv')
```

### 高级功能

#### `set_priority(path, priority)`
设置文件优先级。

```python
fs.set_priority('/important.txt', priority=9)
```

#### `get_priority(path)`
获取文件优先级。

```python
priority = fs.get_priority('/file.txt')
```

#### `preload(path, priority=5)`
预加载文件到内存。

```python
# 异步预加载
task_id = fs.preload('/large_file.bin', priority=7)
```

#### `gc(target_usage=0.5)`
触发垃圾回收，将文件换出到磁盘。

```python
# 换出文件直到内存使用率降至 50%
swapped_count = fs.gc(target_usage=0.5)
print(f"换出了 {swapped_count} 个文件")
```

#### `get_stats()`
获取文件系统统计信息。

```python
stats = fs.get_stats()
print(f"内存使用：{stats['memory']['usage_percent']:.1f}%")
print(f"文件数量：{stats['memory']['file_count']}")
print(f"缓存命中：{stats['cache']['hit_rate']:.1f}%")
print(f"换入次数：{stats['cache']['swaps_in']}")
print(f"换出次数：{stats['cache']['swaps_out']}")
```

#### `get_file_info(path)`
获取文件详细信息。

```python
info = fs.get_file_info('/file.txt')
print(info)
# {
#     'path': '/file.txt',
#     'location': 'memory',  # 或 'disk'
#     'priority': 5,
#     'size': 1024,
#     'access_count': 10,
#     ...
# }
```

## 📊 统计信息结构

`get_stats()` 返回的统计信息包含以下部分：

```python
{
    'uptime_seconds': 3600.5,          # 运行时长
    'memory': {
        'current_usage': 1048576,      # 当前内存使用 (字节)
        'current_usage_mb': 1.0,       # 当前内存使用 (MB)
        'peak_usage': 2097152,         # 峰值内存使用 (字节)
        'peak_usage_mb': 2.0,          # 峰值内存使用 (MB)
        'limit': 8589934592,           # 内存限制 (字节)
        'limit_mb': 8192.0,            # 内存限制 (MB)
        'usage_percent': 12.5,         # 使用百分比
        'file_count': 100,             # 内存中的文件数
        'total_size': 1048576          # 总文件大小
    },
    'disk': {
        'path': './memfs_data',        # 存储路径
        'current_usage': 10485760,     # 磁盘使用 (字节)
        'current_usage_mb': 10.0,      # 磁盘使用 (MB)
        'total_capacity': 1073741824,  # 总容量 (字节)
        'total_capacity_mb': 1024.0,   # 总容量 (MB)
        'usage_percent': 1.0,          # 使用百分比
        'file_count': 500              # 磁盘上的文件数
    },
    'cache': {
        'hits': 1000,                  # 缓存命中次数
        'misses': 50,                  # 缓存缺失次数
        'hit_rate': 95.2,              # 命中率 (%)
        'swaps_in': 45,                # 换入次数
        'swaps_out': 50,               # 换出次数
        'preloads': 10,                # 预加载次数
        'evictions': 50                # 驱逐次数
    },
    'operations': {
        'total_operations': 5000,      # 总操作数
        'read_count': 3000,            # 读取次数
        'write_count': 1500,           # 写入次数
        'delete_count': 500,           # 删除次数
        'avg_read_time_ms': 0.5,       # 平均读取时间 (ms)
        'avg_write_time_ms': 1.2       # 平均写入时间 (ms)
    }
}
```

## 🔧 使用场景

### 1. 临时文件存储
```python
fs = MemFileSystem(memory_limit=0.3)

# 临时数据处理
fs.write('/temp/process_1.dat', large_data, priority=2)
# 内存不足时自动换出
```

### 2. 缓存层
```python
fs = MemFileSystem(memory_limit=0.5)

# 缓存热点数据
def get_data(key):
    if fs.exists(f'/cache/{key}'):
        return fs.read(f'/cache/{key}')
    data = expensive_compute(key)
    fs.write(f'/cache/{key}', data, priority=7)
    return data
```

### 3. 大文件处理
```python
fs = MemFileSystem(memory_limit=0.8)

# 处理大文件，自动换出
for i in range(100):
    fs.write(f'/batch/file_{i}.bin', generate_data(i), priority=3)
    if i % 10 == 0:
        fs.gc(target_usage=0.6)  # 定期 GC
```

### 4. 数据预热
```python
fs = MemFileSystem()

# 预加载即将使用的文件
for next_file in upcoming_files:
    fs.preload(f'/data/{next_file}', priority=6)

# 后台加载，使用时已在内存
```

## ⚠️ 注意事项

1. **线程安全**：所有操作都是线程安全的，可以在多线程环境中使用
2. **资源清理**：使用完毕后调用 `shutdown()` 或 `close()` 释放资源
3. **路径格式**：所有路径以 `/` 为根目录（Linux 风格路径，自动转换 Windows 反斜杠）
4. **内存限制**：合理设置 `memory_limit`，避免占用过多系统内存
5. **存储模式**：
   - `'temp'`：临时存储，关闭时删除所有文件（默认）
   - `'persist'`：持久化存储，关闭后保留文件

## 🧪 运行测试

```bash
# 运行所有测试
pytest tests/ -v

# 运行测试并查看覆盖率
pytest tests/ --cov=memfs --cov-report=html

# 运行特定测试
pytest tests/test_core.py::TestMemFileSystem::test_write_read -v
```

## 📝 示例代码

查看 `examples.py` 获取更多使用示例：

```bash
python examples.py
```

## 📄 许可证

MIT License
