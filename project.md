# MemFS 项目开发规范

---

## a) 语言规范

- **代码注释**：必须使用中文
- **标识符命名**：变量、函数、类名使用英文
- **控制台输出/日志**：使用英文
- **文档**：Markdown 等文档以中文为主

---

## b) 代码变量命名

- **局部变量**：使用小写 + 下划线（snake_case），如 `file_path`, `data_size`
- **全局变量**：使用小写 + 下划线，带模块前缀，如 `_global_fs`
- **常量**：使用大写 + 下划线，如 `DEFAULT_MEMORY_LIMIT`, `MAX_PRIORITY`
- **类名**：使用大驼峰命名（PascalCase），如 `MemFileSystem`, `VirtualFile`
- **函数名**：使用小写 + 下划线，如 `get_file_info`, `set_priority`
- **私有成员**：使用单下划线前缀，如 `_file_locations`, `_pending_ops`
- **特殊方法**：使用双下划线，如 `__init__`, `__enter__`, `__exit__`

---

## c) 代码结构

### 架构分层

```
memfs/
├── api/              # 对外接口层（双接口设计）
│   ├── native.py     # 原生风格 API（write, read, open 等）
│   └── object.py     # 面向对象 API（MemFileSystem 类）
├── core/             # 核心逻辑层
│   ├── filesystem.py # 主文件系统类
│   ├── file.py       # 虚拟文件实现
│   └── directory.py  # 虚拟目录管理
├── storage/          # 存储层
│   ├── hybrid.py     # 混合存储（内存 + 磁盘）
│   ├── memory.py     # 内存管理器
│   ├── real_path.py  # 磁盘路径存储
│   └── lock_manager.py # 文件锁管理
├── cache/            # 缓存与优化层
│   ├── lfu.py        # LFU 缓存算法
│   ├── priority.py   # 优先级队列
│   └── tracker.py    # 访问追踪器
├── async_worker/     # 异步工作层
│   └── worker.py     # 线程池任务执行
├── utils/            # 工具层
│   ├── stats.py      # 统计信息收集
│   └── logger.py     # 操作日志记录
└── config/           # 配置层
    └── __init__.py   # 配置常量定义
```

### 设计原则

- **高内聚**：每个模块职责单一，如 `MemoryManager` 仅管理内存
- **低耦合**：层与层之间通过接口交互，如 `HybridStorage` 组合使用各子模块
- **线程安全**：所有公共方法使用锁保护，支持并发访问
- **异步操作**：耗时操作（磁盘 IO、换入换出）通过 `AsyncWorker` 异步执行

---

## d) 测试规范

### 临时测试
- 放置位置：`tmp/` 目录
- 用途：快速验证、调试代码
- 清理：验证后立即删除

### 持久化单元测试
- 放置位置：`tests/` 目录
- 命名规范：`test_*.py`
- 测试框架：pytest
- 覆盖率要求：核心模块 >80%

### 运行测试命令
```bash
# 运行所有测试
pytest tests/ -v

# 运行测试并查看覆盖率
pytest tests/ --cov=memfs --cov-report=html

# 运行特定测试
pytest tests/test_core.py::TestMemFileSystem::test_write_read -v
```

---

## e) 项目代码地图

### 打包配置文件

| 文件 | 功能说明 |
|------|----------|
| `pyproject.toml` | Python 包配置文件（版本 0.1.0，MIT License，作者 Luoyu） |
| `MANIFEST.in` | 指定分发包包含的文件（README、LICENSE、examples.py、tests/） |
| `LICENSE` | MIT 许可证文件 |

### 文档文件

| 文件 | 功能说明 |
|------|----------|
| `README.md` | 项目英文文档（包含中英文语言切换链接） |
| `readme_cn.md` | 项目中文文档（包含中英文语言切换链接） |

### 核心类与函数

| 文件 | 核心类/函数 | 功能说明 |
|------|-------------|----------|
| `memfs/__init__.py` | 模块导出 | 导出所有公共 API |
| `memfs/api/native.py` | `init()`, `write()`, `read()`, `exists()`, `delete()`, `open()`, `mkdir()`, `rmdir()`, `listdir()`, `glob()`, `set_priority()`, `get_priority()`, `preload()`, `gc()`, `get_stats()`, `get_file_info()`, `get_memory_map()` | 原生风格 API |
| `memfs/api/object.py` | `MemFileSystem`（重导出） | 面向对象 API 入口 |
| `memfs/core/filesystem.py` | `MemFileSystem`, `_normalize_path()`, `shutdown_async()`, `get_memory_map()`, `_on_swap_callback()` | 主文件系统类，协调所有操作；路径规范化；异步关闭；内存地图查询；swap 事件回调 |
| `memfs/core/file.py` | `VirtualFile` | 虚拟文件对象，支持文件流操作 |
| `memfs/core/directory.py` | `VirtualDirectory`, `DirectoryManager` | 虚拟目录树管理，使用 RLock 防止死锁 |
| `memfs/storage/hybrid.py` | `HybridStorage`, `ExternalModificationError`, `_sync_persisted_files_to_directories()` | 混合存储管理器，自动内存/磁盘 tiering；持久化模式下同步磁盘文件到目录树 |
| `memfs/storage/memory.py` | `MemoryManager`, `MemoryFile` | 内存管理器，带 eviction 策略 |
| `memfs/storage/real_path.py` | `RealPathStorage`, `_check_path_safety()` | 磁盘路径存储，1:1 映射虚拟路径；临时模式下的路径安全检查 |
| `memfs/storage/lock_manager.py` | `FileLockManager` | 文件读写锁管理 |
| `memfs/cache/lfu.py` | `LFUCache` | LFU 缓存算法实现 |
| `memfs/cache/priority.py` | `PriorityQueue`, `PriorityEntry` | 优先级队列，用于 eviction 决策 |
| `memfs/cache/tracker.py` | `AccessTracker`, `FileAccessRecord` | 访问频率追踪 |
| `memfs/async_worker/worker.py` | `AsyncWorker`, `Task`, `TaskResult`, `TaskType`, `_atexit_cleanup()` | 异步任务执行器；Python 退出时清理和警告 |
| `memfs/utils/stats.py` | `Statistics`, `MemoryStats`, `DiskStats`, `CacheStats`, `OperationStats` | 统计信息收集器 |
| `memfs/utils/logger.py` | `OperationLogger`, `OperationType`, `LogEntry` | 操作日志记录器（文件日志） |
### Debug 功能

| 函数/模块 | 功能说明 |
|-----------|----------|
| `memfs.core.filesystem.logger` | memfs 专用 logger，使用 `logging.getLogger("memfs")` |
| `get_file_info()` | 返回文件信息，新增 `in_memory` 字段表示文件是否在内存中 |
| `get_memory_map()` | 返回所有缓存文件的内存地图，包含位置、大小、优先级等信息 |
| `_on_swap_callback()` | swap 事件回调，在文件换入换出时自动打印 debug 日志和内存地图 |

**Debug 日志启用方式**：
```python
import logging
logging.basicConfig(level=logging.DEBUG)  # 设置全局 logging 级别为 DEBUG
import memfs  # memfs 的 debug 日志将自动输出到全局配置的目标
```

### 核心数据结构

- **文件位置追踪**：`_file_locations: Dict[str, str]` - 记录文件在 memory/real
- **优先级映射**：`_file_priorities: Dict[str, int]` - 记录文件优先级 (0-10)
- **访问计数**：`_access_counts: Dict[str, int]` - 记录访问次数用于自动优先级提升

### 配置常量

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `memory_limit` | 0.8 | 内存使用限制 (0-1) |
| `persist_path` | './memfs_data' | 持久化/临时目录路径 |
| `storage_mode` | 'temp' | 存储模式：'temp'（临时，关闭时清理）或 'persist'（持久化） |
| `worker_threads` | 4 | 后台工作线程数 |
| `priority_boost_threshold` | 10 | 自动提升优先级的访问次数 |
| 路径前缀 | '/' | 虚拟路径根目录（统一使用 Linux 风格路径） |

### 测试覆盖

- **核心测试**：`tests/test_core.py` - 131 个测试用例，覆盖所有核心模块
- **RealPath 测试**：`tests/test_real_path.py` - 21 个测试用例，包含持久化存储、文件锁、异步操作、持久化模式 listdir、临时模式路径安全检查测试
- **测试框架**：pytest
- **覆盖率**：核心模块 >80%

---

**最后更新**：2026-03-31  
**版本**：0.2.2

---

## 更新日志

### v0.2.2 (2026-03-31)
- 添加 `get_memory_map()` 函数，返回所有缓存文件的内存地图
- `get_file_info()` 新增 `in_memory` 字段，表示文件是否在内存中
- 添加 memfs 专用 logger（`logging.getLogger("memfs")`），支持全局 logging 级别控制
- 文件换入换出时自动打印 debug 日志和内存地图
- 更新 project.md 代码地图

### v0.2.1 (2026-03-28)
- 删除压缩相关功能（compression、compression_level 参数已移除）
- 添加 `storage_mode` 参数（'temp'/'persist'）
- 添加 `clear_persist()` 函数用于清理持久化数据
- 更新 README 文档，删除压缩相关内容
- 添加中英文语言切换链接
- 项目说明更新为自用项目，不主动维护
