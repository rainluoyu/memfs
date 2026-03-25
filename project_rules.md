# MemFS 项目规范

## 一、语言规范
- **注释语言**：中文
- **代码与输出**：英文（变量名、函数名、类名、日志、错误信息等）

## 二、命名规范
- **类名**：大驼峰命名（PascalCase），如 `VirtualFile`, `MemoryManager`
- **函数/方法名**：小写字母 + 下划线（snake_case），如 `set_priority`, `get_stats`
- **变量名**：小写字母 + 下划线（snake_case）
- **常量名**：全大写 + 下划线，如 `DEFAULT_MEMORY_LIMIT`
- **私有成员**：单下划线前缀，如 `_internal_method`

## 三、代码风格
- **缩进**：4 个空格
- **行宽**：最大 100 字符
- **导入顺序**：标准库 → 第三方库 → 本地模块
- **类型注解**：所有公共 API 必须包含类型注解

## 四、抽象策略
- 使用基类定义接口规范
- 核心模块采用策略模式（如压缩算法、换出算法）
- 异步操作统一通过 Worker 线程池处理

## 五、功能地图

### 核心模块
| 功能 | 文件路径 | 类/函数 |
|------|----------|---------|
| 文件系统核心 | `memfs/core/filesystem.py` | `MemFileSystem` |
| 虚拟文件对象 | `memfs/core/file.py` | `VirtualFile` |
| 虚拟目录 | `memfs/core/directory.py` | `VirtualDirectory`, `DirectoryManager` |
| 内存管理 | `memfs/storage/memory.py` | `MemoryManager`, `MemoryFile` |
| 磁盘持久化 | `memfs/storage/disk.py` | `DiskStorage` |
| 混合存储 | `memfs/storage/hybrid.py` | `HybridStorage` |
| LFU 缓存 | `memfs/cache/lfu.py` | `LFUCache` |
| 优先级队列 | `memfs/cache/priority.py` | `PriorityQueue`, `PriorityEntry` |
| 访问追踪 | `memfs/cache/tracker.py` | `AccessTracker`, `FileAccessRecord` |
| 异步工作 | `memfs/async_worker/worker.py` | `AsyncWorker`, `Task`, `TaskType` |
| 原生接口 | `memfs/api/native.py` | `open`, `read`, `write`, `exists` 等 |
| 面向对象接口 | `memfs/api/object.py` | `MemFileSystem` |
| 使用统计 | `memfs/utils/stats.py` | `Statistics`, `MemoryStats`, `DiskStats` |
| 操作日志 | `memfs/utils/logger.py` | `OperationLogger`, `OperationType`, `LogEntry` |
| 压缩工具 | `memfs/utils/compress.py` | `Compressor`, `GzipCompressor`, `Lz4Compressor`, `ZstdCompressor` |

### 压缩算法支持
- `GzipCompressor` - 标准库 gzip，压缩级别 1-9
- `Lz4Compressor` - lz4 库，极速压缩（需安装）
- `ZstdCompressor` - zstandard 库，高压缩率（需安装）
- `CompressionFactory` - 工厂类，统一创建压缩器

## 六、测试规范
- 框架：`pytest`
- 测试目录：`tests/`
- 必须覆盖：核心逻辑、边界条件、异常情况

## 七、临时产物
- 临时文件/目录必须放在 `tmp/` 下
- 任务完成后清理

## 八、Python 版本
- 最低要求：Python 3.8+
- 类型注解使用 `typing` 模块
