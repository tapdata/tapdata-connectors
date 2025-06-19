# StarRocks 连接器文件缓存功能

## 概述

为 StarRocks 连接器添加了本地文件缓存功能，在没有达到配置的刷新大小或刷新时间时，将数据缓存到本地临时文件中，而不是仅保存在内存缓冲区里。这样可以：

1. **避免内存溢出**：处理大量数据时不会占用过多内存
2. **提高稳定性**：即使进程异常，缓存的数据也不会丢失
3. **支持更大的缓冲区**：可以配置更大的刷新阈值而不担心内存问题

## 工作原理

### 1. 缓存文件管理
- **文件位置**：`${java.io.tmpdir}/starrocks-cache/`
- **文件命名**：`starrocks-cache-{threadId}-{timestamp}.tmp`
- **文件格式**：与 StarRocks 流加载格式一致（JSON 或 CSV）

### 2. 数据流程
```
数据事件 → 序列化 → 写入缓存文件 → 达到阈值 → 读取文件 → 发送到 StarRocks → 清理文件
```

### 3. 缓存文件结构
```
{batchStart}
{lineEnd}
{record1}
{lineEnd}
{record2}
{lineEnd}
...
{lineEnd}
{batchEnd}
```

## 核心功能

### 1. 自动文件管理
- **初始化**：每个线程创建独立的缓存文件
- **写入**：数据实时写入文件并刷新到磁盘
- **读取**：刷新时从文件读取所有数据
- **清理**：刷新完成后自动删除文件并重新创建

### 2. 内存优化
- **流式写入**：数据直接写入文件，不在内存中累积
- **缓冲写入**：使用 BufferedWriter 提高写入性能
- **即时刷新**：每次写入后立即刷新到磁盘

### 3. 错误处理
- **写入失败**：记录错误日志并抛出异常
- **文件损坏**：自动重新创建缓存文件
- **清理失败**：记录警告但不影响主流程

## 配置影响

### 刷新大小阈值 (flushSizeMB)
- **小值**：频繁刷新，文件较小，延迟较低
- **大值**：减少刷新次数，文件较大，可能延迟较高

### 刷新时间阈值 (flushTimeoutSeconds)
- **小值**：强制频繁刷新，减少数据延迟
- **大值**：允许更多数据积累，提高吞吐量

## 性能特性

### 1. 磁盘 I/O 优化
- 使用 BufferedWriter 减少系统调用
- 每次写入后立即刷新确保数据持久化
- 顺序写入模式，对 SSD 友好

### 2. 内存使用
- 内存使用量与数据量无关
- 只保留少量元数据（文件路径、大小计数等）
- 避免大数据集导致的内存压力

### 3. 并发安全
- 每个线程使用独立的缓存文件
- 文件名包含线程 ID 避免冲突
- 无需额外的同步机制

## 日志记录

### 文件操作日志
```
INFO  - Initialized cache file: /tmp/starrocks-cache/starrocks-cache-123-1640995200000.tmp
DEBUG - Written 1024 bytes to cache file
DEBUG - Finalized cache file: /tmp/starrocks-cache/starrocks-cache-123-1640995200000.tmp, size: 10.50 MB
DEBUG - Loaded cache file to stream: /tmp/starrocks-cache/starrocks-cache-123-1640995200000.tmp, size: 10.50 MB
DEBUG - Cleaned up cache file: /tmp/starrocks-cache/starrocks-cache-123-1640995200000.tmp, was 10.50 MB in size
```

### 错误处理日志
```
ERROR - Failed to initialize cache file: Permission denied
ERROR - Failed to write to cache file: Disk full
WARN  - Failed to close cache file writer: Stream closed
WARN  - Failed to delete cache file during shutdown: File in use
```

## 监控建议

### 1. 磁盘空间监控
- 监控临时目录的磁盘使用量
- 设置磁盘空间告警阈值
- 定期清理异常残留的缓存文件

### 2. 文件操作监控
- 监控文件创建/删除的频率
- 关注文件大小的增长趋势
- 检查文件操作的错误率

### 3. 性能监控
- 监控文件写入的延迟
- 观察刷新操作的耗时
- 分析磁盘 I/O 的影响

## 故障排除

### 1. 磁盘空间不足
```
ERROR - Failed to write to cache file: No space left on device
```
**解决方案**：
- 清理临时目录
- 减小刷新阈值
- 增加磁盘空间

### 2. 权限问题
```
ERROR - Failed to initialize cache file: Permission denied
```
**解决方案**：
- 检查临时目录权限
- 确保进程有写入权限
- 更改临时目录位置

### 3. 文件锁定
```
WARN - Failed to delete cache file during shutdown: File in use
```
**解决方案**：
- 检查是否有其他进程占用文件
- 重启应用程序
- 手动清理残留文件

## 最佳实践

### 1. 配置优化
- 根据磁盘性能调整刷新阈值
- 在 SSD 上可以设置较大的缓冲区
- 在机械硬盘上建议较小的缓冲区

### 2. 监控设置
- 设置磁盘空间告警（建议 80% 阈值）
- 监控文件操作错误率
- 定期检查临时目录的文件数量

### 3. 维护建议
- 定期清理临时目录
- 监控磁盘 I/O 性能
- 根据业务需求调整配置参数
