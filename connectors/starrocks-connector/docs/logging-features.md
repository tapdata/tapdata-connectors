# StarRocks 连接器日志功能

## 概述

为 StarRocks 连接器添加了详细的日志记录功能，用于监控数据处理和刷新过程的详细信息。

## 日志功能

### 1. 每批事件处理日志

每批事件处理完成后，会记录以下信息：

```
Batch processed: events=100, batch_data_size=2.50 MB, accumulated_buffer_size=15.75 MB, 
flush_size_config=100 MB, flush_timeout_config=300 seconds, waiting_time=45000 ms
```

**包含信息：**
- `events`: 本批处理的事件数量
- `batch_data_size`: 本批数据的大小（格式化显示）
- `accumulated_buffer_size`: 当前累积的缓冲区数据大小
- `flush_size_config`: 配置的刷新大小阈值（MB）
- `flush_timeout_config`: 配置的强制刷新时间（秒）
- `waiting_time`: 距离上次刷新的等待时间（毫秒）

### 2. 刷新触发日志

当触发刷新时，会记录触发原因和详细信息：

#### 大小阈值触发
```
Flush triggered by size_threshold: current_size=102.50 MB, size_threshold=100.00 MB, 
waiting_time=45000 ms, time_threshold=300000 ms
```

#### 时间阈值触发
```
Flush triggered by time_threshold: current_size=50.25 MB, size_threshold=100.00 MB, 
waiting_time=305000 ms, time_threshold=300000 ms
```

#### 其他原因触发
```
Flush triggered by data_columns_changed: current_size=75.30 MB
Flush triggered by buffer_full: current_size=256.00 KB
```

### 3. 刷新完成日志

每次刷新完成后，会记录详细的性能信息：

#### 成功刷新
```
Flush completed: flushed_size=102.50 MB, waiting_time=45000 ms, 
flush_duration=2500 ms, response=RespContent{status='Success', ...}
```

#### 失败刷新
```
Flush failed: flushed_size=102.50 MB, waiting_time=45000 ms, 
flush_duration=5000 ms, error=Connection timeout
```

**包含信息：**
- `flushed_size`: 刷新的数据大小（格式化显示）
- `waiting_time`: 等待时间（从上次刷新到本次刷新开始）
- `flush_duration`: 刷新操作耗时
- `response/error`: 刷新结果或错误信息

### 4. 每小时限制日志

当触发每小时写入限制时：

```
Hourly write limit exceeded. Current hour written: 50.25 GB, limit: 50.00 GB. 
Will wait 25 minutes until next hour.
```

## 数据格式化

所有数据大小都使用易读的格式显示：
- 小于 1KB: `512 B`
- 1KB - 1MB: `1.50 KB`
- 1MB - 1GB: `102.50 MB`
- 大于 1GB: `2.30 GB`

## 日志级别

- **INFO**: 批处理状态、刷新触发、刷新完成
- **WARN**: 每小时限制超出
- **ERROR**: 刷新失败
- **DEBUG**: 详细的内部状态信息

## 监控建议

### 性能监控
1. 观察 `flush_duration` 来监控刷新性能
2. 监控 `waiting_time` 来了解数据延迟
3. 关注 `batch_data_size` 来了解数据处理量

### 配置优化
1. 如果 `flush_duration` 过长，考虑减小 `flushSizeMB`
2. 如果 `waiting_time` 过长，考虑减小 `flushTimeoutSeconds`
3. 根据 `accumulated_buffer_size` 的增长模式调整配置

### 问题诊断
1. 频繁的 `size_threshold` 触发可能需要增大缓冲区
2. 频繁的 `time_threshold` 触发可能需要增大超时时间
3. `buffer_full` 触发说明原有缓冲区配置可能过小

## 示例日志序列

```
2024-01-15 10:30:15 INFO  - Batch processed: events=50, batch_data_size=1.25 MB, 
                           accumulated_buffer_size=25.50 MB, flush_size_config=100 MB, 
                           flush_timeout_config=300 seconds, waiting_time=15000 ms

2024-01-15 10:35:20 INFO  - Flush triggered by time_threshold: current_size=45.75 MB, 
                           size_threshold=100.00 MB, waiting_time=305000 ms, 
                           time_threshold=300000 ms

2024-01-15 10:35:22 INFO  - Flush completed: flushed_size=45.75 MB, waiting_time=305000 ms, 
                           flush_duration=2000 ms, response=RespContent{status='Success'}
```

这些日志信息可以帮助运维人员：
- 实时监控数据处理状态
- 优化配置参数
- 快速定位性能问题
- 分析数据处理模式
