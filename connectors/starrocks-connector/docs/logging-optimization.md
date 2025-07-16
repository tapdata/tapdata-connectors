# StarRocks 连接器日志优化

## 优化目标

减少日志噪音，提供更有意义的监控信息，避免每批事件都打印状态日志。

## 优化前的问题

### 1. 日志过于频繁
```java
// 每批事件处理后都会打印
TapLogger.info(TAG, "Batch processed: events={}, batch_data_size={}, ...");
```

**问题**：
- 高频数据处理时产生大量日志
- 日志文件快速增长
- 重要信息被淹没在大量重复日志中
- 影响系统性能

### 2. 信息重复度高
每批处理的日志格式相同，大部分信息变化不大，造成信息冗余。

## 优化方案

### 1. 时间间隔控制
```java
private long lastLogTime;
private static final long LOG_INTERVAL_MS = 30 * 1000; // 30秒

// 检查是否需要打印状态日志（每30秒一次）
if (currentTime - lastLogTime >= LOG_INTERVAL_MS) {
    logCurrentStatus(processedEvents, batchDataSize, currentTime);
    lastLogTime = currentTime;
}
```

### 2. 关键事件触发
在以下情况下立即打印日志：
- **刷新触发时**：无论是否到达30秒间隔
- **阈值达到时**：大小或时间阈值触发刷新
- **异常情况时**：数据列变化或缓冲区满

### 3. 避免重复打印
```java
// 在刷新触发时更新日志时间，避免重复打印
lastLogTime = System.currentTimeMillis();
```

## 优化后的日志策略

### 1. 定时状态日志（每30秒）
```
Status: events_in_batch=150, batch_data_size=2.50 MB, 
accumulated_buffer_size=45.75 MB, flush_size_config=100 MB, 
flush_timeout_config=300 seconds, waiting_time=125000 ms
```

### 2. 刷新触发日志（立即）
```
Flush triggered by size_threshold: current_size=102.50 MB, size_threshold=100.00 MB, 
waiting_time=180000 ms, time_threshold=300000 ms

Status: events_in_batch=200, batch_data_size=3.25 MB, 
accumulated_buffer_size=102.50 MB, flush_size_config=100 MB, 
flush_timeout_config=300 seconds, waiting_time=180000 ms
```

### 3. 刷新完成日志
```
Flush completed: flushed_size=102.50 MB, waiting_time=180000 ms, 
flush_duration=2500 ms, response=RespContent{status='Success'}
```

## 日志频率对比

### 优化前
```
高频场景：每秒100批 → 每秒100条状态日志
中频场景：每秒10批  → 每秒10条状态日志
低频场景：每秒1批   → 每秒1条状态日志
```

### 优化后
```
高频场景：每秒100批 → 每30秒1条状态日志 + 刷新时额外日志
中频场景：每秒10批  → 每30秒1条状态日志 + 刷新时额外日志
低频场景：每秒1批   → 每30秒1条状态日志 + 刷新时额外日志
```

**减少比例**：在高频场景下，日志量减少约99%

## 监控效果

### 1. 正常运行时
```
2024-01-15 10:30:00 INFO - Status: events_in_batch=500, batch_data_size=12.50 MB, 
                           accumulated_buffer_size=75.25 MB, flush_size_config=100 MB, 
                           flush_timeout_config=300 seconds, waiting_time=150000 ms

2024-01-15 10:30:30 INFO - Status: events_in_batch=480, batch_data_size=11.80 MB, 
                           accumulated_buffer_size=87.05 MB, flush_size_config=100 MB, 
                           flush_timeout_config=300 seconds, waiting_time=180000 ms
```

### 2. 刷新触发时
```
2024-01-15 10:30:45 INFO - Flush triggered by size_threshold: current_size=102.50 MB, 
                           size_threshold=100.00 MB, waiting_time=195000 ms, 
                           time_threshold=300000 ms

2024-01-15 10:30:45 INFO - Status: events_in_batch=320, batch_data_size=8.20 MB, 
                           accumulated_buffer_size=102.50 MB, flush_size_config=100 MB, 
                           flush_timeout_config=300 seconds, waiting_time=195000 ms

2024-01-15 10:30:47 INFO - Flush completed: flushed_size=102.50 MB, waiting_time=195000 ms, 
                           flush_duration=2000 ms, response=RespContent{status='Success'}
```

## 配置建议

### 日志间隔调整
如果需要调整日志打印间隔，修改常量：
```java
private static final long LOG_INTERVAL_MS = 30 * 1000; // 30秒
```

**建议值**：
- **开发环境**：10-15秒，便于调试
- **测试环境**：30秒，平衡监控和性能
- **生产环境**：60秒，减少日志量

### 日志级别设置
- **INFO**：状态日志、刷新触发、刷新完成
- **DEBUG**：详细的内部状态、文件操作
- **WARN**：流量限制、异常情况
- **ERROR**：刷新失败、文件操作失败

## 性能影响

### 1. 日志I/O减少
- 减少磁盘写入操作
- 降低日志文件增长速度
- 减少日志轮转频率

### 2. CPU开销降低
- 减少字符串格式化操作
- 降低日志框架开销
- 减少内存分配

### 3. 可读性提升
- 重要信息更容易发现
- 减少日志噪音
- 便于问题定位

## 监控建议

### 1. 关注指标
- **状态日志间隔**：确认每30秒有状态更新
- **刷新频率**：观察刷新触发的频率和原因
- **数据积累速度**：监控缓冲区增长趋势

### 2. 告警设置
- **长时间无状态日志**：可能表示数据处理停滞
- **频繁刷新触发**：可能需要调整阈值配置
- **刷新失败**：需要立即关注的问题

### 3. 日志分析
- 使用日志聚合工具分析趋势
- 监控刷新性能指标
- 跟踪数据处理吞吐量

## 总结

这次日志优化实现了：

1. **减少日志噪音**：从每批打印改为每30秒打印
2. **保持关键信息**：刷新时仍然打印详细状态
3. **提升性能**：减少I/O和CPU开销
4. **改善可读性**：重要信息更容易发现
5. **灵活配置**：可根据环境调整日志间隔

优化后的日志系统在保持监控能力的同时，显著减少了日志量，提升了系统性能和运维体验。
