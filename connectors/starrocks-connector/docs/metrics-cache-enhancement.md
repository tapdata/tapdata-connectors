# StarRocks 连接器 Metrics 缓存增强

## 概述

为 StarRocks 连接器的 Metrics 系统添加了本地缓存功能，确保在超时自动刷新和停止刷新时能够正确同步 metrics 信息，避免数据统计丢失。

## 问题背景

### 原有问题
1. **超时自动刷新时**：metrics 信息没有被正确处理和清理
2. **停止刷新时**：缓存的 metrics 可能丢失，导致统计不准确
3. **监控盲区**：无法了解当前缓存中有多少未刷新的数据统计

### 业务影响
- 数据处理统计不准确
- 监控信息缺失
- 难以评估数据处理效果

## 解决方案

### 1. 🗄️ Metrics 缓存机制

**新增缓存字段**：
```java
// 缓存的 metrics，用于未刷新时的累积
private long cachedInsert = 0L;
private long cachedUpdate = 0L;
private long cachedDelete = 0L;
```

**双重计数机制**：
```java
public void increase(TapRecordEvent tapRecordEvent) {
    if (tapRecordEvent instanceof TapInsertRecordEvent) {
        insert++;        // 当前批次计数
        cachedInsert++;  // 累积缓存计数
    }
    // ... 其他操作类型
}
```

### 2. 🔄 智能缓存管理

**成功刷新后清理**：
```java
public void clearCache() {
    cachedInsert = 0L;
    cachedUpdate = 0L;
    cachedDelete = 0L;
}
```

**获取缓存信息**：
```java
public String getCachedInfo() {
    return String.format("cached[insert=%d, update=%d, delete=%d, total=%d]", 
        cachedInsert, cachedUpdate, cachedDelete, getCachedTotal());
}
```

### 3. 📊 增强的监控功能

**状态日志增强**：
```java
TapLogger.info(TAG, "Status: events_in_batch={}, batch_data_size={}, " +
    "accumulated_buffer_size={}, flush_size_config={} MB, " +
    "flush_timeout_config={} seconds, waiting_time={} ms, {}",
    processedEvents, formatBytes(batchDataSize), formatBytes(currentBatchSize),
    flushSizeMB, flushTimeoutSeconds, waitTime, metrics.getCachedInfo());
```

**外部监控接口**：
```java
public String getCachedMetricsInfo() {
    return metrics.getCachedInfo();
}

public long getCachedMetricsTotal() {
    return metrics.getCachedTotal();
}
```

## 核心改进

### 1. 定时刷新 Metrics 处理

**刷新前记录**：
```java
TapLogger.info(TAG, "Scheduled flush triggered by timeout: waiting_time={} ms, " +
    "timeout_threshold={} ms, accumulated_size={}, {}", 
    timeSinceLastFlush, flushTimeoutMs, formatBytes(currentBatchSize), 
    metrics.getCachedInfo());
```

**刷新后清理**：
```java
RespContent respContent = flush(tapTable);
if (respContent != null) {
    metrics.clearCache();
    TapLogger.debug(TAG, "Cleared cached metrics after scheduled flush");
}
```

### 2. 停止刷新 Metrics 处理

**停止前记录**：
```java
TapLogger.info(TAG, "Flushing remaining data on stop: accumulated_size={}, {}", 
    formatBytes(currentBatchSize), metrics.getCachedInfo());
```

**停止后清理**：
```java
RespContent respContent = flush(tapTable);
if (respContent != null) {
    metrics.clearCache();
    TapLogger.info(TAG, "Cleared cached metrics after stop flush");
}
```

### 3. 常规刷新 Metrics 处理

**增强的清理逻辑**：
```java
if (null != listResult) {
    metrics.writeIntoResultList(listResult);
    metrics.clear();
    metrics.clearCache(); // 成功刷新后清理缓存
} else {
    metrics.clearCache(); // 即使没有 listResult，也要清理缓存
}
```

## 监控和日志

### 1. 详细的状态日志

**定时状态日志**：
```
Status: events_in_batch=150, batch_data_size=2.50 MB, accumulated_buffer_size=45.75 MB, 
flush_size_config=100 MB, flush_timeout_config=300 seconds, waiting_time=125000 ms, 
cached[insert=1250, update=300, delete=50, total=1600]
```

**定时刷新日志**：
```
Scheduled flush triggered by timeout: waiting_time=305000 ms, timeout_threshold=300000 ms, 
accumulated_size=45.75 MB, cached[insert=1250, update=300, delete=50, total=1600]

Cleared cached metrics after scheduled flush
```

**停止刷新日志**：
```
Flushing remaining data on stop: accumulated_size=25.30 MB, 
cached[insert=800, update=150, delete=25, total=975]

Cleared cached metrics after stop flush
```

### 2. 监控指标

**缓存统计**：
- 缓存的插入操作数
- 缓存的更新操作数
- 缓存的删除操作数
- 缓存的总操作数

**状态跟踪**：
- 缓存清理频率
- 缓存累积趋势
- 刷新成功率

## 使用场景

### 1. 长时间无数据场景
```
时间点 1: cached[insert=100, update=20, delete=5, total=125]
时间点 2: cached[insert=100, update=20, delete=5, total=125] (无新数据)
超时刷新: 清理缓存，metrics 正确统计
```

### 2. 高频数据处理场景
```
批次 1: cached[insert=50, update=10, delete=2, total=62]
批次 2: cached[insert=120, update=25, delete=8, total=153]
批次 3: cached[insert=200, update=40, delete=15, total=255]
达到阈值刷新: 清理缓存，metrics 正确统计
```

### 3. 任务停止场景
```
运行中: cached[insert=300, update=60, delete=12, total=372]
停止信号: 执行停止刷新
停止完成: 清理缓存，确保所有 metrics 都被统计
```

## 性能影响

### 1. 内存开销
- **额外字段**：3个 long 类型字段（24字节）
- **计算开销**：每次操作额外的计数器增加
- **总体影响**：微乎其微

### 2. 处理性能
- **写入性能**：每次操作多一次计数，影响极小
- **刷新性能**：增加缓存清理操作，耗时可忽略
- **监控性能**：字符串格式化操作，按需调用

### 3. 日志影响
- **日志量**：每条状态日志增加约30-50字符
- **可读性**：显著提升，能够清楚了解缓存状态
- **调试价值**：大幅提升问题定位能力

## 最佳实践

### 1. 监控建议
- 定期检查缓存的 metrics 总数
- 关注缓存清理的频率和时机
- 监控缓存累积的趋势

### 2. 告警设置
- 缓存 metrics 长时间不清理
- 缓存 metrics 异常增长
- 刷新失败导致的缓存堆积

### 3. 运维建议
- 通过日志分析数据处理模式
- 根据缓存情况调整刷新策略
- 利用 metrics 信息进行容量规划

## 总结

Metrics 缓存增强实现了：

1. **数据完整性**：确保所有操作都被正确统计
2. **监控可见性**：提供详细的缓存状态信息
3. **智能管理**：自动清理和同步 metrics
4. **性能优化**：最小化性能影响
5. **运维友好**：丰富的日志和监控信息

这些改进使得 StarRocks 连接器的数据处理统计更加准确和可靠，为运维监控和问题诊断提供了强有力的支持。
