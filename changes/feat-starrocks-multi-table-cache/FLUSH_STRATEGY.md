# StarRocks连接器刷新策略详解

## 问题背景

在多表场景下，原有的刷新逻辑存在以下问题：
1. **不公平**：使用全局大小阈值，小表被大表"拖累"
2. **效率低**：每次都刷新所有表，即使某些表数据很少
3. **资源浪费**：产生很多小的HTTP请求

## 新的刷新策略：按表独立刷新

### 核心原理

每个表独立维护自己的：
- **批次大小**：`currentBatchSizeByTable`
- **刷新时间**：`lastFlushTimeByTable`
- **刷新条件判断**：独立计算是否达到阈值

### 刷新触发条件

#### 1. 大小阈值（按表独立）
```java
// 每个表独立判断
long tableCurrentSize = getTableBatchSize(tableName);
boolean sizeThresholdReached = tableCurrentSize + length >= flushSizeBytes;
```

**配置**：`StarrocksConfig.getFlushSizeMB()`
**逻辑**：当**单个表**的数据大小达到配置值时触发刷新

#### 2. 时间阈值（按表独立）
```java
// 每个表独立的刷新时间
long tableLastFlushTime = getTableLastFlushTime(tableName);
long timeSinceLastFlush = currentTime - tableLastFlushTime;
boolean timeThresholdReached = timeSinceLastFlush >= flushTimeoutMs;
```

**配置**：`StarrocksConfig.getFlushTimeoutSeconds()`
**逻辑**：当**单个表**距离上次刷新超过配置时间时触发刷新

#### 3. 数据列变化（按表独立）
```java
// 按表检查dataColumns变化
Set<String> currentDataColumns = dataColumnsByTable.get(tableName);
boolean dataColumnsChanged = !getDataColumns(recordEvent).equals(currentDataColumns);
```

**逻辑**：当表的数据列结构发生变化时立即刷新

### 刷新执行策略

#### 1. 实时刷新（writeRecord触发）
- **触发时机**：每次写入数据时检查
- **刷新范围**：只刷新达到阈值的**单个表**
- **优点**：响应及时，资源利用高效

```java
if (needFlush(tapRecordEvent, bytes.length, isAgg, tableName)) {
    flushTable(tableName, table); // 只刷新当前表
}
```

#### 2. 定时刷新（后台调度）
- **触发时机**：定时调度器（默认每30秒检查一次）
- **刷新范围**：检查所有表，只刷新达到阈值的表
- **优点**：确保数据不会长时间积压

```java
// 检查每个表是否需要刷新
for (String tableName : pendingFlushTables) {
    if (timeoutReached || sizeReached) {
        tablesToFlush.add(tableName);
    }
}
flushSpecificTables(tablesToFlush); // 只刷新需要的表
```

#### 3. 停止刷新（flushOnStop）
- **触发时机**：任务停止时
- **刷新范围**：刷新所有待刷新的表
- **目的**：确保数据不丢失

## 配置参数说明

### 1. 大小阈值配置
```properties
# 单表刷新大小阈值（MB）
flush.size.mb=100
```
- **含义**：单个表的数据达到100MB时触发刷新
- **建议值**：50-200MB，根据内存和网络情况调整

### 2. 时间阈值配置
```properties
# 单表刷新时间阈值（秒）
flush.timeout.seconds=300
```
- **含义**：单个表距离上次刷新超过300秒时触发刷新
- **建议值**：60-600秒，根据数据实时性要求调整

### 3. 定时检查间隔
```java
// 代码中配置，默认30秒
private static final long FLUSH_CHECK_INTERVAL = 30000;
```

## 日志输出示例

### 1. 按表刷新日志
```
[INFO] Table user_table flush triggered by size_threshold: table_size=105.2MB, size_threshold=100.0MB, 
       waiting_time=45000 ms, time_threshold=300000 ms, total_size=256.8MB

[INFO] Table order_table flush triggered by timeout: table_size=23.5MB, size_threshold=100.0MB, 
       waiting_time=305000 ms, time_threshold=300000 ms, total_size=256.8MB
```

### 2. 定时刷新日志
```
[INFO] Table user_table scheduled flush triggered by size_threshold: table_size=102.3MB, 
       waiting_time=120000 ms, timeout_threshold=300000 ms

[INFO] Scheduled flush: 2 tables to flush, total_size=256.8MB
```

### 3. 刷新完成日志
```
[INFO] Table user_table flush completed: flushed_size=105.2MB, waiting_time=45000 ms, 
       flush_duration=1250 ms, response=Success
```

## 性能优化效果

### 1. 减少不必要的刷新
- **原来**：所有表一起刷新，即使某些表数据很少
- **现在**：只刷新达到阈值的表

### 2. 提高并发效率
- **原来**：全局锁，所有表串行刷新
- **现在**：按表独立，可以并行处理不同表

### 3. 更好的资源利用
- **原来**：可能产生很多小的HTTP请求
- **现在**：每次刷新都是有意义的数据量

## 监控和调优

### 1. 关键指标监控
- 每个表的刷新频率
- 每次刷新的数据量
- 刷新耗时分布
- 待刷新表数量

### 2. 调优建议

#### 大小阈值调优
```bash
# 如果刷新太频繁，增加大小阈值
flush.size.mb=200

# 如果内存压力大，减少大小阈值
flush.size.mb=50
```

#### 时间阈值调优
```bash
# 如果对实时性要求高，减少时间阈值
flush.timeout.seconds=60

# 如果可以容忍延迟，增加时间阈值
flush.timeout.seconds=600
```

### 3. 异常情况处理

#### 表数量过多
- 自动清理最老的表（超过50个表时）
- 强制垃圾回收释放内存

#### 单表数据量过大
- 监控单表大小，超过阈值立即刷新
- 记录异常大小的表进行分析

## 兼容性说明

### 1. 配置兼容性
- 所有原有配置参数继续有效
- 新的按表逻辑对用户透明

### 2. API兼容性
- 保留原有的 `flush(TapTable table)` 方法
- 内部调用新的 `flushTable(tableName, table)` 方法

### 3. 行为变化
- **刷新频率**：可能会降低（更高效）
- **刷新粒度**：从全局改为按表
- **资源使用**：更加均衡和高效

这种新的刷新策略确保了多表场景下的公平性和效率，同时保持了良好的兼容性。
