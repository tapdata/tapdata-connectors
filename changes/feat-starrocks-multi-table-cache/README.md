# StarRocks 连接器多表缓存功能修复

## 问题描述

原始的StarRocks连接器存在以下问题：

1. **文件缓存全局共享**：所有表共用一个缓存文件，导致数据混乱
2. **dataColumns全局覆盖**：不同表的列信息相互覆盖，导致数据丢失
3. **刷新机制不完善**：无法按表分别管理和刷新数据
4. **文件名冲突风险**：缓存文件名没有唯一标识，可能发生冲突

## 解决方案

### 1. 按表分离文件缓存

**修改前**：
```java
// 全局共享的缓存文件
private Path tempCacheFile;
private FileOutputStream cacheFileStream;
private boolean isFirstRecord = true;
```

**修改后**：
```java
// 按表管理的缓存文件
private final Map<String, Path> tempCacheFilesByTable;
private final Map<String, FileOutputStream> cacheFileStreamsByTable;
private final Map<String, Boolean> isFirstRecordByTable;
private final Set<String> pendingFlushTables; // 记录还没有flush的表
```

### 2. 按表独立存储dataColumns

**修改前**：
```java
// 全局共享的dataColumns
private final AtomicReference<Set<String>> dataColumns;
```

**修改后**：
```java
// 按表存储dataColumns
private final Map<String, Set<String>> dataColumnsByTable;
```

### 3. 文件命名规范化

**修改前**：
```java
String fileName = String.format("starrocks-cache-%d-%d.tmp",
    Thread.currentThread().getId(), System.currentTimeMillis());
```

**修改后**：
```java
String uuid = UUID.randomUUID().toString().substring(0, 8);
String fileName = String.format("starrocks-cache-%s-%s-%d-%d.tmp",
    tableName, uuid, Thread.currentThread().getId(), System.currentTimeMillis());
```

### 4. 按表刷新机制

**新增方法**：
- `flushTable(String tableName, TapTable table)` - 刷新指定表的数据
- `flushAllPendingTables()` - 刷新所有待刷新的表
- `finalizeCacheFileForTable(String tableName)` - 完成指定表的缓存文件写入
- `cleanupCacheFileForTable(String tableName)` - 清理指定表的缓存文件
- `cleanupAllCacheFiles()` - 清理所有表的缓存文件

## 核心修改点

### 1. 构造函数初始化

```java
// 初始化文件缓存相关的Map
this.tempCacheFilesByTable = new ConcurrentHashMap<>();
this.cacheFileStreamsByTable = new ConcurrentHashMap<>();
this.isFirstRecordByTable = new ConcurrentHashMap<>();
this.pendingFlushTables = ConcurrentHashMap.newKeySet();
```

### 2. 按表初始化缓存文件

```java
private void initializeCacheFileForTable(String tableName) {
    // 创建包含表名和UUID的唯一文件名
    String uuid = UUID.randomUUID().toString().substring(0, 8);
    String fileName = String.format("starrocks-cache-%s-%s-%d-%d.tmp",
        tableName, uuid, Thread.currentThread().getId(), System.currentTimeMillis());
    
    // 存储到对应的Map中
    tempCacheFilesByTable.put(tableName, tempCacheFile);
    cacheFileStreamsByTable.put(tableName, cacheFileStream);
    isFirstRecordByTable.put(tableName, true);
}
```

### 3. 按表写入数据

```java
private void writeToCacheFile(byte[] data, String tableName) throws IOException {
    FileOutputStream cacheFileStream = cacheFileStreamsByTable.get(tableName);
    Boolean isFirstRecord = isFirstRecordByTable.get(tableName);
    
    // 按表管理写入状态
    if (!isFirstRecord) {
        cacheFileStream.write(messageSerializer.lineEnd());
    } else {
        cacheFileStream.write(messageSerializer.batchStart());
        isFirstRecordByTable.put(tableName, false);
    }
    
    cacheFileStream.write(data);
    cacheFileStream.flush();
}
```

### 4. 定时刷新所有表

```java
private void checkAndFlushIfNeeded() {
    synchronized (writeLock) {
        if (lastEventFlag.get() == 0 || pendingFlushTables.isEmpty()) {
            return;
        }
        
        long timeSinceLastFlush = System.currentTimeMillis() - lastFlushTime;
        long flushTimeoutMs = StarrocksConfig.getFlushTimeoutSeconds() * 1000L;
        
        if (timeSinceLastFlush >= flushTimeoutMs) {
            // 刷新所有待刷新的表
            flushAllPendingTables();
        }
    }
}
```

## 测试验证

创建了测试脚本 `test_multi_table_cache.java` 来验证：

1. **缓存文件分离**：验证不同表使用不同的缓存文件
2. **dataColumns独立**：验证每个表的列信息独立存储
3. **文件命名规范**：验证文件名包含表名和UUID
4. **刷新机制**：验证按表刷新和批量刷新功能

## 兼容性保证

- 保留了原有的 `flush(TapTable table)` 方法，内部调用新的 `flushTable()` 方法
- 保留了原有的方法签名，确保现有代码不受影响
- 新增的功能都是向后兼容的

## 性能优化

1. **减少文件冲突**：每个表独立的缓存文件避免了锁竞争
2. **精确刷新**：只刷新有数据变化的表，减少不必要的网络请求
3. **并发安全**：使用ConcurrentHashMap确保多线程安全
4. **内存优化**：及时清理已刷新表的缓存数据

## 使用说明

修改后的连接器会自动：

1. 为每个表创建独立的缓存文件
2. 按表管理数据列信息
3. 根据配置的大小和时间阈值自动刷新
4. 在任务停止时刷新所有剩余数据
5. 在关闭时清理所有缓存文件

无需修改现有的使用代码，所有改进都是内部实现的优化。

## 调试日志功能

为了便于问题排查和验证修复效果，新增了详细的调试日志：

### 1. 数据处理日志

```
[INFO] Processing batch for table user_table: events_count=5, table_fields=[id, name, email]
[INFO] Current dataColumns for table user_table: [id, name, email]
```

### 2. dataColumns设置日志

```
[INFO] Started new load batch for table user_table with operation: 1, dataColumns: [id, name, email]
```

### 3. Columns构建过程日志

**PUT方法日志**：
```
[INFO] [PUT] Building columns for table user_table: tableDataColumns=[id, name, email], tapTable.getNameFieldMap().keySet()=[id, name, email, created_at], uniqueKeyType=Primary
[DEBUG] [PUT] Column id: isInDataColumns=true, isAggregateType=false, shouldInclude=true
[DEBUG] [PUT] Column name: isInDataColumns=true, isAggregateType=false, shouldInclude=true
[DEBUG] [PUT] Column email: isInDataColumns=true, isAggregateType=false, shouldInclude=true
[DEBUG] [PUT] Column created_at: isInDataColumns=false, isAggregateType=false, shouldInclude=false
[INFO] [PUT] Final columns list for table user_table: [`id`, `name`, `email`]
```

**PUT_FROM_FILE方法日志**：
```
[INFO] [PUT_FROM_FILE] Building columns for table user_table: tableDataColumns=[id, name, email], tapTable.getNameFieldMap().keySet()=[id, name, email, created_at], uniqueKeyType=Primary
[DEBUG] [PUT_FROM_FILE] Column id: isInDataColumns=true, isAggregateType=false, shouldInclude=true
[DEBUG] [PUT_FROM_FILE] Column name: isInDataColumns=true, isAggregateType=false, shouldInclude=true
[DEBUG] [PUT_FROM_FILE] Column email: isInDataColumns=true, isAggregateType=false, shouldInclude=true
[DEBUG] [PUT_FROM_FILE] Column created_at: isInDataColumns=false, isAggregateType=false, shouldInclude=false
[INFO] [PUT_FROM_FILE] Columns before adding DELETE_SIGN for table user_table: [`id`, `name`, `email`]
[INFO] [PUT_FROM_FILE] Final columns list for table user_table (with DELETE_SIGN): [`id`, `name`, `email`, `__DORIS_DELETE_SIGN__`]
```

### 4. 日志说明

- **tableDataColumns**: 当前事件中实际包含数据的列
- **tapTable.getNameFieldMap().keySet()**: 表结构中定义的所有列
- **uniqueKeyType**: 表类型（Primary/Unique/Aggregate/Duplicate）
- **isInDataColumns**: 列是否在当前数据中存在
- **isAggregateType**: 是否为聚合表类型
- **shouldInclude**: 是否应该包含在最终的columns列表中

### 5. 测试脚本

提供了 `test_columns_debug.java` 测试脚本，可以验证：
- 单表数据写入的columns构建过程
- 多表数据写入时各表独立的columns管理
- 稀疏数据（部分列为空）的处理情况

通过这些日志，可以清楚地看到：
1. 每个表的dataColumns是如何独立管理的
2. columns列表是如何根据实际数据列和表类型构建的
3. 不同表之间的dataColumns不会相互影响
