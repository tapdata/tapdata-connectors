# StarRocks连接器 Metaspace 内存溢出修复

## 问题描述

在使用StarRocks连接器处理多表数据时，出现了 `java.lang.OutOfMemoryError: Metaspace` 错误。

Metaspace是Java 8+中用来存储类元数据的内存区域，溢出通常由以下原因引起：
- 类加载器泄漏
- 动态类生成过多
- 内存配置不当
- 连接器重复加载

## 根本原因分析

在我们的多表缓存实现中，可能的内存泄漏点：

1. **文件流资源泄漏**：每个表的缓存文件流没有正确关闭
2. **Map对象累积**：多个ConcurrentHashMap持续增长，没有及时清理
3. **UUID对象创建**：频繁创建UUID对象可能导致额外的类加载
4. **并发表数量无限制**：没有限制同时处理的表数量

## 修复措施

### 1. 资源管理优化

**修改前**：
```java
// 可能导致资源泄漏
private void initializeCacheFileForTable(String tableName) {
    String uuid = UUID.randomUUID().toString().substring(0, 8);
    FileOutputStream cacheFileStream = new FileOutputStream(tempCacheFile.toFile());
    // 直接存储，没有清理之前的资源
}
```

**修改后**：
```java
private void initializeCacheFileForTable(String tableName) {
    // 先清理该表的现有资源（如果存在）
    cleanupCacheFileForTable(tableName);
    
    // 使用简化的文件名，避免过多对象创建
    long timestamp = System.currentTimeMillis();
    long threadId = Thread.currentThread().getId();
    String fileName = String.format("starrocks-cache-%s-%d-%d.tmp", tableName, threadId, timestamp);
    
    FileOutputStream cacheFileStream = new FileOutputStream(tempCacheFile.toFile());
    // 存储到Map中
}
```

### 2. 内存保护机制

**新增表数量限制**：
```java
// 内存保护：限制同时处理的表数量
private static final int MAX_CONCURRENT_TABLES = 50;

private void checkMemoryAndTableLimits(String tableName) {
    // 检查表数量限制
    if (tempCacheFilesByTable.size() >= MAX_CONCURRENT_TABLES) {
        // 强制清理一些最老的表
        cleanupOldestTables();
    }
}
```

**内存监控**：
```java
private void checkMemoryAndTableLimits(String tableName) {
    Runtime runtime = Runtime.getRuntime();
    long usedMemory = runtime.totalMemory() - runtime.freeMemory();
    long maxMemory = runtime.maxMemory();
    double memoryUsagePercent = (double) usedMemory / maxMemory * 100;
    
    // 如果内存使用率超过80%，强制垃圾回收
    if (memoryUsagePercent > 80) {
        TapLogger.warn(TAG, "High memory usage detected ({}%), forcing garbage collection", 
            String.format("%.1f", memoryUsagePercent));
        System.gc();
    }
}
```

### 3. 完善的资源清理

**优化shutdown方法**：
```java
public void shutdown() {
    // 停止定时调度器
    if (flushScheduler != null) {
        flushScheduler.shutdown();
        flushScheduler = null;
    }
    
    // 关闭HTTP客户端
    if (this.httpClient != null) {
        this.httpClient.close();
    }
    
    // 清理所有缓存文件
    cleanupAllCacheFiles();
    
    // 清理所有Map，释放内存
    tempCacheFilesByTable.clear();
    cacheFileStreamsByTable.clear();
    isFirstRecordByTable.clear();
    dataColumnsByTable.clear();
    pendingFlushTables.clear();
    
    // 强制垃圾回收
    System.gc();
}
```

## JVM 配置建议

### 1. Metaspace 配置

```bash
# 增加Metaspace大小
-XX:MetaspaceSize=256m
-XX:MaxMetaspaceSize=512m

# 启用Metaspace垃圾回收
-XX:+UseG1GC
-XX:+UnlockExperimentalVMOptions
-XX:+UseStringDeduplication
```

### 2. 内存监控配置

```bash
# 启用详细的GC日志
-XX:+PrintGC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-XX:+PrintGCApplicationStoppedTime

# 内存溢出时生成堆转储
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/tmp/heapdump.hprof

# 启用JFR监控
-XX:+FlightRecorder
-XX:StartFlightRecording=duration=60s,filename=/tmp/flight.jfr
```

### 3. 推荐的完整JVM参数

```bash
java -Xms2g -Xmx4g \
     -XX:MetaspaceSize=256m \
     -XX:MaxMetaspaceSize=512m \
     -XX:+UseG1GC \
     -XX:MaxGCPauseMillis=200 \
     -XX:+UnlockExperimentalVMOptions \
     -XX:+UseStringDeduplication \
     -XX:+HeapDumpOnOutOfMemoryError \
     -XX:HeapDumpPath=/tmp/heapdump.hprof \
     -XX:+PrintGC \
     -XX:+PrintGCDetails \
     -XX:+PrintGCTimeStamps \
     -jar your-application.jar
```

## 监控和预防

### 1. 内存使用监控

连接器现在会定期打印内存使用情况：
```
[INFO] Memory usage: 75.2% (1536/2048 MB), active tables: 25
```

### 2. 表数量监控

当表数量接近限制时会有警告：
```
[WARN] Too many concurrent tables (50), forcing cleanup of oldest tables
```

### 3. 资源清理日志

```
[INFO] Shutting down StarrocksStreamLoader, active tables: 15
[INFO] Cleaned up cache files for 15 tables during shutdown
[INFO] StarrocksStreamLoader shutdown completed
```

## 应急处理

如果再次出现Metaspace溢出：

### 1. 立即措施
```bash
# 重启应用程序
# 增加Metaspace大小
-XX:MaxMetaspaceSize=1g

# 启用更频繁的垃圾回收
-XX:+UseG1GC -XX:MaxGCPauseMillis=100
```

### 2. 排查步骤
1. 检查日志中的内存使用情况
2. 查看活跃表数量是否异常
3. 检查是否有表没有正确清理
4. 分析堆转储文件（如果生成）

### 3. 临时缓解
```java
// 手动触发垃圾回收
System.gc();

// 减少并发表数量限制
MAX_CONCURRENT_TABLES = 20;
```

## 长期优化建议

1. **监控Metaspace使用趋势**：建立监控告警
2. **优化表处理策略**：考虑批量处理相似表
3. **定期清理机制**：实现更智能的资源清理策略
4. **内存池化**：考虑使用对象池减少对象创建

## 验证修复效果

修复后应该观察到：
- Metaspace使用量稳定
- 内存使用率保持在合理范围
- 没有资源泄漏警告
- 表数量得到有效控制
