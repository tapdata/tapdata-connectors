# StarRocks 连接器文件缓存修复

## 问题描述

在实现文件缓存功能后，出现了 JSON 格式解析错误：

```
"Message": "Failed to iterate json array as object. error: INCORRECT_TYPE: The JSON element does not have the requested type."
```

## 问题原因

### 1. 数据格式不一致
原始实现中，我们使用 `BufferedWriter` 以字符串方式处理文件，这导致：
- 字符编码可能不一致
- 换行符处理不正确
- JSON 数组格式被破坏

### 2. HTTP 头设置期望
StarRocks 的 JSON 格式配置：
```java
header.put("strip_outer_array", "true");
header.put("fuzzy_parse", "true");
```

这意味着 StarRocks 期望接收一个完整的 JSON 数组格式：
```json
[{"field1":"value1","field2":"value2"},{"field1":"value3","field2":"value4"}]
```

### 3. 序列化器格式要求
JsonSerializer 的格式要求：
- `batchStart()`: `[` (数组开始)
- `lineEnd()`: `,` (逗号分隔)
- `batchEnd()`: `]` (数组结束)

## 修复方案

### 1. 改用二进制流处理
将 `BufferedWriter` 改为 `FileOutputStream`：
```java
// 修改前
private BufferedWriter cacheFileWriter;

// 修改后
private FileOutputStream cacheFileStream;
```

### 2. 保持字节级别的一致性
所有数据操作都使用 `byte[]`，确保与原始序列化器完全一致：
```java
// 写入批次开始标记
cacheFileStream.write(messageSerializer.batchStart());

// 写入分隔符
cacheFileStream.write(messageSerializer.lineEnd());

// 写入实际数据
cacheFileStream.write(data);

// 写入批次结束标记
cacheFileStream.write(messageSerializer.batchEnd());
```

### 3. 简化文件读取
直接读取整个文件内容，避免逐行处理：
```java
// 直接读取整个文件内容并写入到 recordStream
byte[] fileContent = Files.readAllBytes(tempCacheFile);
recordStream.write(fileContent);
```

## 修复后的数据流程

### 1. 写入阶段
```
数据事件 → 序列化为 byte[] → 写入文件 (二进制) → 刷新到磁盘
```

### 2. 文件格式
```
[{record1},{record2},{record3}]
```
完全符合 StarRocks 期望的 JSON 数组格式。

### 3. 读取阶段
```
读取完整文件 → 直接写入 recordStream → 发送到 StarRocks
```

## 关键改进点

### 1. 数据一致性
- 使用二进制流确保字节级别的一致性
- 避免字符编码转换问题
- 保持与原始序列化器完全相同的格式

### 2. 性能优化
- 减少字符串转换开销
- 直接的字节操作更高效
- 简化的文件读取逻辑

### 3. 错误处理
- 更清晰的错误日志
- 完善的资源清理
- 异常情况下的恢复机制

## 验证方法

### 1. 编译验证
```bash
cd connectors/starrocks-connector
mvn clean compile
```

### 2. 格式验证
检查生成的缓存文件内容是否为有效的 JSON 数组格式。

### 3. 功能验证
- 测试小批量数据写入
- 测试大批量数据写入
- 测试超时刷新
- 测试大小阈值刷新

## 预期效果

修复后应该能够：
1. 正确处理 JSON 格式数据
2. 避免 "Failed to iterate json array as object" 错误
3. 保持与原有功能完全兼容
4. 提供稳定的文件缓存功能

## 监控建议

### 1. 错误监控
监控 StarRocks 返回的错误信息，特别关注：
- JSON 格式错误
- 数据类型错误
- 编码相关错误

### 2. 性能监控
- 文件写入性能
- 刷新操作耗时
- 内存使用情况

### 3. 数据完整性
- 验证写入的记录数与实际处理的记录数一致
- 检查数据格式的正确性
- 监控数据丢失情况

## 最佳实践

### 1. 测试建议
- 在生产环境部署前进行充分测试
- 测试各种数据类型和大小
- 验证异常情况下的恢复能力

### 2. 配置建议
- 根据数据量调整刷新阈值
- 监控磁盘空间使用
- 设置合适的超时时间

### 3. 运维建议
- 定期清理临时文件
- 监控文件系统性能
- 备份重要配置
