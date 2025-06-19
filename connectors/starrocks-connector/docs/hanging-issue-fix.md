# StarRocks 连接器卡住问题修复

## 问题描述

在配置 100MB 刷新阈值时，连接器出现卡住现象，但使用 curl 命令直接发送相同的缓存文件却能成功。

## 问题分析

通过对比用户的 curl 命令和我们的代码实现，发现了两个关键问题：

### 1. HTTP 头参数类型错误

**问题代码**：
```java
header.put("ignore_json_size", true);  // 布尔值
```

**正确代码**：
```java
header.put("ignore_json_size", "true");  // 字符串
```

**用户的 curl 命令**：
```bash
curl -H "ignore_json_size: true"  # 字符串格式
```

### 2. HTTP Content-Length 设置错误

**问题根源**：
在文件缓存实现中，我们没有正确设置 `recordStream` 的 `contentLength`，导致 `InputStreamEntity` 获取到错误的长度信息。

**问题流程**：
```java
// 1. 在 finally 块中重置为 0
recordStream.setContentLength(0L);

// 2. 在 loadCacheFileToStream 中没有设置正确的长度
recordStream.write(fileContent);  // 写入数据但没有设置长度

// 3. 在创建 HTTP 实体时获取到错误的长度
InputStreamEntity entity = new InputStreamEntity(recordStream, recordStream.getContentLength());
// getContentLength() 返回 0，导致 HTTP 请求异常
```

## 修复方案

### 1. 修复 HTTP 头参数类型
```java
// 修复前
header.put("ignore_json_size", true);

// 修复后
header.put("ignore_json_size", "true");
```

### 2. 修复 Content-Length 设置
```java
private void loadCacheFileToStream() throws IOException {
    try {
        if (tempCacheFile != null && Files.exists(tempCacheFile)) {
            recordStream.startInput();

            // 直接读取整个文件内容并写入到 recordStream
            byte[] fileContent = Files.readAllBytes(tempCacheFile);
            recordStream.write(fileContent);
            
            // 重要：设置正确的 contentLength，这对 HTTP 请求很关键
            recordStream.setContentLength(fileContent.length);

            recordStream.endInput();
            TapLogger.debug(TAG, "Loaded cache file to stream: {}, size: {}, contentLength: {}",
                tempCacheFile.toString(), formatBytes(Files.size(tempCacheFile)), fileContent.length);
        }
    } catch (IOException e) {
        TapLogger.error(TAG, "Failed to load cache file to stream: {}", e.getMessage());
        throw e;
    }
}
```

## 问题影响分析

### 1. ignore_json_size 参数错误
- **影响**：StarRocks 可能无法正确解析大型 JSON 数据
- **症状**：在处理大量数据时可能出现解析错误或性能问题
- **严重性**：中等，影响大数据量处理

### 2. Content-Length 错误
- **影响**：HTTP 请求可能被服务器拒绝或挂起
- **症状**：连接器卡住，无响应，超时
- **严重性**：高，导致功能完全不可用

## 对比分析

### 用户的 curl 命令
```bash
curl --location-trusted -u root: \
  -H "format: json" \
  -H "strip_outer_array: true" \
  -H "ignore_json_size: true" \
  -T starrocks-cache-407-1750339534097.tmp \
  http://127.0.0.1:8030/api/t/mock_1000w/_stream_load
```

**关键特点**：
- 使用 `-T` 参数直接传输文件，自动设置正确的 Content-Length
- 所有头参数都是字符串格式
- 使用 `--location-trusted` 处理重定向

### 我们的实现
```java
// HTTP 头设置
header.put("format", "json");
header.put("strip_outer_array", "true");
header.put("ignore_json_size", "true");  // 修复后

// HTTP 实体创建
InputStreamEntity entity = new InputStreamEntity(recordStream, recordStream.getContentLength());
// 现在 getContentLength() 返回正确的文件大小
```

## 验证方法

### 1. 日志验证
查看日志中的 contentLength 信息：
```
DEBUG - Loaded cache file to stream: /tmp/starrocks-cache/file.tmp, size: 10.50 MB, contentLength: 11010048
```

### 2. HTTP 请求验证
确认 HTTP 请求头包含正确的参数：
```
Content-Length: 11010048
format: json
strip_outer_array: true
ignore_json_size: true
```

### 3. 功能验证
- 测试 100MB 配置下的数据处理
- 验证大数据量不再卡住
- 确认刷新操作正常完成

## 预防措施

### 1. 参数类型检查
- 确保所有 HTTP 头参数使用字符串类型
- 添加参数验证逻辑

### 2. Content-Length 验证
- 在创建 HTTP 实体前验证 contentLength > 0
- 添加相关的调试日志

### 3. 测试覆盖
- 添加大数据量测试用例
- 验证各种配置下的刷新行为
- 对比 curl 命令的行为

## 总结

这次修复解决了两个关键问题：
1. **HTTP 头参数类型错误**：确保与 StarRocks 服务器的兼容性
2. **Content-Length 设置错误**：确保 HTTP 请求的正确性

修复后，StarRocks 连接器应该能够：
- 正确处理 100MB 配置的刷新阈值
- 避免在大数据量处理时卡住
- 与 curl 命令行为保持一致
- 提供稳定的文件缓存功能

这些修复确保了文件缓存功能的可靠性和与 StarRocks 服务器的完全兼容性。
