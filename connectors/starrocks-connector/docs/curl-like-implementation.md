# StarRocks 连接器 Curl 风格实现

## 概述

为了解决 RecordBuffer 缓冲区限制导致的阻塞问题，我们实现了一个新的 `putFromFile` 方法，直接模仿 curl 的行为，绕过 RecordBuffer，直接从文件流式传输到 HTTP 请求。

## 问题回顾

### 原始问题
```bash
# 用户的 curl 命令可以成功
curl --location-trusted -u root: \
  -H "format: json" \
  -H "strip_outer_array: true" \
  -H "ignore_json_size: true" \
  -T starrocks-cache-407-1750339534097.tmp \
  http://127.0.0.1:8030/api/t/mock_1000w/_stream_load

# 但我们的代码会卡住
recordStream.write(fileContent); // 100MB 数据无法写入 768KB 缓冲区
```

### 根本原因
- **RecordBuffer 容量限制**：768KB 总容量无法容纳 100MB 数据
- **阻塞写入机制**：当缓冲区满时，write() 方法会永久阻塞
- **架构不匹配**：RecordBuffer 设计用于小批量实时数据，不适合大文件传输

## 新的解决方案

### 1. 直接文件传输
```java
/**
 * 直接从缓存文件发送数据，模仿 curl 的行为
 */
public RespContent putFromFile(final TapTable table) throws StreamLoadException, StarrocksRetryableException {
    // 直接从文件创建 InputStreamEntity，模仿 curl -T 的行为
    long fileSize = Files.size(tempCacheFile);
    FileInputStream fileInputStream = new FileInputStream(tempCacheFile.toFile());
    InputStreamEntity entity = new InputStreamEntity(fileInputStream, fileSize);
    entity.setContentType("application/json");
    
    // ... HTTP 请求构建和发送
}
```

### 2. 完全绕过 RecordBuffer
```java
// 修改前：通过 RecordBuffer
loadCacheFileToStream(); // 会阻塞
RespContent respContent = put(table); // 使用 recordStream

// 修改后：直接文件传输
RespContent respContent = putFromFile(table); // 直接从文件
```

## 技术对比

### Curl 命令分析
```bash
curl -T file.tmp http://server/api
```
**工作原理**：
- `-T` 参数直接上传文件
- 自动设置正确的 Content-Length
- 流式传输，不受内存限制
- 无中间缓冲区

### 我们的实现
```java
FileInputStream fileInputStream = new FileInputStream(tempCacheFile.toFile());
InputStreamEntity entity = new InputStreamEntity(fileInputStream, fileSize);
```
**工作原理**：
- 直接从文件创建输入流
- 自动设置正确的 Content-Length
- 流式传输，不受内存限制
- 无中间缓冲区

## 实现细节

### 1. HTTP 实体创建
```java
// 获取文件大小
long fileSize = Files.size(tempCacheFile);

// 创建文件输入流
FileInputStream fileInputStream = new FileInputStream(tempCacheFile.toFile());

// 创建 HTTP 实体，指定正确的长度
InputStreamEntity entity = new InputStreamEntity(fileInputStream, fileSize);
entity.setContentType("application/json");
```

### 2. HTTP 头设置
```java
HttpPutBuilder putBuilder = new HttpPutBuilder();
putBuilder.setUrl(loadUrl)
        .baseAuth(StarrocksConfig.getUser(), StarrocksConfig.getPassword())
        .addCommonHeader()
        .addFormat(writeFormat)  // 设置 format: json
        .addColumns(columns)
        .setLabel(label)
        .setEntity(entity);
```

### 3. 资源管理
```java
try (CloseableHttpResponse execute = httpClient.execute(httpPut)) {
    return handlePreCommitResponse(execute);
} finally {
    // 确保文件流被关闭
    try {
        fileInputStream.close();
    } catch (IOException e) {
        TapLogger.warn(TAG, "Failed to close file input stream: {}", e.getMessage());
    }
}
```

## 性能优势

### 1. 内存使用
```
原始方案：
文件(100MB) → 内存(100MB) → RecordBuffer(768KB) → 阻塞 ❌

新方案：
文件(100MB) → 直接流式传输 → HTTP → 成功 ✅
```

### 2. 处理能力
- **无大小限制**：理论上支持任意大小的文件
- **恒定内存**：内存使用与文件大小无关
- **高效传输**：直接流式传输，无额外拷贝

### 3. 稳定性
- **无阻塞风险**：完全绕过 RecordBuffer 的容量限制
- **资源安全**：正确的资源管理和异常处理
- **兼容性**：与 curl 行为完全一致

## 日志改进

### 新增日志
```java
TapLogger.debug(TAG, "Call stream load http api from file, url: {}, headers: {}, file_size: {}", 
    loadUrl, putBuilder.header, formatBytes(fileSize));
```

### 错误处理
```java
} catch (Exception e) {
    throw new StreamLoadException(String.format("Call stream load from file error: %s", e.getMessage()), e);
}
```

## 使用场景

### 1. 大文件处理
- **100MB+ 数据**：不再受 RecordBuffer 限制
- **批量导入**：支持大批量数据处理
- **高吞吐量**：适合高频大数据场景

### 2. 内存受限环境
- **低内存环境**：恒定的内存使用
- **容器化部署**：不会因大文件导致 OOM
- **资源优化**：更好的资源利用率

## 兼容性

### 1. 完全向后兼容
- 保留原有的 `put()` 方法
- 只在文件缓存场景下使用新方法
- 不影响现有功能

### 2. 配置兼容
- 所有现有配置继续有效
- HTTP 头设置保持一致
- 认证和权限机制不变

## 监控建议

### 1. 性能监控
- 监控文件传输耗时
- 观察内存使用情况
- 跟踪传输成功率

### 2. 错误监控
- 文件读取错误
- 网络传输错误
- 资源泄漏检查

## 总结

新的 `putFromFile` 实现：

1. **完全模仿 curl 行为**：直接文件传输，无中间缓冲
2. **解决阻塞问题**：绕过 RecordBuffer 容量限制
3. **提升性能**：恒定内存使用，支持大文件
4. **保持兼容性**：与现有功能完全兼容
5. **增强稳定性**：消除大文件处理的风险

这个实现确保了 StarRocks 连接器能够像 curl 一样稳定、高效地处理任意大小的数据文件。
