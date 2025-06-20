# StarRocks 连接器限速功能调整

## 调整概述

将限速功能从"每小时多少GB"调整为"每分钟多少MB"，提供更精细的流量控制，同时保持默认不限速的设置。

## 调整内容

### 1. 配置项变更

#### 配置文件 (spec_starrocks.json)
```json
// 修改前
"hourlyLimitGB": {
  "title": "${hourlyLimitGB}",
  "default": 0,
  "tooltip": "${hourlyLimitGBTip}",
  "max": 10000
}

// 修改后
"minuteLimitMB": {
  "title": "${minuteLimitMB}",
  "default": 0,
  "tooltip": "${minuteLimitMBTip}",
  "max": 10000
}
```

#### 国际化文本
- **英文**: "Per-Minute Write Limit (MB)" - "Maximum data to write per minute (in MB)"
- **简体中文**: "每分钟写入限制 (MB)" - "每分钟最大写入数据量（单位：MB）"
- **繁体中文**: "每分鐘寫入限制 (MB)" - "每分鐘最大寫入數據量（單位：MB）"

### 2. 后端代码变更

#### StarrocksConfig.java
```java
// 修改前
private Integer hourlyLimitGB = 0;
public Integer getHourlyLimitGB() { return hourlyLimitGB; }

// 修改后
private Integer minuteLimitMB = 0;
public Integer getMinuteLimitMB() { return minuteLimitMB; }
```

#### 限制器类重命名
```java
// 修改前
HourlyWriteLimiter.java

// 修改后
MinuteWriteLimiter.java
```

### 3. 限制器逻辑调整

#### 时间单位变更
```java
// 修改前：基于小时
int hour = LocalDateTime.now().getHour();
if (hour != currentHour) {
    currentHour = hour;
    currentHourWritten.set(0);
}

// 修改后：基于分钟
int minute = LocalDateTime.now().getMinute();
if (minute != currentMinute) {
    currentMinute = minute;
    currentMinuteWritten.set(0);
}
```

#### 数据单位转换
```java
// 修改前：GB 转字节
this.hourlyLimitBytes = (long) hourlyLimitGB * 1024 * 1024 * 1024;

// 修改后：MB 转字节
this.minuteLimitBytes = (long) minuteLimitMB * 1024 * 1024;
```

#### 等待时间计算
```java
// 修改前：等待到下个小时
public long getMinutesToNextHour() {
    LocalDateTime nextHour = now.truncatedTo(ChronoUnit.HOURS).plusHours(1);
    return ChronoUnit.MINUTES.between(now, nextHour);
}

// 修改后：等待到下一分钟
public long getSecondsToNextMinute() {
    LocalDateTime nextMinute = now.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
    return ChronoUnit.SECONDS.between(now, nextMinute);
}
```

## 功能对比

### 调整前 (每小时GB)
- **时间粒度**: 1小时
- **数据单位**: GB
- **适用场景**: 粗粒度的长期流量控制
- **等待时间**: 最长59分钟
- **示例**: 每小时最多写入50GB

### 调整后 (每分钟MB)
- **时间粒度**: 1分钟
- **数据单位**: MB
- **适用场景**: 精细的实时流量控制
- **等待时间**: 最长59秒
- **示例**: 每分钟最多写入100MB

## 优势分析

### 1. 更精细的控制
```
调整前：每小时50GB = 平均每分钟853MB (但可能瞬间突发)
调整后：每分钟100MB = 严格控制每分钟流量
```

### 2. 更快的响应
- **调整前**: 超限后最长等待59分钟
- **调整后**: 超限后最长等待59秒

### 3. 更好的实时性
- 分钟级别的流量控制更适合实时数据处理
- 减少对下游系统的冲击
- 更容易进行容量规划

### 4. 更直观的配置
- MB/分钟比GB/小时更直观
- 更容易根据网络带宽进行配置
- 更适合现代高频数据处理场景

## 配置建议

### 1. 网络带宽配置
```
1Gbps 网络: 约125MB/s → 建议配置 7500MB/分钟 (约60%利用率)
100Mbps 网络: 约12.5MB/s → 建议配置 750MB/分钟 (约60%利用率)
10Mbps 网络: 约1.25MB/s → 建议配置 75MB/分钟 (约60%利用率)
```

### 2. 业务场景配置
```
实时同步: 50-200MB/分钟 (保证低延迟)
批量导入: 500-2000MB/分钟 (提高吞吐量)
备份场景: 100-500MB/分钟 (平衡性能和资源)
```

### 3. 系统资源配置
```
高性能环境: 1000-5000MB/分钟
标准环境: 100-1000MB/分钟
资源受限环境: 10-100MB/分钟
```

## 兼容性说明

### 1. 配置迁移
- 旧的 `hourlyLimitGB` 配置将被忽略
- 新部署默认使用 `minuteLimitMB = 0` (不限制)
- 需要手动重新配置限速参数

### 2. 行为变化
- 限速检查频率从每小时变为每分钟
- 等待时间从分钟级别变为秒级别
- 流量控制更加严格和及时

### 3. 监控调整
- 监控指标需要从小时级别调整为分钟级别
- 告警阈值需要相应调整
- 日志记录频率可能增加

## 测试建议

### 1. 功能测试
- 测试不同限速配置下的行为
- 验证超限后的等待和恢复机制
- 确认配置为0时不限速

### 2. 性能测试
- 测试高频小数据场景
- 测试低频大数据场景
- 验证限速对整体性能的影响

### 3. 稳定性测试
- 长时间运行测试
- 边界条件测试 (如分钟边界)
- 异常恢复测试

## 监控指标

### 1. 新增指标
- 每分钟实际写入量
- 限速触发频率
- 平均等待时间

### 2. 调整指标
- 将小时级别指标调整为分钟级别
- 增加秒级别的精细监控
- 添加限速效果评估指标

## 总结

这次调整实现了：

1. **更精细的流量控制**: 从小时级别提升到分钟级别
2. **更快的响应速度**: 等待时间从分钟级别降低到秒级别
3. **更直观的配置**: MB/分钟比GB/小时更容易理解和配置
4. **更好的实时性**: 适合现代高频数据处理需求
5. **保持向后兼容**: 默认不限速，不影响现有用户

调整后的限速功能更适合实时数据处理场景，提供了更精确和及时的流量控制能力。
