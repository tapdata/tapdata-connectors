# 飞书表格批量插入记录使用指南

## 概述

本文档介绍如何使用飞书表格连接器的批量插入功能。我们提供了两种批量插入方法：

1. **完整版批量插入** (`batchInsertRecords`) - 支持重复检查和自动更新
2. **简化版批量插入** (`simpleBatchInsertRecords`) - 仅执行插入操作，性能更好

## API 接口

### 完整版批量插入

```javascript
function batchInsertRecords(connectionConfig, nodeConfig, eventDataList, settings)
```

**参数说明：**
- `connectionConfig`: 连接配置对象
- `nodeConfig`: 节点配置对象  
- `eventDataList`: 事件数据列表，每个元素包含 `afterData` 字段
- `settings`: 表格设置，JSON字符串格式，包含主键信息

**返回值：**
```javascript
{
    success: boolean,           // 操作是否成功
    message: string,           // 操作结果描述
    results: Array,            // 详细结果数组
    insertedCount: number,     // 插入的记录数量
    updatedCount: number       // 更新的记录数量
}
```

### 简化版批量插入

```javascript
function simpleBatchInsertRecords(connectionConfig, nodeConfig, recordsData)
```

**参数说明：**
- `connectionConfig`: 连接配置对象
- `nodeConfig`: 节点配置对象
- `recordsData`: 记录数据数组，每个元素是要插入的字段数据

**返回值：**
```javascript
{
    success: boolean,           // 操作是否成功
    message: string,           // 操作结果描述
    insertedCount: number,     // 插入的记录数量
    data: Object              // 飞书API返回的原始数据
}
```

## 使用示例

### 示例1：完整版批量插入（推荐用于需要去重的场景）

```javascript
// 准备事件数据列表
let eventDataList = [
    { afterData: { "姓名": "张三", "年龄": 25, "部门": "技术部", "邮箱": "zhangsan@example.com" } },
    { afterData: { "姓名": "李四", "年龄": 30, "部门": "产品部", "邮箱": "lisi@example.com" } },
    { afterData: { "姓名": "王五", "年龄": 28, "部门": "设计部", "邮箱": "wangwu@example.com" } }
];

// 设置主键（用于重复检查）
let settings = JSON.stringify({ 
    keys: ["姓名"]  // 以姓名作为主键，如果记录已存在则更新
});

// 执行批量插入
let result = batchInsertRecords(connectionConfig, nodeConfig, eventDataList, settings);

// 处理结果
if (result.success) {
    console.log(`成功插入 ${result.insertedCount} 条记录，更新 ${result.updatedCount} 条记录`);
} else {
    console.error(`批量插入失败: ${result.message}`);
}
```

### 示例2：简化版批量插入（推荐用于纯插入场景）

```javascript
// 准备记录数据
let recordsData = [
    { "姓名": "张三", "年龄": 25, "部门": "技术部", "邮箱": "zhangsan@example.com" },
    { "姓名": "李四", "年龄": 30, "部门": "产品部", "邮箱": "lisi@example.com" },
    { "姓名": "王五", "年龄": 28, "部门": "设计部", "邮箱": "wangwu@example.com" }
];

// 执行批量插入
let result = simpleBatchInsertRecords(connectionConfig, nodeConfig, recordsData);

// 处理结果
if (result.success) {
    console.log(`成功插入 ${result.insertedCount} 条记录`);
} else {
    console.error(`批量插入失败: ${result.message}`);
}
```

## 性能建议

1. **批量大小**: 建议每次批量插入不超过 500 条记录，以避免API超时
2. **数据格式**: 确保字段名称与飞书表格中的列名完全匹配
3. **错误处理**: 始终检查返回结果的 `success` 字段，并处理可能的错误
4. **选择合适的方法**:
   - 如果需要避免重复数据，使用 `batchInsertRecords`
   - 如果确定数据不重复，使用 `simpleBatchInsertRecords` 获得更好性能

## 注意事项

1. **权限要求**: 确保飞书应用具有对目标表格的写入权限
2. **字段类型**: 数据类型需要与表格字段类型匹配
3. **必填字段**: 确保所有必填字段都有值
4. **API限制**: 遵守飞书API的调用频率限制

## 错误处理

常见错误及解决方案：

- **权限不足**: 检查应用权限配置
- **字段不匹配**: 确认字段名称和类型正确
- **数据格式错误**: 检查数据格式是否符合要求
- **API限制**: 减少批量大小或增加调用间隔

## API 配置

批量插入功能使用以下飞书API：

```
POST /open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create
```

该API已在 `postman_api_collection.json` 中配置为 `batchAddRows`。
