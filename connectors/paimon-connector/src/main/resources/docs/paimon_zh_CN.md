# Apache Paimon 连接器

## 概述

Apache Paimon 是一个流式数据湖平台，支持高速数据摄取、变更数据捕获和高效数据查询。此连接器允许 Tapdata 将数据写入 Paimon 表。

## 支持的操作

- **连接测试**: 验证仓库可访问性和写入权限
- **模型加载**: 从 Paimon 加载表定义
- **创建表**: 创建新的 Paimon 表
- **删除表**: 删除 Paimon 表
- **清空表**: 删除表中的所有数据
- **写入记录**: 插入、更新和删除记录

## 配置说明

### 仓库路径
Paimon 存储数据的根路径。可以是:
- 本地文件系统: `/path/to/warehouse`
- HDFS: 将构造为 `hdfs://host:port/path`
- S3: `s3://bucket/path`
- OSS: `oss://bucket/path`

### 存储类型
选择 Paimon 的存储后端:
- **Local**: 本地文件系统（用于测试）
- **HDFS**: Hadoop 分布式文件系统
- **S3**: Amazon S3 或 S3 兼容存储
- **OSS**: 阿里云对象存储服务

### S3 配置
使用 S3 存储时:
- **S3 端点**: S3 服务端点（例如：https://s3.amazonaws.com）
- **S3 访问密钥**: AWS 访问密钥 ID
- **S3 密钥**: AWS 密钥
- **S3 区域**: AWS 区域（例如：us-east-1）

### HDFS 配置
使用 HDFS 存储时:
- **HDFS 主机**: NameNode 主机名
- **HDFS 端口**: NameNode 端口（默认：9000）
- **HDFS 用户**: HDFS 操作用户（默认：hadoop）

### OSS 配置
使用 OSS 存储时:
- **OSS 端点**: OSS 服务端点（例如：https://oss-cn-hangzhou.aliyuncs.com）
- **OSS 访问密钥**: 阿里云访问密钥 ID
- **OSS 密钥**: 阿里云访问密钥

### 数据库名称
Paimon 数据库名称（默认：default）

## 数据类型映射

| Paimon 类型 | Tapdata 类型 |
|-------------|--------------|
| BOOLEAN | TapBoolean |
| TINYINT | TapNumber |
| SMALLINT | TapNumber |
| INT | TapNumber |
| BIGINT | TapNumber |
| FLOAT | TapNumber |
| DOUBLE | TapNumber |
| DECIMAL | TapNumber |
| CHAR | TapString |
| VARCHAR | TapString |
| STRING | TapString |
| BINARY | TapBinary |
| VARBINARY | TapBinary |
| DATE | TapDate |
| TIMESTAMP | TapDateTime |
| TIMESTAMP_LTZ | TapDateTime |
| ARRAY | TapArray (以 JSON 字符串存储) |
| MAP | TapMap (以 JSON 字符串存储) |
| ROW | TapMap (以 JSON 字符串存储) |

## 限制

1. **读取操作**: 此连接器目前仅支持写入操作。不支持读取和 CDC 操作。
2. **索引**: Paimon 不支持传统索引。仅支持主键。
3. **模型变更**: 不支持运行时动态模型变更。

## 最佳实践

1. **主键**: 始终为表定义主键以实现高效的更新和删除。
2. **批量大小**: 使用适当的批量大小以获得更好的写入性能。
3. **存储选择**: 根据部署环境选择适当的存储后端。
4. **分区**: 对于大表，考虑使用 Paimon 的分区功能。

## 故障排除

### 连接问题
- 验证仓库路径可访问
- 检查存储凭据是否正确
- 确保与存储后端的网络连接

### 写入失败
- 验证仓库路径的写入权限
- 检查表模型是否与源数据匹配
- 查看 Paimon 日志以获取详细错误信息

### 性能问题
- 增加批量大小以提高吞吐量
- 为工作负载使用适当的存储后端
- 考虑启用 Paimon 的压缩功能

## 参考资料

- [Apache Paimon 文档](https://paimon.apache.org/)
- [Paimon GitHub 仓库](https://github.com/apache/paimon)

