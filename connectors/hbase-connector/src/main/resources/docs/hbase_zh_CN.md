# HBase 连接器

HBase 是一个分布式、可扩展的大数据存储，基于 Google Bigtable 模型。此连接器使 Tapdata 能够读写 Apache HBase。

## 前置条件

- HBase 2.x 集群，ZooKeeper 可访问
- Tapdata 代理与 HBase/ZooKeeper 之间网络互通

## 连接配置

| 参数 | 说明 |
|---|---|
| ZooKeeper 集群地址 | ZooKeeper 地址，格式：`host1:2181,host2:2181,host3:2181` |
| ZooKeeper ZNode 父路径 | HBase 在 ZooKeeper 中的根节点，默认：`/hbase` |
| 用户名 | 可选，Hadoop Simple 认证用户 |
| 密码 | 可选，为未来 Kerberos 支持预留 |

## 数据模型

- HBase 表映射为 Tapdata 表
- 行键映射为 `row_key` 字段（主键）
- 源字段作为 qualifier 存储在可配置的列族中（默认：`cf`），遵循 HBase 每个表 2–3 个列族的最佳实践
- 读取多列族表时，额外的列族以 JSON 字段呈现（`qualifier: value` 键值对）

## 支持的操作

| 操作 | 支持情况 |
|---|---|
| 批量读取 | 支持，含断点续读 |
| 批量计数 | 支持 |
| 写入（增/改/删） | 支持 |
| 创建表 | 支持，使用单个可配置列族 |
| 删除表 | 支持 |
| 增量同步 (CDC) | 当前版本不支持 |

## 限制

- 当前版本不支持增量同步（CDC）
- 所有值均以字符串形式处理；二进制、数值、时间类型会被序列化为字符串
- 暂不支持 Kerberos 认证
- 不支持高级查询过滤
