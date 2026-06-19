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
| 用户名 | 可选，用于 Kerberos 认证 |
| 密码 | 可选，用于 Kerberos 认证 |

## 数据模型

- HBase 表映射为 Tapdata 表
- 行键映射为 `row_key` 字段（主键）
- 每个列族映射为一个字段，值为 `qualifier: value` 的 JSON 对象

## 限制

- 当前版本不支持增量同步（CDC）
- 所有值均以字符串形式处理
