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

### 写缓冲磁盘溢写

此开关直接对应 Paimon 1.3.2 的 `write-buffer-spillable`：开启传入 `true`，关闭、未配置或显式 null 均传入 `false`。

- **新建表：** 开关优先于自定义表属性和 Catalog 默认值。
- **已有表：** 只覆盖当前任务的写入参数，不将 Spill 选项写回共享表配置，也不改变其他任务或读取参数。
- **生效时机：** 重启任务、重新创建写入上下文后应用，不支持运行中热切换。
- **磁盘容量：** 已有表继续使用自身的 `write-buffer-spill.max-disk-size`，界面容量不会覆盖它。新表开启时沿用现有容量配置及自定义属性优先级。原生容量限制不是整个任务的磁盘总配额。

关闭该开关不等于禁止全部本地磁盘访问。常规主键写缓冲按此参数执行；Append/Postpone 达到原生 Writer 数量阈值后可能进入强制缓冲路径，Compaction、动态索引及其他临时文件仍按 Paimon 原生行为处理。

旧版本中关闭开关可能仍使用原生默认 `true`。本次修正后，重启任务会实际传入 `false`，可能改变内存使用、flush 频率和文件数量；不保证生产吞吐保持不变。临时目录仍须等相关使用者退出后才清理，超时则按既有规则保留。

### 原生维护模式

`snapshot.expire.execution-mode` 由 Paimon 解释：SYNC、ASYNC 均可使用，连接器不强制设置模式、不 ALTER 已有表，也不自行执行或重试维护。未配置时使用 Catalog 和 Paimon 的默认值。历史已改为 SYNC 的表不会自动恢复；连接器的 `enableAsyncCommit` 是另一个独立参数。

停止时先等待原生维护主执行器退出，再关闭提交资源；超时或关闭失败保留资源。已知限制 B1：原生异常路径可能遗留共享线程池中的派生任务，主执行器退出并不证明这些任务全部退出。

依据：[Paimon 1.3 配置说明](https://paimon.apache.org/docs/1.3/maintenance/configurations/#write-buffer-spillable)。

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
| TIME(0-3) | TapTime |
| TIMESTAMP | TapDateTime |
| TIMESTAMP_LTZ | TapDateTime |
| STRING | TapArray (以 JSON 字符串存储) |
| STRING | TapMap (以 JSON 字符串存储) |
| STRING | TapRow/TapRaw (以 JSON 字符串存储) |

`INT` 和 `INTEGER` 都创建 Paimon `INT`。不带参数的 `DECIMAL` 使用 `DECIMAL(38,10)`；
不带参数的 `TIME` 使用 `TIME(3)`，仅支持 0-3 位精度，包含亚毫秒精度的输入会被拒绝。

DECIMAL 值必须精确符合物理列的 precision 和 scale。精度溢出或缩减小数位数会改变数值时，写入失败；连接器不会执行 `HALF_UP` 舍入。例如，写入 `DECIMAL(5,2)` 时，`1.235` 会被拒绝，`1.2300` 则可作为 `1.23` 写入，因为去除尾随零不会改变数值。

连接器不会自动迁移已有目标列：历史 STRING 列仍按实际物理类型写入，原生 Paimon
ARRAY/MAP/ROW/MULTISET/VARIANT 目标列会被拒绝。复杂 CDC 值必须以 JSON 写入 STRING 列。

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

### 磁盘临时目录

`diskTmpDir` 必须填写本机绝对路径，例如 `/data/tapdata/paimon-tmp`。
多个目录可用逗号分隔；目录项两侧的空白会被去除，空目录项会被拒绝。
禁止填写相对路径、URI 或 Paimon 表属性（例如 `'write-buffer-size' = '128mb'`）。
连接、任务节点及表级覆盖配置均需检查；表级配置可覆盖全局目录。
非法配置会在创建 Catalog 和执行临时目录清理之前报错，不会自动回退到工作目录。
默认值仍为 `/tmp`；显式置空时使用 `java.io.tmpdir`，该系统属性缺失或为空时使用 `/tmp`。
已有的相对路径配置需要改成绝对路径。该修复不会自动搬迁或删除历史错误目录。

## 参考资料

- [Apache Paimon 文档](https://paimon.apache.org/)
- [Paimon GitHub 仓库](https://github.com/apache/paimon)

## 自定义 SQL 查询校验（executeCommand）

`executeCommand` 的 `executeQuery` 命令支持以下单表只读子集，复用 Paimon Core 和 JSqlParser，不引入 Flink、Spark 或其他 SQL 引擎：

- `SELECT *`、普通列投影、列别名。
- `COUNT(*)`、`COUNT(1)`、`COUNT(col)`、`SUM`、`MIN`、`MAX`、`AVG`；不支持 DISTINCT 聚合。`SUM/AVG` 只接受数值列，DECIMAL 全程使用 BigDecimal；AVG 按 DECIMAL128 精度舍入。空输入的 COUNT 为 0，其余聚合为 NULL；分组查询的空输入返回空结果集。
- 多列 `GROUP BY`，以及 `CAST(timestamp_column AS DATE)` 日期投影、分组。日期转换仅接受 DATE 或无时区 TIMESTAMP，不隐式选择时区。
- 按已选择的表达式、别名、列序号 `ORDER BY`，支持 ASC/DESC、NULLS FIRST/LAST；默认 ASC 时 NULL 在前、DESC 时 NULL 在后。排序仅处理受结果行数限制的最终结果。
- 原有 WHERE 子集：列与常量比较、BETWEEN、IN、IS NULL、有限的 LIKE、AND/OR/NOT。WHERE 下推至 Paimon，并由 reader 精确执行；读取投影保留 SELECT、GROUP BY、WHERE 所需的列。

不支持 JOIN、通用子查询、WITH、UNION、HAVING、窗口函数、通用算术表达式、分页、表别名或带数据库/schema 前缀的表名。不支持的语法明确报错，不会静默忽略。历史 `COUNT(1) FROM (SELECT * ...)` 计数包装继续支持，历史直接 COUNT(1) 和计数包装仍返回 Long；其他查询返回 `List<Map<String,Object>>`，建议为所有表达式指定唯一别名。

客户示例可写成以下形式；SQL Server 源端也使用相同的四个别名，避免两种驱动对未命名表达式返回不同字段名：

```sql
SELECT CAST(CreationTime AS DATE) AS creation_date,
       COUNT(*) AS row_count,
       SUM(TotalValues) AS total_values,
       SUM(PointsAfterRedemption) AS points_after_redemption
FROM DRSRedemptions
WHERE TransactionDatetime BETWEEN '2025-01-01 00:00:00' AND '2025-07-01 00:00:00'
  AND op <> 'd'
GROUP BY CAST(CreationTime AS DATE)
ORDER BY CAST(CreationTime AS DATE)
```

### 执行限制

新增投影、聚合、排序查询默认要求分区列或前导主键上的比较、BETWEEN 或 IN 过滤。如果客户表的 `TransactionDatetime` 不符合要求，需在 **Paimon 连接配置**启用“允许无分区或前导主键过滤的自定义查询”，对应 `queryAllowFullScan=true`；直接调用 API 时也可以在命令参数传 `allowFullScan=true`。命令参数优先于连接配置。历史 `SELECT *` 和直接/包装 COUNT(1) 保持允许无键过滤的行为，但仍应用下面的资源限制。

`executeQuery` 命令参数（与 `sql` 一起放在 `params`）：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `queryTimeoutSeconds` | 300 | 正整数秒；在 catalog/规划/批次/行/结果阶段检查的协作式截止时间 |
| `maxScanRows` | 10000000 | reader 实际交付给连接器的最大行数，**不是底层文件物理扫描行数** |
| `maxGroups` | 100000 | 最大聚合分组数，按组保存状态，不缓存全部明细行 |
| `maxResultRows` | 1000 | 最大结果行数；不会将被截断结果当成成功返回 |
| `allowFullScan` | 连接配置，默认 false | 是否允许没有上述键过滤的新增查询 |

数值参数必须是范围内的正整数；0、负数、小数、溢出值均拒绝。任一限制触发，整次命令失败，不返回部分结果。reader 和 batch 在异常路径也会释放。日志记录计划 splits 数、投影列数、reader 交付行数、有效行数、分组数和耗时。

存在键过滤只表示可能裁剪，不保证实际裁剪。非键过滤、宽范围过滤仍可能读取大量文件；聚合成本取决于过滤后行数和分组数。行数、分组数限制不等于精确的内存字节上限。超时不能强制中断已阻塞的 Paimon/文件系统 I/O，也不能代替存储端 I/O 超时；不会启动超时后仍在后台继续扫描的查询线程。需要用客户表布局、存储和代表性数据评估吞吐及裁剪效果。
