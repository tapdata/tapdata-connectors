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

### 本地临时目录与 Spill 回收

连接器在 `diskTmpDir` 下按以下结构存放 Paimon 的本地 Spill 与索引文件，每个写入表各有独立的租约目录：

```text
<diskTmpDir>/tapdata-paimon-v2/<进程标识>/<租约>/owner.lock
<diskTmpDir>/tapdata-paimon-v2/<进程标识>/<租约>/io/paimon-io-<UUID>/
```

- **存活判定：** 只以操作系统文件锁判断目录是否仍在使用，不按修改时间推断。运行中任务的目录不会被其他任务或其他引擎删除。
- **正常停止：** 任务正常停止后，其目录全部删除，不留空目录。
- **异常退出：** 进程崩溃（OOM、`kill -9`）后遗留的目录，由同一台机器上下一次启动 Paimon 任务的引擎回收（包括重启后的同一引擎）；每次最多回收 64 个，其余留到下一次。
- **部署建议：** 每个引擎进程使用独占的 `diskTmpDir`。Kubernetes 使用每个 Pod 独立的 emptyDir 或独立的 subPath，Docker 使用独立的 volume。
- **网络或 FUSE 文件系统：** `diskTmpDir` 位于 NFS、CIFS、FUSE、Ceph 等文件系统时，跨主机文件锁不可靠，连接器不自动回收遗留目录，只记录告警。请改用本地磁盘，或按下方步骤手动清理。
- **升级：** 升级连接器后请重启引擎，避免同一进程内新旧版本并存。旧版本在 `diskTmpDir` 根下创建的 `paimon-io-*` 目录与 `.paimon-io-*.tapdata-owner.lock` 文件不会被新版本打开或删除。确认使用该目录的所有引擎都已升级并重启后，可以一次性删除：

  ```sh
  # 先列出，确认没有任何旧版本引擎仍在使用该目录后再删除
  find <diskTmpDir> -maxdepth 1 \( -name 'paimon-io-*' -o -name '.paimon-io-*.tapdata-owner.lock' \) -print
  find <diskTmpDir> -maxdepth 1 \( -name 'paimon-io-*' -o -name '.paimon-io-*.tapdata-owner.lock' \) -exec rm -rf {} +
  ```

- **排查：** 租约的创建、退役、保留、回收与跳过事件写入引擎日志，logger 名为 `io.tapdata.connector.paimon.spill`；任务日志只记录摘要。

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


### 本地临时目录与升级

建议每个引擎使用独占、持久且容量充足的本地临时目录。配置 `diskTmpDir` 时，同时检查
`fs.s3a.buffer.dir` 和 `fs.s3a.bucket.<bucket>.buffer.dir` 覆盖值。初始化写探针只验证
当时能写入一字节，不预留上传块，也不保证运行期间空间始终充足。

升级后必须重启引擎：旧任务的 ClassLoader 不遵守新增的回收同步协议。
`systemd-tmpfiles` 对 `/tmp` 的清理以及容器卷删除，都可能越过应用文件锁删除活跃目录。
请在主机或容器中配置临时目录的保留规则。NFS/FUSE 等不在准入白名单内的文件系统不自动回收。

旧平铺 `paimon-io-*` 目录只告警、不自动删除，升级不会自动释放这些空间。
先停止所有使用这些根目录的引擎和任务，再用
`find <root> -maxdepth 1 -type d -name 'paimon-io-*'` 列出候选，
用 `du -sh -- <specific-directory>` 检查体积，仅逐个删除已确认不再使用的目录。
引擎运行时不得删除 `tapdata-paimon-v2` 或其他仍在使用的临时目录。

启动最多等待回收 200ms；超时后后台扫描继续持锁完成，忙时不积压后续扫描。
回收 WARN/ERROR 同时转发到有界异步任务日志；日志后端阻塞或队列饱和仍可能丢失事件，
关闭摘要中的 `droppedLogs` 可辅助观察。容量或挂载问题需使用引擎账号，在相同挂载命名空间下
结合 `df -hT`、`df -i`、`findmnt -T <directory>`、`namei -l <directory>` 检查。
