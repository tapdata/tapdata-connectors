# Paimon Connector 微批提交与多表 at-least-once Spec

## 1. 文档状态

- 状态：Approved for Planning，方案 B 与本版 Spec 已确认；生产实现仍须等待 `tasks/plan.md` 和 `tasks/todo.md` 审阅批准。
- 本文只定义需求、边界、状态机、失败语义和验收标准；Plan/Todo 获得用户批准前不得进入实现。
- 目标分支：`jira/hotfix-paimon-connector`。
- 当前基线：`ddb1e7d7bb3c94468447fcfca9d0fb283062703c`。
- 允许改动范围：`connectors/paimon-connector/`。
- 禁止改动范围：Tapdata 引擎、PDK/common-lib、Apache Paimon 源码及其他 connector。

## 2. 已确认合同

以下条目已经在采访中确认，属于实现必须满足的合同，不是待定方案：

1. Connector 只保证 at-least-once，不声明 exactly-once。
2. at-least-once 必须覆盖单个 Paimon 目标任务中的多表写入，以及来自不同源节点的独立 offset 进度。
3. 不修改引擎源码、PDK/common-lib 或公共接口，也不改变引擎的事件分组、并发、Heartbeat 生成、DDL 和停止协议。方案 B 通过 Paimon Connector 的静态 capability 选择引擎既有的 callback 分支；由此造成的 CDC offset 改为 Connector 管理，以及第 11 条限定的纯 INITIAL_SYNC 恢复行为变化，是唯一获准的引擎运行分支变化。
4. CDC 必须跨多次 `writeRecords` 累积，并按表独立触发 Paimon snapshot 提交；不得继续每次调用都提交。
5. 保留默认值：`batchAccumulationSize=100000`、`commitIntervalMs=30000`、`enableAsyncCommit=true`。
6. 必须保留定时扫描。低流量表在没有下一次 `writeRecords` 时，也必须由后台扫描达到时间阈值后提交。
7. initial batch 的到达、记录数和停留时间不得触发 CDC 数量阈值或 scheduler 提交；每张表只在 `afterInitialSync` 强制提交。`afterInitialSync` 成功完成时间作为该表最近一次成功 snapshot 时间；INITIAL_SYNC_CDC 任务若在此后空闲超过 `commitIntervalMs`，首个小 CDC batch 在当前调用内立即提交，这是已确认的时间基准合同，不属于 initial buffer 参与 CDC 阈值。
8. DDL 前只强制 drain 目标表；其他表继续累积。停止时强制 drain 全部表。
9. 任一不可安全继续的写入、提交、offset 回调或后台任务失败都必须形成首个粘滞故障，阻止后续 offset 前移，并通过引擎可见入口传播。
10. 当前 `PaimonTableWriteContext`、稳定 `commitUser`、pending commit 重试和状态恢复能力必须保留；不得恢复旧版直接管理 `StreamTableWrite`/`StreamTableCommit` 的实现。
11. 采用方案 B：在 `spec.json` 全局恢复 `flush_offset_callback`，只改 paimon-connector，并接受纯 INITIAL_SYNC 在任务全部完成前发生任何重新启动——包括进程崩溃、强制停止、正常手动停止后再启动——都可能从更早 batch offset、最坏从初始位置重放。append-only 目标允许因此产生重复数据和额外 snapshot，但绝不允许跳过未完成数据；该行为只保证 at-least-once。当前引擎不把 initial `batchOffset` 注入目标 `TapRecordEvent.info`，Connector 不得伪造或回调不存在的 initial offset。对 INITIAL_SYNC_CDC，`afterInitialSync` 只完成 Paimon commit，其后首个有效 CDC Heartbeat 才推进 stream offset；该 Heartbeat 前重启同样允许重放 initial。
12. 没有 offset 回调的源端使用、连接测试和元数据操作必须继续可用；第一次收到携带有效源 offset 的目标 CDC DML 或有效 CDC Heartbeat 时，回调仍不可用则必须在写入任何行或登记屏障之前失败。
13. 除第 3、11 条明确批准的 callback 分支切换和纯 INITIAL_SYNC 完成前重启语义外，不得改变 Paimon Connector 的其他当前功能。实现只能修改、加强和完善 CDC 微批提交，以及为其正确性必需的 offset 屏障、定时触发、pending commit 确认、失败传播和关闭协作；禁止借机重构或改变不相关行为。
14. 本文所称“与历史 Feature 功能等价”，只指在非空 CDC batch 和有效、非 `null` 配置下恢复同表跨调用累计、数量阈值、调用内时间阈值及无后续写入时的后台时间提交能力；不表示逐行、逐时序或错误行为等价。除 `DEC-02` 明确接受的纯 INITIAL_SYNC 完成前恢复进度变化外，第 4.6 节列出的 initial 污染、固定相位延迟、`null` 边界、CDC offset 提前或丢失、失败重放和关闭错误必须修正，禁止为追求 bug-for-bug 等价而恢复。

产品决定证据：`DEC-01`、`DEC-02`。源码可行性与限制证据：`C-01`～`C-10`、`E-01`～`E-07`、`P-01`、`M-01`～`M-02`。

## 3. 目标与非目标

### 3.1 目标

在不修改引擎源码和公共接口的前提下，通过方案 B 选择引擎既有 callback 分支，恢复并重写 Paimon Connector 的 CDC 微批提交，使小批量、高频 `writeRecords` 不再一调用一 snapshot，同时满足以下结果：

- 同表跨调用累计；多表分别累计、分别提交。
- 数量阈值、调用内时间检查、后台时间扫描三种触发方式共同生效。
- Paimon snapshot 成功之前绝不回调覆盖该数据的源 offset。
- Heartbeat 只有在其之前、同一源节点涉及的所有目标表数据均已成功提交后才允许回调。
- 进程停止和目标表 DDL 不遗留未处理的 writer buffer 或 pending commit。
- 异步线程中的失败不会只写日志；任务在下一次可传播入口确定失败。

### 3.2 非目标

- 不提供 Paimon snapshot 与 Tapdata offset 的跨系统原子事务。
- 不提供 exactly-once；Paimon 成功、offset 持久化失败时允许源端重放。
- 不提供跨 JVM 的多 writer 协调或分布式锁。
- 不把多张 Paimon 表的 snapshot 合并为跨表原子提交。
- 不修改引擎的 HashMap 分组顺序、分区并发处理器、Heartbeat 生成方式或 snapshot 保存方式。
- 不改变既有 bucket mode、主键、Schema、DDL、spill、compaction 或读取语义。
- 不改变现有 DML 映射、类型转换、默认值处理、表/字段命名、动态 bucket 路由、commit state 格式、Catalog options/storage/CatalogFactory 语义、连接测试、元数据发现、源端读取和错误码口径；本 Spec 明确列出的微批正确性修复除外。
- 不在 Connector 内恢复纯 INITIAL_SYNC 的 batch offset 持久化，也不承诺其完成前重启时的断点续传效率；该限制已经作为方案 B 的明确取舍被接受。

### 3.3 最小改动预算

实现允许触碰的行为范围只包括：

1. 在 `spec.json` 恢复静态 `flush_offset_callback`，由 `PaimonConnector` 注入 callback 并转发 CDC Heartbeat；纯 INITIAL_SYNC 的恢复行为严格采用第 2 节第 11 条已接受边界，不实现 connector 侧补偿。
2. 重写 CDC 微批累计、数量/时间触发、单线程 scheduler 和 per-table generation 状态。
3. 把现有 pending commit 确认接入所有微批强制提交入口，并修正其线程安全、失败传播和 source batch 不重放语义。
4. 让既有 `afterInitialSync`、目标表 DDL drain 和 `close` 与微批状态正确协作；它们原有业务时机、作用表范围和 Paimon 操作仍保持不变。
5. 增加仅覆盖上述行为的内部协调类、测试、日志和配置文案修正。

以下改动一律超出范围：修改非 Paimon Connector 模块；替换当前 `PaimonTableWriteContext`/commit state store；改变记录转换、writer/router、bucket、Schema/DDL action、Catalog options/storage/CatalogFactory 语义、读端或元数据路径；升级依赖；改变公共 PDK API；与本 Feature 无关的清理、重命名或格式化。若实现发现必须越过该预算，必须停止并先修订 Spec 交用户确认。

证据链：`C-03`～`C-09`、`E-07`、`DEC-01`、`DEC-02`。

## 4. 事实基线

### 4.1 版本边界

| 组件 | 已核实版本 | 本 Spec 使用的事实 |
| --- | --- | --- |
| tapdata-connectors | `ddb1e7d7bb3c94468447fcfca9d0fb283062703c` | 当前实现基线 |
| 用户指定提交 | `966447b29f72f0260a318853d201766ba9b99251` | 该提交未改动 `connectors/paimon-connector` |
| 指定提交父提交 | `821ff7e33633a54e6abb5f0919d505d42e1098a5` | 被称为“旧实现”的 Paimon 微批代码所在状态 |
| Tapdata 引擎 | `f91bfe4a66ea99362440ca87c36b4c1883ca4cd9` | offset callback、Heartbeat 和并发水位屏障事实 |
| tapdata-common-lib | `c4cab105152c04ef01e50e586cc616d855e48ed7` | PDK/common-lib 接口基线 |
| Apache Paimon | `release-1.3.1` / `28dfdfed24877c5f4c36b7c2409794fc8ef79607` | `filterAndCommit`、snapshot 和提交器语义 |

本次取证使用的只读仓库根目录：

- tapdata-connectors：`/Users/SL/javaProject/tapdata-connectors`
- Tapdata 引擎：`/Users/SL/javaProject/tapdata`
- tapdata-common-lib：`/Users/SL/javaProject/tapdata-common-lib`
- Apache Paimon：`/Users/SL/javaProject/paimon`

核实命令：

```bash
git diff --quiet \
  821ff7e33633a54e6abb5f0919d505d42e1098a5 \
  966447b29f72f0260a318853d201766ba9b99251 \
  -- connectors/paimon-connector
```

该命令返回 `0`，因此不得表述为“`966447b...` 新增或删除了 Paimon 微批特性”。准确表述是：该提交前后的 Paimon 模块相同；旧特性存在于其父提交所代表的仓库状态。

### 4.2 源码证据链索引

证据格式固定为“锁定 revision → 文件与行号 → 可直接观察的事实 → 支撑的合同”。行号均以第 4.1 节锁定 revision 为准；实现期间若基线变化，必须重新取证，不能沿用失效行号。`DEC-*` 表示用户确认的产品决定，不伪装成源码事实。

| ID | revision 与源码锚点 | 可直接观察的事实 | 支撑的合同 |
| --- | --- | --- | --- |
| `H-01` | `821ff7e...`，`PaimonService.java:91-103,199-232`；命令：`git show 821ff7e33633a54e6abb5f0919d505d42e1098a5:connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java \| nl -ba` | 旧状态存在按表计数、提交时间、表锁、单线程 `scheduleAtFixedRate` 扫描。 | 历史微批 Feature 确实存在；需要保留后台时间触发。 |
| `H-02` | `821ff7e...`，同文件 `970-1084` | 旧 `writeRecords` 在 stage 判断前对所有 batch 跨调用累加记录数，并按 `size<=0`、数量阈值、时间阈值判断；命中阈值后只有首条事件 stage 为 `CDC` 才在该路径提交，非 CDC 直接返回且不清计数。 | 历史微批触发能力确实存在；旧实现没有正确隔离 initial/CDC 计数。 |
| `H-03` | `821ff7e...`，同文件 `2019-2049,2915-2959` | `flushTable` 用 `tableKey` 调用 callback，而 offset Map 以表名存储；`close` 吞 flush 失败；callback 可能在队头表未提交时回调并吞异常。 | 旧 key、offset 顺序、失败传播和关闭实现不得复用。 |
| `H-04` | `821ff7e...`，同文件 `943-959,970-992,1347-1373` | 旧 `afterInitialSync` 把 `tapTable.getId()` 直接作为 writer/committer 缓存键；`writeRecords` 使用 `database.tableName`，而缓存 helper 按传入字符串精确取值。因此两条路径创建或复用不同的 writer/committer，`afterInitialSync` 不能提交 `writeRecords` 已累计的 writer buffer。 | initial 必须使用当前规范 tableKey/context，且提交同一表唯一写入上下文。 |
| `H-05` | `821ff7e...`，同文件 `148-170,199-232,943-958,1029-1078` | scheduler 在 Service 初始化时按全局固定相位 `scheduleAtFixedRate(interval, interval)`；表的 `lastCommitTime` 在第一批被累计的数据到达后初始化，成功 CDC commit 后更新，但 `afterInitialSync` 成功后不更新也不清累计计数。 | 保留后台时间提交能力；修正固定相位额外等待和 initial 对 CDC 计数/时间基准的污染。 |
| `H-06` | `821ff7e...`，同文件 `984-1105,1139-1145,1151-1178` | write、prepare 或 commit 抛错后，旧外层循环重建 Catalog 并重新执行整个 batch，最多重试 3 次；已经进入 writer 的 source batch 可能被再次写入。重建会先清空累计计数，因此计数从零重新累计而不是在原值上翻倍，但部分或全部成功的数据仍可能被重写/重提交。 | 新实现只能确认同一 pending commit，不能重放已经进入 writer 的 source batch。 |
| `H-07` | `821ff7e...`，`PaimonConnector.java:489-508`；`spec.json:11-22` | 旧 connector 全局声明 `flush_offset_callback`；Heartbeat callback payload 会把 `HeartbeatEvent.referenceTime` 写入 `TapCallbackOffset.eventTime`。 | 新实现必须保留 CDC Heartbeat 的 nullable eventTime；方案 B 恢复全局 capability，但不得复用旧 callback 顺序错误。 |
| `C-01` | `ddb1e7d...`，[`PaimonConfig.java`](../src/main/java/io/tapdata/connector/paimon/config/PaimonConfig.java) `87-97`；[`spec.json`](../src/main/resources/spec.json) `491-542,694-697,751-754,808-811` | Java 字段默认值为 `100000/30000/true`，JSON 默认值一致；但 `PaimonConfig.java:87` 注释和 `spec.json` 三种语言 placeholder 仍写 `10000`。 | 保留默认值并同时修正 Java 注释和三种 JSON 文案。 |
| `C-02` | `ddb1e7d...`，[`PaimonService.java`](../src/main/java/io/tapdata/connector/paimon/service/PaimonService.java) `82-87,140-153,210-214,272-320,1116-1147,1179-1208,1227-1248,1261-1318,1339-1369,2329-2363,3327-3382`；[`spec.json`](../src/main/resources/spec.json) `11-20` | 固定 false 开关跳过 scheduler，CDC 每次调用提交，setter 丢弃 callback，capability 缺失。旧 `firstOffsetByTable`、`committedOffsetTables`、`offsetCallbackLock`、公开 getter、写入收集分支、各提交入口的调用点和 `commitCallback` 仍完整存在，但生产 setter 始终令 callback 为 `null`：受 `flushOffsetCallback != null` 保护的 Map 写入不可达，`commitCallback` 也立即返回。 | 当前没有对等微批功能；方案 B 必须消除旁路、删除而不是复活旧表序 callback 机器，并接通新的 capability/generation 屏障。当前生产路径不存在 `firstOffsetByTable` 无界增长。 |
| `C-03` | `ddb1e7d...`，`PaimonService.java:108-165,986-1015,2318-2371,3288-3325` | 当前实现已有每表唯一 context、表锁、DDL 目标表 drain、sticky failure、close 异常聚合；`flushTable` 仍以 count 为零直接返回。 | 保留当前安全基础；补足全部阶段 writer buffer 可见性、pending commit、scheduler 与生命周期竞态，避免 initial 不计入 CDC 阈值后被 DDL/stop 漏 flush。 |
| `C-04` | `ddb1e7d...`，[`PaimonTableWriteContext.java`](../src/main/java/io/tapdata/connector/paimon/service/PaimonTableWriteContext.java) `185-247` | `commit()` 先保存 pending messages，再委托 `retryPendingCommit()` 间接调用 `filterAndCommit`；有 pending 时，后者接受返回 `0..size`，清 pending、推进 identifier 并保存状态；无 pending 时直接返回 `nextCommitIdentifier-1`，不修改或保存状态。 | 微批协调器不得替换 context 的 pending commit/retry 语义，也不得把空 pending 的幂等 no-op 当作新 commit 成功。 |
| `C-05` | `ddb1e7d...`，[`PaimonCommitStateStore.java`](../src/main/java/io/tapdata/connector/paimon/service/PaimonCommitStateStore.java) `23-33,54-94,97-124` | 状态只保存 stable commit user/next identifier；不保存 source offset/CommitMessages，也没有事务型 offset binding。 | 只能声明 at-least-once；重启恢复继续使用现有对账。 |
| `C-06` | `ddb1e7d...`，[`PaimonConnector.java`](../src/main/java/io/tapdata/connector/paimon/PaimonConnector.java) `56-76,83-97,173-188,366-395,484-488` | 当前 `onStart` 不注入 callback；`onStop` 已传播 close 错误；注册了 `processControl/afterInitialSync`，但 `processControl` 是 no-op。 | Connector 只负责接线/传播，Service 实现提交和屏障。 |
| `C-07` | `ddb1e7d...`，`PaimonService.java:1179-1208,1339-1369` | 当前写入路径把 `maxRetries` 固定为 `3`；commit 结果不明且 context 有 pending 时，重试同一 pending messages，确认当前 batch 的 pending 成功后直接返回而不重放该 batch。 | 新协调器必须把“原失败调用后最多 3 次同 pending 确认”作为 commit 内部协议，并覆盖所有 commit 入口。 |
| `C-08` | `ddb1e7d...`，`PaimonService.java:643-1015,1071-1820`；`PaimonWriteSemanticContractResolver.java:31-388`；`PaimonTableWriteContextFactory.java:32-220`；`PaimonBucketWriterStrategyFactory.java:19-71` | 当前目标端已有建表/DDL、整批 preflight、DML image 校验、表语义解析、bucket strategy/context factory 和唯一 table context 写入链。 | 本 Feature 只能改变 commit 时机及其协调，不能改变记录转换、Schema/DDL action、bucket、writer/router 或 context 创建合同。 |
| `C-09` | `ddb1e7d...`，`PaimonConnector.java:145-190,310-370`；`PaimonService.java:2495-2849,2860-3000` | Connector 独立注册 schema discovery、batch/stream read、query capabilities；Service 具有独立的 Paimon batch/stream read 路径。 | callback/micro-batch 改动不得侵入源端读取、连接测试或元数据能力。 |
| `C-10` | `ddb1e7d...`，`PaimonService.java:1116-1147,1261-1318` | 当前 initial/CDC 写入仍先增加共用计数；`afterInitialSync` 成功后会清计数并更新时间，CDC 则被固定 false 旁路强制每次 commit 后清计数。 | 当前 initial 状态清理优于旧实现但仍无 CDC 微批；新实现必须保留当前 `afterInitialSync` 成功清理并进一步在计数入口隔离 stage。 |
| `E-01` | `f91bfe4...`，`HazelcastTargetPdkDataNode.java:752-764` | 引擎用 `HashMap` 按目标表分组并 `forEach(writeRecord)`。 | 不得把表调用/Map 插入顺序当作源 offset 顺序。 |
| `E-02` | `f91bfe4...`，`PartitionConcurrentProcessor.java:183-203,240-268,297-334` | watermark runner 等待各分区处理并 countDown 后才调用 `flushOffset.accept`。 | Heartbeat 到 Connector 时，此前已分派 DML 的 `writeRecords` 已完成；可建立 generation 屏障。 |
| `E-03` | `f91bfe4...`，`HazelcastTargetPdkBaseNode.java:1744-1751,1875-1929` | capability 开启 connector-managed offset；Heartbeat metadata 被传入 `processControl`；普通 DML 在 callback 模式下不自动推进 offset。 | Heartbeat 是 CDC offset 屏障，DML commit 不直接 callback。 |
| `E-04` | `f91bfe4...`，同文件 `325-375,1932-1953,1995-2045` | callback 用 `nodeIds[0],targetNode` 选择进度槽，读取 `TapCallbackOffset.eventTime` 并通过临时 `TapRecordEvent.referenceTime` 更新 `SyncProgress.eventTime`，再 `saveToSnapshot`；Connector 只拿到无返回值的 Consumer。`flushOffsetCallback` 对缺失 stage/offset 或 CDC `sourceTime` 的输入只返回 `false`，调用方不检查该返回值。 | 不同 sourceLane 独立；eventTime 必须原样传递；callback 没有持久化 ack；Connector 必须在登记 CDC 屏障前完整校验 payload，不能依赖引擎 callback 拒绝坏数据。 |
| `E-05` | `f91bfe4...`，同文件 `1467-1484` | 目标 DML info 只注入 `streamOffset/syncStage/sourceTime/nodeIds`，没有注入 `batchOffset`。 | Connector 不得实现或声称 initial batch offset callback。 |
| `E-06` | `f91bfe4...`，`HazelcastSourcePdkDataNode.java:343-356,875-905,952-960,1425-1484`；`HazelcastSourcePdkBaseNode.java:708-741,1429-1459`；`TapdataHeartbeatEvent.java:19-42`；`HazelcastTargetPdkBaseNode.java:1656-1665,2437-2457`；`HazelcastTargetPdkDataNode.java:1417-1439`；`PartitionConcurrentProcessorTest.java:203-238` | source 先 enqueue CompleteSnapshot，之后才调用 `doCdc()`；`doCdc()` 先 enqueue StartingCDC，随后才进入 polling/normal CDC。polling Heartbeat 在 CDC loop 内生成，normal CDC Heartbeat 也在 CDC wrapper 中转换，factory 固定 stage 为 CDC。INITIAL_SYNC_CDC 初始化阶段的早期 Heartbeat 被显式改成 INITIAL_SYNC。target 收到 CompleteSnapshot 后并发调用每表 `afterInitialSync` 并 `allOf(...).join()`；并发处理器测试固定 StartingCDC 晚于 CompleteSnapshot/CompleteTableSnapshot。 | INITIAL_SYNC Heartbeat 必须 no-op；所有 initial 表 commit 完成后才处理 StartingCDC 及有效 CDC Heartbeat，首个有效 CDC Heartbeat 才可推进；崩溃窗口采用重放。 |
| `E-07` | `f91bfe4...`，`SyncProgress.java:37-42`；`HazelcastTargetPdkBaseNode.java:169,257,1467-1484,1550-1566,1577-1595,1744-1751,1875-1953,1995-1998,2238-2245`；`HazelcastSourcePdkBaseNode.java:708-741,784-786`；`HazelcastSourcePdkDataNode.java:343-369,698-710` | `SyncProgress` 初始持有空 batch-offset Map。静态 capability 一旦存在，目标节点对所有任务设置全局 `offsetCallbackEnable=true`；此时普通 initial DML 不再调用 `flushOffsetCallback`，目标 DML info 又没有 `batchOffset`。完成表事件仍把该表的 batch offset 写入内存 Map，但该分支不把 `flushOffset` 置为 `true`；定时 `saveToSnapshot` 和关闭时的同一调用均会在该标志为 `false` 时直接返回。纯 INITIAL_SYNC 不初始化 stream offset、不产生启动 stream Heartbeat，snapshot 完成后也不进入 CDC，因而没有后续事件替它设置该保存标志。 | 方案 B 会降低纯 INITIAL_SYNC 完成前重启时的恢复进度，但不会跳过源数据；该 at-least-once 重放与重复风险由 `DEC-02` 明确接受，Connector 不得伪造 initial offset 规避。 |
| `P-01` | `c4cab10...`，`TapConnectionContext.java:17,58-64`；`ProcessControlFunction.java:7-8`；`TapCallbackOffset.java:89-99` | callback 类型是 `Consumer<Object>`；`processControl` 可抛 Throwable；valid offset 只表示 batch/stream offset 至少一个存在，nodeIds 需额外验证。 | callback 无 ack；Heartbeat 失败可同步传播；sourceLane 必须单独 fail-fast。 |
| `M-01` | Paimon `release-1.3.1`，`TableCommitImpl.java:205-278` | `filterAndCommit` 过滤已提交 identifier，只提交 retry 集合并返回其数量，合法返回可以是零。 | 不能把返回零当失败；pending retry 保持幂等路径。 |
| `M-02` | Paimon `release-1.3.1`，`StoreMultiCommitter.java:150-194` | 多表 committable 先按表分组，再循环逐表 commit/filterAndCommit。 | 多表 snapshot 不是跨表原子事务，Connector 需要 offset barrier 而不是假设原子提交。 |
| `DEC-01` | 第一阶段采访确认 | at-least-once、多表覆盖、默认值、后台扫描、仅改 connector、DDL/stop 强制 drain，并且原则上不得改变 Paimon Connector 其他当前功能。 | 本文除后续明确例外外的规范性产品边界。 |
| `DEC-02` | 2026-07-27 方案 B 采访确认 | 用户明确选择方案 B，并接受纯 INITIAL_SYNC 在任务全部完成前因崩溃、强制停止或正常手动停止后的任意重启而从更早位置、最坏从头重放；append-only 目标允许重复，但不得丢数据，仍只保证 at-least-once。 | 这是对 `DEC-01`“其他行为不变”的唯一明确例外；全局 capability 可以恢复，相关风险必须进入合同、测试、验收和发布说明。 |

证据链的推导规则：源码只证明当前/历史可观察行为；本文的修复算法属于在 `DEC-01` 边界内消除已证明缺陷、并应用 `DEC-02` 唯一例外的设计决定。若实现与任一证据推导冲突，必须先更新 Spec 并重新由用户确认，不能用代码注释替代证据。

### 4.3 旧实现确实存在微批能力

父提交 `821ff7e...` 的 `PaimonService` 具备以下设施：

- `accumulatedRecordCount`：按表累计跨调用记录数。
- `lastCommitTime`：按表保存提交计时基准。
- `commitLocks`：串行化同表写入和提交。
- `asyncCommitExecutor`：`scheduleAtFixedRate` 定时扫描。
- `firstOffsetByTable`：尝试保存每表首个 offset。
- `writeRecordsWithStreamWriteInternal`：达到数量或时间阈值时提交。
- `flushAll`/`flushTable`：停止前提交累计数据。

因此，历史分支确实具备“跨 `writeRecords` 累计并由数量/时间阈值提交”的 Feature。

历史写入路径的准确执行顺序是：先把整批记录写入 writer，再把 `recordEvents.size()` 加入表级计数并初始化表级时间基准，然后按“`batchSize<=0`、累计数 `>=batchSize`、elapsed `>=commitInterval`”的优先顺序判断是否提交。三个判断逻辑上组成 OR；只要任一条件成立即进入提交分支。提交成功后计数归零、时间基准更新为完成时间。该有效顺序是新 Spec 恢复 CDC 微批能力的直接参照。

旧 scheduler 不是从每张表第一批数据到达时开始独立固定周期，而是在 Service `init()` 时建立一个全局固定相位。忽略线程抖动、锁等待和提交耗时，在表第一批数据到达后没有后续 `writeRecords` 的情况下，第一次满足条件的扫描名义延迟属于 `[commitIntervalMs, 2 * commitIntervalMs)`；默认配置下即约 `[30000ms, 60000ms)`。这个额外固定相位等待不是要保留的功能合同。

### 4.4 旧实现不得复用的错误

以下行为已经由历史源码确认，新的实现必须通过结构设计排除，而不是打补丁隐藏：

1. `LinkedHashMap<tableName, firstOffset>` 的插入顺序不是全局源事件顺序；引擎按表使用 `HashMap` 分组调用，表调用顺序不能代表源 offset 顺序。
2. `commitCallback(B)` 可以取得队头 A 的 offset 并在 A 尚未提交时调用 callback，存在 offset 提前风险。
3. 定时器向 `commitCallback` 传入 `database.table`，offset Map 却以 `table` 为键，可能导致条目无法删除。
4. 后台提交异常只记录日志，任务仍可继续写入和推进 offset。
5. `close()` 吞掉 flush 异常并继续报告正常关闭。
6. 定时器扫描所有累计表，没有把 initial sync 与 CDC eligibility 隔离。
7. DDL 执行前没有可靠 drain 目标表。
8. `afterInitialSync` 把 `tapTable.getId()` 直接作为 writer/committer 缓存键，而 `writeRecords` 使用 `database.tableName`；缓存 helper 按字符串精确取值，因此它会创建或复用另一个 writer/committer，不能提交 `writeRecords` 累积的数据。旧写入循环还尝试从 `TapRecordEvent.info` 读取引擎根本没有注入的 `batchOffset`，因此该 initial offset 路径不可达。
9. 旧版 writer/committer 与 commit identifier 没有当前 `PaimonTableWriteContext` 的 pending commit 和重启对账能力。
10. initial batch 在 stage 判断前已经增加 `accumulatedRecordCount` 并初始化 `lastCommitTime`；`afterInitialSync` 成功后又不清计数、不更新时间基准，因此后续 CDC 数量/时间判断会被 initial 数据污染，initial-only 表也会继续进入 scheduler 扫描候选。
11. `scheduleAtFixedRate` 使用 Service 级固定相位；表在某次扫描刚结束后收到第一批数据时，达到时间阈值后还可能再等待接近一个完整 interval。
12. write/prepare/commit 失败后旧外层重试会重新执行整个 source batch；若原 batch 已部分或全部进入 writer，会造成重复写入或重复提交。资源重建会先清空累计计数，重试时计数从零重新累计而不是在原值上翻倍；错误本质是重写 source batch，而不是计数单调重复增加。

证据链：`H-03`、`H-04`、`H-05`、`H-06`、`H-07`。

### 4.5 当前实现没有对等功能

当前 [`PaimonService.java`](../src/main/java/io/tapdata/connector/paimon/service/PaimonService.java) 中：

- `ASYNC_OFFSET_CONTRACT_VERIFIED=false` 使 `initAsyncCommit()` 直接返回。
- CDC 分支在每次 `writeRecords` 返回前设置 `shouldCommit=true`。
- 当前路径仍会在提交前增加表级计数并初始化 `lastCommitTime`，但每次 CDC 成功提交后立即把计数清零，所以这些字段不能形成跨调用微批。
- `setFlushOffsetCallback` 丢弃传入 callback。
- 旧表序 callback 机器仍完整留在源码：`firstOffsetByTable`、`committedOffsetTables`、`offsetCallbackLock`、`getFirstOffsetByTable()`、写入循环的 offset 收集分支和 `commitCallback` 均存在；多个提交入口仍调用这个立即返回的 `commitCallback`。
- 这套机器当前是不可达死代码，不是正在增长的缓存：唯一生产 setter 始终把 `flushOffsetCallback` 置为 `null`，因此写入循环中受 `flushOffsetCallback != null` 保护的 `firstOffsetByTable.put(...)` 不会执行，`commitCallback` 也会在入口返回。方案 B 激活时必须整体删除旧机器，不能只让 callback 重新变为非空。
- [`spec.json`](../src/main/resources/spec.json) 已移除 `flush_offset_callback` capability。
- 当前 Java 配置字段和 `spec.json` 的默认值仍为 `100000/30000/true`；Java 注释和三种 JSON placeholder 中的 `10000` 只是陈旧文案。前述旁路使三个有效默认值不能产生历史微批运行效果。

因此当前实现保留了部分字段和死代码，但不具备历史 Feature 的对等运行能力；小 batch 会产生高频 Paimon snapshot。

### 4.6 历史实现、当前代码与新 Spec 的等价性对比

下表中的“等价”严格采用第 2 节第 14 条定义。当前代码是本 Spec 要修改的基线；“新 Spec”描述尚未实现的目标合同，不能表述为当前仓库已经具备。

| 观察项 | 历史实现 `821ff7e...` | 当前代码 `ddb1e7d...` | 新 Spec 的确定合同 | 等价性结论 |
| --- | --- | --- | --- | --- |
| CDC 跨调用累计 | 每表计数跨调用保留，成功 commit 后归零。 | 每次 CDC 调用都 commit 并归零，实际不能跨调用累计。 | 每表只累计完整接收的非空 CDC batch，成功确认 commit 后归零。 | 新 Spec 对正常 CDC 功能等价；当前代码不等价。 |
| 数量阈值 | `batchSize<=0` 立即提交；正数配置下累计数 `>=batchSize` 提交。 | 分支仍存在，但被“每次 CDC 同步提交”旁路覆盖。 | `batchAccumulationSize<=0` 每次非空 CDC 提交；正数配置下累计数 `>=batchAccumulationSize` 提交。 | 有效、非 `null` 配置下功能等价。 |
| 调用内时间阈值 | 第一批被累计的数据建立时间基准；后续调用满足 elapsed `>=commitInterval` 时提交。 | 分支仍存在，但 CDC 在到达该分支前已被强制设置为提交。 | CDC-only 表从第一批完整接收的 CDC 数据建立基准；后续调用满足 elapsed `>=commitIntervalMs` 时提交。 | 对 CDC-only 正常路径功能等价。 |
| 无后续写入的时间提交 | Service 级单线程 `scheduleAtFixedRate` 扫描累计表。 | 固定 false 开关阻止 scheduler 创建。 | Service 私有单线程 scheduler 在无新写入时自动唤醒，重新检查 eligible 表并提交到期数据。 | 后台时间提交能力等价；当前代码不等价。 |
| scheduler 精确时序 | 全局固定相位使低流量表名义提交延迟为 `[interval, 2*interval)`。 | scheduler 不运行。 | 按每表最近 deadline 调度，到期后重新检查；不人为增加第二个 interval。 | 机制和精确时序刻意不等价，修复旧延迟。 |
| initial sync | initial 也增加 CDC 共用计数/时间基准；`afterInitialSync` 成功后未重置二者。 | initial 仍先增加共用计数，但当前 `afterInitialSync` 使用 context 提交后会清计数并更新时间；scheduler 被关闭，不会后台扫描 initial。 | initial 从入口起就不参与 CDC 数量阈值和 scheduler eligibility；只由 `afterInitialSync` 强制 commit，成功后继续清计数并重置时间基准。 | 新 Spec 不恢复旧污染，并保留当前成功清理语义。 |
| 成功 commit 后状态 | 计数归零并把时间基准设为完成时间；`afterInitialSync` 例外。 | 每次 CDC 同步 commit 后执行相同更新。 | 所有成功确认的 commit 都发布 generation、归零被覆盖计数并更新基准；包括 `afterInitialSync`。 | CDC 正常路径等价；initial 路径修正。 |
| 配置为缺失或 `null` | Java 字段有默认值；若运行时显式变成 `null`，size 为 null 会立即提交，interval/async 为 null 会关闭相应时间能力。 | 保留旧 null 分支。 | 缺失或 `null` 统一回落到 `100000/30000/true`；只有显式 `<=0`/`false` 才关闭对应能力。 | 严格边界不等价；按已确认默认合同规范化。 |
| commit 结果不明 | 外层循环可能重放整个 source batch；资源重建先清零计数，随后重写时从零重新累计，因此计数不会在原值上翻倍，但数据仍可能被重复写入/提交。 | 已有 `PaimonTableWriteContext` pending 确认，但只完整接入部分入口。 | 所有入口复用同一 pending messages/identifier 做有界确认；已进入 writer 的 source batch 绝不重写。 | 刻意不等价，修复重复写入。 |
| offset、失败和关闭 | 多表 callback 顺序、table key、异常传播和关闭均存在已确认错误。 | 当前同步提交避免异步 offset 前移，但代价是一调用一 snapshot；已有 sticky failure、pending state、DDL drain 和 close 聚合基础。 | 在当前安全基础上增加多表/多源 generation 屏障、异步失败传播和有序停止。 | 不复用旧错误；只保证 connector-managed at-least-once。 |
| 纯 INITIAL_SYNC batch offset | 旧 `spec.json` 全局开启 callback；完成表 offset 只写入引擎内存 Map，未置保存标志，connector 又取不到 initial `batchOffset`，且纯 initial 没有后续 CDC 事件触发保存。 | capability 已移除；普通 initial DML 会走引擎平台路径设置保存标志，当前由引擎持久化 batch offset。 | 方案 B 全局恢复 capability，不伪造 initial offset；任务全部完成前任何重启均允许从更早位置、最坏从头重放，append-only 目标允许重复但不得丢数据。 | 与当前恢复进度刻意不等价；这是 `DEC-02` 已批准的唯一行为例外，不是待解决问题。 |

最终结论固定为：方案 B 恢复历史 Feature 的有效 CDC 微批能力，但不追求 bug-for-bug 等价；当前代码尚未实现该对等能力。静态 callback capability 对纯 INITIAL_SYNC 恢复进度的影响已由 `DEC-02` 明确接受并转化为测试、验收和发布合同，因此不再构成实施阻塞。实现验收不得只检查字段或 scheduler 是否存在，必须用第 16 节测试证明跨调用 snapshot 数量、两个阈值、无后续写入提交、多表 at-least-once、纯 initial 不丢数据以及所有显式非等价修正。

证据链：`H-01`～`H-07`、`C-01`～`C-07`、`C-10`、`E-01`～`E-07`、`DEC-01`、`DEC-02`。

### 4.7 可依赖的引擎顺序合同

在已核实的 Tapdata 引擎基线中：

1. 非并发目标队列按事件顺序处理。
2. 并发目标使用 `PartitionConcurrentProcessor`：offset/control 被转换为 watermark；watermark runner 等待所有分区消费者对同一 `CountDownLatch` 完成后，才调用 `flushOffset.accept(event)`。
3. 分区消费者在 `countDown` 之前调用目标 `writeRecords` 处理此前 DML。
4. 引擎按目标表用 `HashMap` 分组 DML，表组调用顺序不稳定。
5. 启用 `flush_offset_callback` 后，普通 DML 不再由引擎自动推进 offset；Heartbeat 通过 connector `processControl` 传递。
6. 回调按 `nodeIds[0] + targetNodeId` 选择引擎进度槽，然后执行 `flushOffsetCallback` 和 `saveToSnapshot()`。
7. callback 是 `Consumer<Object>`，不提供持久化 acknowledgement；其返回不能被解释为 exactly-once 证明。
8. 引擎向目标 `TapRecordEvent.info` 注入 CDC `streamOffset`、`syncStage`、`sourceTime` 和 `nodeIds`，但不注入 initial `batchOffset`；Connector 无法从 `writeRecords` 获得 initial batch offset。
9. INITIAL_SYNC_CDC 源端先 enqueue `TapdataCompleteSnapshotEvent`，之后才调用 `doCdc()`；`doCdc()` 先 enqueue `TapdataStartingCdcEvent`，之后才进入 CDC reader。目标事件顺序保证 StartingCDC 晚于 CompleteSnapshot；polling CDC Heartbeat 在该 CDC loop 内生成，Heartbeat factory 显式设置 stage 为 `CDC`。
10. 源端初始化 stream offset 时可能更早生成一个 Heartbeat，但 INITIAL_SYNC_CDC 会把它的 stage 明确改为 `INITIAL_SYNC`；它不是 CDC offset 屏障，Connector 必须保持 no-op。
11. `TapdataCompleteSnapshotEvent` 在目标端同步等待所有表的 `afterInitialSync` 完成。因此 initial 的安全边界是“全部 initial 数据先由 `afterInitialSync` commit，之后首个有效 CDC Heartbeat 再推进 stream offset”；若在首个 CDC Heartbeat 前崩溃，源端重放 initial 而不会跳过未提交数据。
12. `flush_offset_callback` 是 connector 级静态 capability，引擎据此对所有同步类型设置 callback 模式，而不是只对 CDC 设置。callback 模式会跳过普通 DML 的平台托管 offset；纯 INITIAL_SYNC 又没有 stream Heartbeat，Connector 也拿不到 initial batch offset。方案 B 明确接受由此产生的完成前重启扩大重放，不允许 Connector 伪造 offset 或宣称保持当前断点恢复效率。
13. 引擎 callback Consumer 会读取 `TapCallbackOffset.eventTime`，并把它作为临时 `TapRecordEvent.referenceTime` 写入 `SyncProgress.eventTime`；丢失该字段会改变当前延迟/进度观测数据。

本 Spec 只依赖“Heartbeat 到达 Connector 时，引擎已完成该屏障之前的 DML `writeRecords` 调用”这一事实，不依赖表分组迭代顺序。

## 5. 对外配置和能力合同

### 5.1 配置

| 配置 | 缺省值 | 确定语义 |
| --- | ---: | --- |
| `batchAccumulationSize` | `100000` | `>0` 时，某表 CDC 累计记录数达到或超过该值即在当前调用内提交；`<=0` 时该表每次非空 CDC `writeRecords` 都立即提交。 |
| `commitIntervalMs` | `30000` | `>0` 时启用时间阈值；`<=0` 时完全禁用调用内时间触发和后台定时扫描。 |
| `enableAsyncCommit` | `true` | `true` 且 `commitIntervalMs>0` 时启用后台扫描能力；Service 初始化只建立无工作线程的 scheduler adapter，首次出现 `cdcEligible=true` 且确有未提交 CDC 状态的表后才惰性创建唯一 daemon worker。`false` 时始终不创建后台线程，但每次 CDC `writeRecords` 仍检查时间阈值。 |

缺失或反序列化为 `null` 的配置必须回落到上述缺省值。`spec.json`、`PaimonConfig` 和测试中的默认值必须一致。`PaimonConfig.java` 中仍写成 `10000 records` 的字段注释，以及 `spec.json` 三种语言中仍写成 `10000` 的 placeholder，都必须改为 `100000`。

证据链：`H-02`、`H-05`、`C-01`、`DEC-01`。

### 5.2 Capability 与 callback 注入

CDC 微批在 `writeRecords` 返回后继续缓存数据时，必须由 Connector 控制 stream offset 推进，因此方案 B 必须在 `spec.json` 全局恢复 `flush_offset_callback`。该 capability 会同时改变纯 INITIAL_SYNC 的 batch offset 行为，且 Connector 当前拿不到补偿所需的 initial `batchOffset`；此影响已经按第 2 节第 11 条接受。接线合同固定为：

- `spec.json` 必须声明且只声明一个 `flush_offset_callback` capability；不得增加无法改变静态 capability 作用域的伪动态开关。
- `PaimonConnector.onStart` 必须把可用的 `TapConnectionContext.getFlushOffsetCallback()` 原样注入当前 `PaimonService`。
- callback 注入不得成为 `onStart` 的全局必选条件；connection test、元数据操作和源端读取允许 callback 为 `null`。
- 第一次 offset-bearing 的目标 DML 必须在 preflight 阶段校验 callback 非空；失败发生在创建/使用 writer 和写入任何行之前。第一次有效 CDC Heartbeat 也必须在登记屏障前执行同一校验。
- callback 一旦用于目标写入，在 `RUNNING`、`STOPPING` 或 `FAILED` 状态不得被替换为另一个实例或清空。唯一例外是第 12 节完成所有 callback drain/抑制及资源关闭后的终止清理；此时允许释放 callback 强引用，并且 Service 随即或已经进入 `CLOSED`，不得再接受任何目标写入或 callback。
- initial DML、`afterInitialSync` 和 INITIAL_SYNC Heartbeat 均不得调用该 callback；纯 INITIAL_SYNC 的扩大重放只能作为已接受风险存在，不能通过合成 batch/stream offset 掩盖。

证据链：`C-02`、`C-06`、`E-03`、`E-04`、`E-05`、`E-07`、`P-01`、`DEC-01`、`DEC-02`。

## 6. 术语和状态模型

### 6.1 规范键

- `tableKey`：`database + "." + targetTableName`，所有 writer、计数、锁、DDL、调度和提交状态统一使用该键。
- `sourceLane`：事件 `nodeIds` 非空列表中的第一个元素 `nodeIds[0]`。它与引擎选择 `SyncProgress` 的源节点维度一致。
- `stage`：只允许明确识别为 `INITIAL_SYNC` 或 `CDC`。可以接受引擎传入的 enum 或字符串，但进入协调器前必须规范化为这两个精确值；缺失、类型错误或其他值均为 malformed。非空 `writeRecords` batch 中的事件必须属于同一 stage；混合或冲突 stage 必须在写入前失败。
- `valid CDC Heartbeat`：同时满足以下全部条件：规范化 stage 精确为 `CDC`；`streamOffset != null`；`sourceTime` 的运行时类型为 `Long` 且非空；`nodeIds` 是非空列表且 `nodeIds[0]` 是非空字符串；callback 已注入。仅满足 `TapCallbackOffset.hasValidOffset()` 不足以证明 CDC Heartbeat 有效，因为该方法也接受只有 batch offset 的对象。
- `callback eventTime`：精确取 `HeartbeatEvent.getReferenceTime()`，允许为 `null`；它不参与 Heartbeat 有效性判断，也不得在 Connector 内用当前时间或其他 DML 时间替换。引擎已在缺少原生 `HeartbeatEvent` 时用 `sourceTime` 构造 referenceTime，Connector 只负责原样保存和回调。

每条 CDC DML 都必须具有非空 `sourceLane`，即使该条事件本身没有携带 stream offset 也不例外；否则 Connector 无法把该数据纳入后续 Heartbeat 的表依赖，必须在写入前失败。initial DML 不依赖 Connector offset barrier，因此不强制要求 sourceLane。

### 6.2 Service 生命周期

Service 必须具有单向状态机：

```text
NEW -> RUNNING
NEW -> FAILED -> CLOSED
NEW -> CLOSED
RUNNING -> STOPPING -> CLOSED
RUNNING -> FAILED -> CLOSED
STOPPING -> FAILED -> CLOSED
```

- `NEW`：构造完成但尚未完成配置校验、Catalog 和 scheduler adapter 初始化；拒绝所有写入性入口，且不得创建 scheduler worker。
- `RUNNING`：允许新的写入、Heartbeat、DDL、`afterInitialSync` 和后台扫描进入。
- `STOPPING`：拒绝新入口；等待已经获准的操作退出；不得开始新的后台扫描。
- `FAILED`：保存首个根因，拒绝新入口，停止调度新的提交和所有 offset callback。
- `CLOSED`：资源释放完成；任何写入性入口都失败。
- 状态只能沿图中方向前进，不得从 `FAILED` 恢复为 `RUNNING`。从未开始初始化的 `NEW` 实例允许直接清理到 `CLOSED`。

`PaimonService` 构造函数必须只建立 coordinator、lifecycle 和无工作线程的 scheduler adapter，并保持 `NEW`。`PaimonConnector.onStart` 按“创建 Service → 原样注入可用 callback → 调用 `init()`”执行；`init()` 只有在配置校验、Catalog 创建和 scheduler adapter 初始化全部成功后，才把生命周期原子发布为 `RUNNING`。初始化任一步失败时，必须先保存原始失败并进入 `FAILED`，再 best-effort 关闭已经创建的资源、释放 callback 强引用并进入 `CLOSED`，最后以初始化异常为主异常、清理异常为 suppressed 向 `onStart` 传播。初始化失败路径不得启动 scheduler worker、执行 Paimon commit 或 callback。该规则只增加初始化边界的生命周期发布和失败清理，不得改变 Catalog options、storage 配置或 CatalogFactory 调用语义。

所有外部入口和 scheduler task 必须通过同一个 ingress gate 登记/注销 in-flight 操作。切换到 `STOPPING` 必须原子地关闭新入口；等待 in-flight 时不得持有表锁、Paimon 资源锁或 offset coordinator 锁。

### 6.3 表级提交状态

每个 `tableKey` 必须维护以下逻辑状态；字段可以封装在新协调器内，不要求继续使用现有散落 Map：

- `bufferedRecordCount`：上次成功提交后已完整写入当前 writer、尚未被成功 commit 覆盖的全部阶段记录数；initial 和 CDC 都增加该值。它只用于强制 drain、pending 对账和可观测性，不参与 CDC 数量/时间阈值。
- `accumulatedRecordCount`：上次成功提交后已完整接收的 CDC 记录数；initial 永不增加该值。它只用于 `batchAccumulationSize` 判断和 CDC scheduler eligibility。
- `commitIntervalBaseTimeMs`：有成功提交时等于最近一次 commit 完成时间；该表尚无任何成功提交时，在第一批 CDC 数据完整接收后初始化为该时刻。
- `acceptedGeneration`：该表完整接收 CDC batch 后递增的单调代数。
- `committedGeneration`：已被成功 Paimon commit 覆盖的最大代数。
- `pendingCommitTargetGeneration`：当前 `PaimonTableWriteContext` 中 pending commit 所覆盖的代数；不存在 pending commit 时为空。
- `lastAcceptedGenerationBySource`：每个 `sourceLane` 最近一次被该表完整接收的代数。
- `cdcEligible`：该表是否已完整接收至少一个 CDC batch。

时间基准分两种确定场景，不得混用：

1. CDC-only 表尚无成功 commit 时，不得在 Service 启动时预先计时；第一批非空 CDC 数据完整接收后才把 `commitIntervalBaseTimeMs` 初始化为当时时钟。因此 Service 启动后即使先空闲超过 interval，第一批小 batch 也不会仅因启动空闲而立即提交。
2. INITIAL_SYNC_CDC 表在 `afterInitialSync` 成功后，必须把 `commitIntervalBaseTimeMs` 更新为该 initial snapshot 的完成时间。initial batch 的记录数和等待时间从未进入 CDC 计数或 scheduler eligibility；这里只把最近一次已成功 snapshot 的完成时间作为后续 CDC 基准。若此后空闲达到 interval，首个小 CDC batch 必须在当前调用内立即提交。

此后每次成功确认的 CDC/DDL/stop commit 也把基准更新为完成时间。scheduler 必须按第 8.2 节的表级最近 deadline 调度，而不是复用旧 Service 级固定相位。上述第二种场景是已确认合同并保留当前 `afterInitialSync` 更新时间的语义，不得改成“initial 后清空基准”。

所有同表写入、pending commit 重试、generation 发布、Paimon commit、DDL drain 和 context close 必须由现有表级生命周期锁串行化。不同表不得因为 Paimon I/O 被一个全局锁串行化。

证据链：`C-03`、`C-04`、`M-02`。

### 6.4 源 offset 屏障状态

每个 `sourceLane` 必须维护一个 `LaneOffsetState`，其中至多各有一个 `pending` 和一个 `inFlight`：

- `pending`：尚未开始 callback 的最新 Heartbeat；保存完整 `TapCallbackOffset` 副本、单调 `version` 及 `requiredGenerationByTable`。
- `inFlight`：已经取得 lane 内 callback 执行权的 Heartbeat；保存与 pending 相同的不可变内容、唯一 callback token，以及外部 Consumer 是否已经开始的 single-assignment atomic/volatile `consumerStarted` 状态。

同一 `sourceLane` 的替换和执行规则是确定的：

1. coordinator 锁内把 ready 的 `pending` 移到 `inFlight` 是 lane 内 callback reservation 线性化点（即第 9.3 节所称 offset callback-start）；移动后 `pending=null`，初始 `consumerStarted=false`。
2. 新 Heartbeat 在 reservation 线性化点之前到达，替换旧 `pending`；在线性化点之后到达，不得取消 `inFlight`，而是创建或替换新的 `pending`。
3. callback 完成只能按 token 清除完全匹配的 `inFlight`，绝不能删除之后到达的 `pending`。
4. callback 成功清除 `inFlight` 后，立即重新评估同 lane 的 `pending`；若 ready，则按版本顺序开始下一次 callback。
5. callback 失败时保留失败的 `inFlight` 和当时最新的 `pending` 供诊断/重放，形成粘滞故障并禁止任何后续 callback。
6. reservation 与真正调用外部 Consumer 是两个不同边界；生命周期使用第 9.3 节的 Consumer-start 线性化点判断 callback 属于 `RUNNING` 还是 stop 全局 drain。

不得比较 opaque `streamOffset`/`batchOffset` 的内容。同一 lane 的 H1 已开始 callback 后才到达 H2 时，H1 和 H2 必须依次 callback；H1 尚未开始时到达 H2，只保留 H2。不同 `sourceLane` 的状态相互独立。

## 7. 写入与提交协议

### 7.1 CDC batch 接收顺序

单次非空 CDC `writeRecords` 必须按以下顺序执行：

1. 通过 Service ingress gate；若状态不是 `RUNNING`，立即抛出保存的首个失败或生命周期异常。
2. 对整个 batch 完成 stage、offset metadata 和 sourceLane 校验。
3. batch 携带有效 offset 时，确认 callback 已注入；否则在任何 Paimon 写入前失败。
4. 获取目标表生命周期锁。
5. 若 `PaimonTableWriteContext.hasPendingCommit()` 为真，先按第 7.2 节的有界确认协议重试同一 pending；成功后只发布 `pendingCommitTargetGeneration` 对应的 committed generation 并清空该 pending target，只有重试耗尽或确定性终止错误才进入 `FAILED`。
6. 在表锁内完成既有表写语义及 DML preflight，取得或复用唯一规范 `PaimonTableWriteContext`。
7. 把 batch 中每条记录写入该 context。任何一条失败时不得发布新的 accepted generation；Service 进入 `FAILED`，offset 不前移。
8. 全部记录写入成功后，`acceptedGeneration` 恰好递增一次；为 batch 中每个不同 `sourceLane` 更新 `lastAcceptedGenerationBySource`；`bufferedRecordCount` 和 CDC 专用 `accumulatedRecordCount` 均增加 `recordEvents.size()`；标记 `cdcEligible=true`。
9. 在同一表锁内判断数量阈值和时间阈值。达到任一阈值时提交；否则返回并保留 writer buffer。
10. 提交成功后尝试释放已经满足条件的 Heartbeat；callback 不得在表锁或 coordinator 状态锁内执行。

空 batch 不创建 context、不改变计数/generation、不提交、不回调 offset。

### 7.2 Paimon commit 成功边界

在表锁内开始新 commit 时，必须先记录 `commitTargetGeneration=acceptedGeneration`；一旦 context 形成 pending commit，必须同时把该值保存为 `pendingCommitTargetGeneration`。重试 pending commit 时必须复用该 target，不得把重试成功误算为覆盖之后才接收的数据。只有 `PaimonTableWriteContext.commit()` 或 `retryPendingCommit()` 成功返回后，才允许：

1. 设置 `committedGeneration=max(committedGeneration, commitTargetGeneration)`；
2. 将该 commit 覆盖的 `bufferedRecordCount` 和 CDC `accumulatedRecordCount` 归零；
3. 将 `commitIntervalBaseTimeMs` 更新为 commit 成功后的时钟值；
4. 评估并触发 offset callback。

当前 ingress 内的 pending 确认属于同一次 commit 操作，不是新的 source batch 重放。必须统一使用以下协议：

1. `commit()` 或 `retryPendingCommit()` 抛错后，先在同一表锁内检查 `hasPendingCommit()`。
2. 若存在 pending，复用同一 prepared messages、commit identifier 和 `pendingCommitTargetGeneration`，在原失败调用之后最多再调用 `retryPendingCommit()` **3 次**；该上限保持当前实现的 `maxRetries=3`。生产重试间隔保持当前 1000ms，但必须封装为可测试、可感知中断的等待策略，不得在测试中真实 sleep。
3. 任一次确认成功即发布该 pending target 对应的 committed generation、清 pending target、归零其覆盖的 `bufferedRecordCount` 与 CDC `accumulatedRecordCount` 并更新时间。若 pending 来自本次已经写入的 source batch，直接返回本次结果，绝不能重新进入写循环并把该 batch 再写一次。
4. 第一次 commit/确认失败只表示结果不明，尚不形成粘滞故障。只有三次追加确认全部失败，或失败后 `hasPendingCommit()==false` 因而没有幂等确认路径时，才把最初异常作为主因形成粘滞故障；每次 retry 异常按发生顺序作为 suppressed 保留。
5. 行 preflight、转换、路由或 writer 的确定性失败不属于 pending 确认；一旦 writer ingress 已开始即按第 14 节直接形成粘滞故障，不重放整个 source batch。
6. 该 helper 和完全相同的 3 次上限必须用于 write 数量/时间触发、scheduler、DDL drain、`afterInitialSync` 和 stop drain；不得只在 `writeRecords` catch 中保留重试。
7. 普通运行入口在 retry delay 被中断时，立即恢复 interrupt 标记，以 `InterruptedException` 终止本次确认并形成粘滞故障；close 内发生中断时改用第 12 节的不可中断清理规则。

prepare、`filterAndCommit`、commit state save 任一阶段失败时，在上述确认协议成功前不得执行四项成功发布。pending commit 重试成功且已经覆盖当前全部 buffer 时，不得紧接着再调用一次 `commit()` 制造空 snapshot。即使 `bufferedRecordCount` 和 CDC `accumulatedRecordCount` 均为 `0`，只要 context 的 `hasPendingCommit()` 为真，强制 flush、DDL drain 和停止 drain 都必须重试 pending commit。

`retryPendingCommit()` 在 pending 为空时返回 `nextCommitIdentifier-1` 是当前 context 的幂等 no-op：不得调用 `filterAndCommit`、不得推进 identifier、不得保存状态。Service 的统一 helper 只有在同一表锁内已经观察到 `hasPendingCommit()==true` 且持有对应 `pendingCommitTargetGeneration` 时，才允许用一次成功返回发布 generation；空 pending 的 no-op 返回值绝不是本轮 commit 成功证据，不能据此清计数、更新时间或触发 callback。

不得绕过或重写以下当前语义：

- stable `commitUser`；
- 单调 `nextCommitIdentifier`；
- `filterAndCommit` 返回 `0..pendingSize` 均为合法结果；
- snapshot 已确认但 task state save 失败时保持 fenced，重启后以同 user 最新 snapshot 对账；
- writer/router 写入失败后不在原 context 内重放整个 source batch。

证据链：`C-04`、`C-05`、`C-07`、`M-01`。

## 8. 数量与时间触发

### 8.1 调用内触发

- `batchAccumulationSize<=0`：每次非空 CDC 调用在返回前提交该表。
- `batchAccumulationSize>0`：累计数 `>=` 阈值时在当前调用内提交。
- `commitIntervalMs>0`：无论 `enableAsyncCommit` 取值，每次 CDC 调用在写入成功后都检查 `now-commitIntervalBaseTimeMs >= commitIntervalMs`；满足时在当前调用内提交。尚无基准时先按第 6.3 节初始化，当前调用的 elapsed 为 `0`。
- `commitIntervalMs<=0`：不得执行时间判断。

### 8.2 后台时间扫描

Service 初始化时只创建不持有工作线程的 scheduler adapter。仅当 `enableAsyncCommit=true && commitIntervalMs>0`，并且至少一张表在本次状态发布后同时满足 `cdcEligible=true` 与“CDC `accumulatedRecordCount>0` 或存在由 CDC commit 形成的 pending commit”，才惰性创建一个 Service 私有的单线程 daemon worker；创建与启动必须幂等，并发出现多张首批未提交 CDC 表时仍只能产生一个 worker。若首批 CDC 已在当前调用内因数量/时间阈值提交且没有 pending，则不得仅因曾发布 `cdcEligible=true` 创建 worker。配置不满足、Service 仍为 `NEW`、只发生 initial、连接测试、元数据操作或源端读取时均不得创建 worker。scheduler 必须满足：

本文“保留后台定时扫描”的确定含义是：即使没有下一次 `writeRecords`，后台线程也会在最近表级 deadline 到达时自动唤醒，扫描并重新校验所有 CDC eligible 且有未提交状态的表，然后提交所有已经到期的表。该合同要求保留自主后台时间触发，不要求复用旧 `scheduleAtFixedRate` 的全局固定相位。

1. 只处理 `cdcEligible=true` 且 CDC `accumulatedRecordCount>0` 或存在由 CDC commit 形成的 pending commit 的表；`bufferedRecordCount` 中只有 initial 数据不能使表获得 scheduler eligibility。
2. 永不提交 initial-only 表。
3. 同一时刻最多一个扫描任务；同表仍通过表锁与写入、DDL、停止串行化。
4. 依据每张表的 `commitIntervalBaseTimeMs + commitIntervalMs` 安排最近到期提交。实现不得用固定相位扫描人为增加另一个完整 `commitIntervalMs` 的等待；允许 scheduler 抖动、正在执行的提交和表锁等待造成的实际延迟。
5. 到期时重新在表锁内检查状态和时间，避免与调用内提交重复创建 snapshot。
6. 没有待提交表时保持空闲；新表变为 eligible 或一次 commit 改变 deadline 后，必须重新计算最近 deadline。
7. scheduler commit 结果不明且存在 pending 时，先在同一个已登记的 scheduler task 内完成第 7.2 节最多 3 次确认；只有确认耗尽、确定性终止错误或 scheduler 自身其他 Throwable 才设置首个粘滞故障、停止后续调度并记录完整堆栈。不得把第一次可幂等确认的异常立即粘滞，也不得仅日志后继续。
8. scheduler 线程不能越过 PDK 主动向引擎线程注入异常。故障必须在下一次 `writeRecords`、Heartbeat `processControl`、DDL、`afterInitialSync` 或 `onStop` 中抛出。

证据链：`H-01`、`H-02`、`H-03`、`H-05`、`C-02`、`C-07`、`DEC-01`。

## 9. 多表 Heartbeat offset 屏障

### 9.1 建立屏障

`PaimonConnector.processControl` 收到 Heartbeat 时必须委托 Service：

1. 先规范化 `syncStage`。stage 明确为 `INITIAL_SYNC` 的 Heartbeat 保持 no-op，不登记屏障、不提交、不 callback；这是引擎初始化 stream offset 时可能生成的早期 Heartbeat。非 Heartbeat control event 同样保持 no-op。
2. Heartbeat 的 stage 缺失、类型错误或不是已识别的 `INITIAL_SYNC/CDC` 时，作为 malformed payload 在登记屏障前形成粘滞故障并同步抛出，不能依赖引擎 `flushOffsetCallback` 的静默返回。
3. 对 stage 为 `CDC` 的 Heartbeat，逐项校验第 6.1 节 `valid CDC Heartbeat`：必须有 stream offset、`Long sourceTime`、非空 `nodeIds[0]` 和已注入 callback。任一不满足均在登记屏障前形成粘滞故障并同步抛出。
4. 构造 callback payload 时必须把规范化 stage 写成精确字符串 `"CDC"`，复制 streamOffset、sourceTime、完整 nodeIds，并把当前 `HeartbeatEvent.getReferenceTime()` 原样写入 nullable `eventTime`；不得用 batch offset 代替 stream offset，不得丢弃或重算 eventTime，也不得保留调用方可修改的 Map/List 引用。
5. 在 coordinator 锁内遍历所有表，但只复制 `lastAcceptedGenerationBySource` 中确实存在该 `sourceLane` 条目的表，形成 `requiredGenerationByTable`；从未接收该 lane 数据的表不得人为填入 generation `0`。随后以新 version 创建或替换 `LaneOffsetState.pending`。
6. 若所有依赖均已满足且该 lane 没有 `inFlight`，在 coordinator 锁内把 pending 移到 inFlight 作为 callback-start 线性化点；释放锁后执行 callback。否则保留 pending。

DDL 自身携带的 offset 不直接 callback，必须等待后续有效 CDC Heartbeat。

证据链：`E-01`、`E-02`、`E-03`、`E-04`、`E-06`、`P-01`。

### 9.2 允许回调的唯一条件

对 pending Heartbeat `H`，只有以下谓词为真才允许回调：

```text
for every (tableKey, requiredGeneration) in H.requiredGenerationByTable:
    table[tableKey].committedGeneration >= requiredGeneration
```

这一定义不读取 Map 插入顺序，也不依赖 A 表和 B 表谁先被引擎调用。`RUNNING` 状态下某表 Paimon commit 成功后必须重新评估所有受该表影响的 sourceLane。`STOPPING` 状态的逐表 drain 只发布 committed generation，严禁逐表触发 callback；仅在全部表 drain 成功后执行第 12 节的一次全局 callback drain。

`requiredGenerationByTable` 为空时，上述全称谓词为真：该 Heartbeat 之前没有 Connector 接收的目标 DML，不需要也不得为了 callback 人为创建空 Paimon snapshot。

### 9.3 callback 并发、替换和失败

- Heartbeat 存在 generation 依赖时，每个依赖都必须先由成功 Paimon commit 覆盖；无表依赖 Heartbeat 可以直接 callback，不要求也不创建空 snapshot。
- callback 必须在表锁和 coordinator 状态锁之外执行。
- offset callback-start/reservation 必须在 coordinator 锁内把 ready pending 原子移动为 inFlight，并使用不可变 version/token；不得在只打“执行中”标记后继续允许新 Heartbeat 覆盖同一对象。
- callback 完成只可按 token 清除匹配的 inFlight。H1 执行期间到达的 H2 保存在 pending；H1 成功后再评估并执行 H2，H1 的完成绝不能清除 H2。
- 同一 `sourceLane` 不得并发执行两个 callback。不同 sourceLane 可以独立判定，但实现必须确保 callback Consumer 本身不会被并发调用；采用 Service 级 callback 执行锁串行化所有 Consumer 调用。
- 已移动为 inFlight 的 callback 在取得 Service 级 callback 执行锁后、调用外部 Consumer 前，必须通过与 close 共用的生命周期 gate 原子执行“确认仍为 `RUNNING` 并把 `consumerStarted=true`”；这是外部 Consumer-start 线性化点。若 close 先把状态切为 `STOPPING`，本次不得调用 Consumer，保留 `consumerStarted=false` 的 inFlight，等待第 12 节处理；若 Consumer-start 先成功，则 callback 属于 STOPPING 前已开始的 ingress，close 必须等待其完成，不能把已经发生的外部副作用倒退。第 12 节显式全局 callback drain 是唯一允许在 `STOPPING` 下设置 `consumerStarted=true` 的路径。
- 若更早的 callback 已失败，尚未 Consumer-start 的其他 lane 不得调用 Consumer，其 inFlight 保留；由此保证“已经预约但尚未执行”的 callback 不会越过首个故障。
- callback 抛出异常时，失败的 inFlight 和当前最新 pending Heartbeat 都必须保留，首个 callback 异常成为粘滞故障，Service 进入 `FAILED`，不得继续回调其他 lane 或该 lane 的 offset。
- callback 成功不等于引擎 snapshot 已获得跨系统原子确认。因为数据先 commit、offset 后 callback，callback 后任何引擎持久化失败只会引起重放，不会造成未提交数据被跳过，故满足 at-least-once。

证据链：`C-05`、`E-04`、`P-01`、`M-02`。

### 9.4 多表例子

```text
source S: A1 -> B1 -> Heartbeat H1

Connector 完整接收 A1: A.accepted=1, A.lastAccepted[S]=1
Connector 完整接收 B1: B.accepted=1, B.lastAccepted[S]=1
H1.required={A:1, B:1}

A commit 成功、B 未成功: H1 禁止 callback
B commit 成功: H1 允许 callback
```

若引擎先调用 B 的表组再调用 A，结果不变。

## 10. Initial sync 协议

1. initial batch 必须使用当前 `PaimonTableWriteContext` 写入。整批写入成功后只增加 `bufferedRecordCount`，不得增加 CDC `accumulatedRecordCount`、`acceptedGeneration` 或 `lastAcceptedGenerationBySource`；其记录数和在 writer 中的停留时间不得参与 CDC 数量阈值、调用内时间判断或 scheduler eligibility。`afterInitialSync` 成功完成后建立第 6.3 节定义的最近成功 snapshot 时间基准；这只影响后续首个 CDC batch 的提交时机。
2. Connector 不得尝试从 initial `TapRecordEvent.info` 读取、排序、保存或合成 `batchOffset`，也不得在 `afterInitialSync` 调用 offset callback；当前引擎没有向该接口暴露 initial batch offset。
3. `afterInitialSync(table)` 必须取得该表锁：有 pending commit 时只重试并确认它覆盖的 target generation；没有 pending commit 时调用一次 `commit()`，保留当前“每表 initial 完成边界强制 commit”的语义，即使该表本轮没有记录也不例外。不得在 pending 重试成功后追加第二次空 commit。
4. commit 成功后，归零该表 `bufferedRecordCount` 和 CDC `accumulatedRecordCount` 并更新成功提交时间；commit 结果不明且有 pending 时先完成第 7.2 节有界确认，只有确认耗尽或无幂等确认路径的终止失败才形成粘滞故障并由 `afterInitialSync` 向引擎抛出。
5. `afterInitialSync` 可由引擎对多表并发调用；不同表的 Paimon I/O 保持独立。
6. 源端会在 INITIAL_SYNC_CDC 初始化 stream offset 时产生一个 stage 为 `INITIAL_SYNC` 的早期 Heartbeat；第 9.1 节要求它 no-op。源端先 enqueue CompleteSnapshot，之后才调用 `doCdc()`；`doCdc()` 先 enqueue StartingCDC 再启动 CDC reader。目标事件顺序保证 StartingCDC 晚于 CompleteSnapshot，目标端处理 CompleteSnapshot 时等待所有表的 `afterInitialSync` 完成。因此首个有效 CDC Heartbeat 必须使用第 9 节的多表 generation 屏障，只能在其依赖的 CDC 数据以及此前已经完成的 initial commit 之后 callback。
7. INITIAL_SYNC_CDC 在首个有效 CDC Heartbeat callback 前发生任何重启时，Connector 不推进 initial offset；initial 重放属于允许的 at-least-once 行为。首个有效 CDC Heartbeat callback 后，引擎会通过既有 callback 路径设置保存标志并保存 stream offset；本 Spec 不把 callback 调用解释为跨系统持久化 ACK。
8. 对纯 INITIAL_SYNC，静态 capability 使普通 initial DML 不再触发平台保存标志；完成表事件只更新引擎内存 batch-offset Map。任务全部完成前发生进程崩溃、强制停止或正常手动停止后再启动时，恢复点允许早于已完成表，最坏允许从初始位置重放。
9. 上述重放不得跳过任何未完成源数据；已经成功进入 Paimon snapshot 的数据允许再次写入。append-only 目标允许出现重复行，主键表也不得依赖合并效果宣称 exactly-once。额外读取、写入和 snapshot 开销是已接受结果，不得转化为静默丢弃重放数据的优化。
10. 纯 INITIAL_SYNC 正常运行至引擎将任务标记为全部完成时仍必须成功结束；方案 B 接受的是完成前重启的恢复进度变化，不是正常执行失败、提前完成或数据缺失。

证据链：`C-04`、`E-05`、`E-06`、`E-07`、`P-01`、`DEC-02`。

## 11. DDL 协议

当前 `runTableDdl` 的表级 drain 方向必须保留并补全：

1. 入口先检查粘滞故障和生命周期。
2. 只把目标 `tableKey` 标记为 draining，并取得目标表锁。
3. 若目标表 `bufferedRecordCount>0`，强制 commit；若 context 存在 pending commit，优先按第 7.2 节 retry。不得使用 CDC `accumulatedRecordCount` 判断是否跳过 drain：initial 数据已进入 writer、但尚未调用 `afterInitialSync` 时也必须在 DDL action 前进入 snapshot。两者均为零时不得制造空 snapshot。
4. 成功发布 committed generation 并处理由该表解除的 Heartbeat 屏障。
5. 关闭并移除目标表 context，然后执行 Catalog DDL。
6. 最后清理目标表 writer 派生状态、owner、计数、deadline 和 eligibility。`acceptedGeneration`、`committedGeneration`、各 sourceLane 的最后接收代数以及表锁对象必须在整个 Service 生命周期内保持单调/稳定：即使 DDL 后重建同名表也不得归零；否则仍引用 DDL 前代数的多表 Heartbeat 会丢失已提交证明或与新代数混淆。只有 Service close 才清除这些协调状态。
7. 不得在仍可能存在等待线程时从锁表移除表锁并为同一 `tableKey` 创建第二把锁。
8. 任何 drain、callback、close 或 DDL 失败都必须传播；DDL action 只有在此前步骤全部成功时才可执行。
9. 其他表不得被 flush、关闭或重置，继续按原阈值累计。

DDL event 自身的 offset 不在这里 callback；后续 Heartbeat 负责推进。

证据链：`C-03`、`E-03`、`DEC-01`。

## 12. 停止与关闭协议

`PaimonConnector.onStop` 必须继续传播 `PaimonService.close()` 失败。Service close 必须幂等，并按以下顺序执行：

若 `close()` 进入时 Service 仍为从未开始初始化的 `NEW`，必须原子禁止后续初始化，释放 constructor 已建立的内部强引用后直接进入 `CLOSED`；不得启动 scheduler worker、创建 Catalog、执行 Paimon commit 或 callback。若初始化已经失败并按第 6.2 节完成一次清理，后续 `close()` 不得重复任何 I/O，只重新抛出已保存的初始化/清理聚合异常。

1. 当前为 `RUNNING` 时原子切换到 `STOPPING`；当前已经为 `FAILED` 时保持 `FAILED`。两种情况都从此拒绝新写入、Heartbeat、DDL、`afterInitialSync` 和新 scheduler task。
2. 对 scheduler 调用有序 `shutdown`，阻止新任务；等待 scheduler 终止、已经登记的 ingress，以及 Consumer-start 已在线性化点发生的 callback 完成。`consumerStarted=false` 的 lane inFlight 只是预约，不作为待完成 I/O 等待，而是保留到步骤 5。不得用 `shutdownNow` 主动中断正在执行的 Paimon commit。
3. 进入 stop-drain callback-suppressed 模式后逐表强制 drain：`bufferedRecordCount>0` 时 commit，存在 pending commit 时按第 7.2 节优先 retry；两者均为零时不制造空 snapshot。不得使用 CDC `accumulatedRecordCount` 跳过含 initial writer buffer 的表。每张表成功只发布 committed generation，整个逐表循环中一律不评估、不预约、不执行 offset callback。
4. 某表失败后继续 best-effort drain 其他表并聚合异常。如果任一表 drain 失败、close 前已有粘滞故障或等待阶段发生失败，则不得发生任何 **Consumer-start 在线性化上晚于 `STOPPING`** 的 callback；不能在 drain A/B 成功后启动 callback，再因后续表 C 失败而暴露越界 offset。Consumer-start 已先于 `STOPPING` 线性化的 callback 属于停止前操作，close 只能等待并记录其结果，不能承诺撤销已经发生的外部副作用。
5. 仅当所有表 drain 全部成功、等待阶段无失败且 close 前没有粘滞故障时，退出 callback-suppressed 模式并执行一次全局 callback drain。每个 lane 必须先处理 `consumerStarted=false` 的既有 inFlight H1；H1 成功后才能移动并处理该 lane 的 pending H2。没有 inFlight 的 lane 直接评估 pending。不同 lane 没有 offset 先后合同，但所有 Consumer 调用仍串行；任一 callback 失败后保留其 inFlight/pending、立即停止其余 callback 并进入失败聚合。这里“一次”指一个全局 drain 阶段，不限制实际 callback 数量。
6. 逐表关闭 context 并释放物理表 owner，然后执行既有其他资源清理。
7. 在所有 callback 已完成或被最终抑制、且不会再发生 Consumer-start 后，清理 callback、调度器、计数、generation 和 pending offset 的强引用；这是第 5.2 节运行期不可替换/清空合同的唯一终止例外。无论是否需要抛出聚合异常，资源清理结束后最终进入 `CLOSED`。
8. 以最早发生的异常为主异常；所有后续 flush、callback、close 和 cleanup 异常通过 `addSuppressed` 保留。不得吞掉物质性关闭失败。

若进入 close 前已经是 `FAILED`，仍执行 best-effort pending commit/flush 和资源关闭，但整个 close 期间不得再 callback offset，最终以原粘滞故障为主异常抛出。

close 的中断语义必须同时满足“完成清理”和“保留中断”：

1. 等待 scheduler、in-flight ingress/callback 或 stop-drain retry delay 时捕获 `InterruptedException`，把首次中断按真实发生顺序记为主异常或 suppressed。
2. 捕获后调用等价于 `Thread.interrupted()` 的操作暂时清除 interrupt 状态，并继续不可中断等待循环，直到 scheduler 已终止且 in-flight 为零；随后仍执行全部表 drain、context close 和资源清理。不得因一次中断永久跳过仍在运行的 commit，也不得在其运行时关闭底层资源。
3. 清理全部结束、保存 close 聚合结果后，必须在返回或抛出之前调用 `Thread.currentThread().interrupt()` 恢复当前关闭线程的 interrupt 标记。
4. 中断本身是物质性 close 失败；即使其余清理成功，首次 `close()` 也必须抛出保存的聚合异常。若另一个更早的粘滞故障已存在，中断作为 suppressed 保留。

重复调用 `close()` 不得再次执行 scheduler 等待、flush、callback 或资源关闭：首次 close 成功时后续调用直接返回；首次 close 失败时后续调用重新抛出首次保存的聚合异常，不能把已经失败的关闭改报为成功。

证据链：`H-03`、`C-03`、`C-06`、`DEC-01`。

## 13. 锁顺序与线程安全约束

实现必须遵守以下顺序，任何代码路径不得反向获取：

1. ingress/lifecycle gate 只负责状态、in-flight 计数和 Consumer-start 原子标记；不持有它执行 Paimon I/O、外部 callback 或等待其他锁。
2. 表级生命周期锁保护单表 writer/commit/generation/DDL；持有表锁时可以短暂进入 coordinator 发布状态。
3. coordinator 锁只保护 generation 依赖、pending Heartbeat 和 callback token；不得持有它等待表锁或执行 Paimon I/O。
4. callback 执行锁只保护外部 Consumer 调用；不得在持有表锁或 coordinator 锁时获取。持有 callback 执行锁后允许短暂进入 lifecycle gate 完成 Consumer-start；反向的 lifecycle gate -> callback 执行锁被禁止，close 必须先释放 lifecycle gate再等待或执行 callback。
5. callback 返回后可重新进入 coordinator 锁按 token 确认结果。

允许的方向是“表锁 -> 短暂 coordinator 状态更新 -> 释放全部锁 -> callback 执行锁 -> 短暂 lifecycle gate 标记 Consumer-start -> 释放 lifecycle gate -> callback -> coordinator 完成确认”。禁止“coordinator 锁 -> 表锁”“lifecycle gate -> callback 执行锁”和“任意内部状态锁内调用外部 callback”。

所有共享字段必须通过同一锁协议或具有明确的原子/volatile 可见性；不得依赖 `ConcurrentHashMap` 本身来证明跨多个字段的状态转换原子性。

## 14. 失败分类与传播

### 14.1 必须形成粘滞故障

- 已开始写入后的行转换、路由或 writer 失败；
- `prepareCommit`、`filterAndCommit`、commit state save 失败后不存在 pending 幂等确认路径，或第 7.2 节 3 次追加确认全部耗尽；
- scheduler 扫描自身抛出 Throwable，或其提交在完成第 7.2 节有界确认后仍失败；
- offset callback 抛出的异常；
- DDL drain、context close 或 Catalog DDL action 失败；
- 无法证明继续写入不会越过既有失败的状态不一致；
- 任一 CDC DML 缺少有效 sourceLane，或 offset-bearing 目标 CDC DML 缺少 callback；
- CDC Heartbeat 的 stage、streamOffset、`Long sourceTime`、nodeIds/sourceLane 或 callback 任一不满足第 6.1 节；Heartbeat stage 缺失、类型错误或未识别；
- 普通运行入口的 pending retry delay 被中断。

首个故障使用 compare-and-set 保存。后续入口抛出的包装异常必须保留首个故障为 cause；后续关闭错误只能作为 suppressed，不能覆盖根因。

### 14.2 不得发生的失败处理

- 不得只日志后继续 scheduler。
- 不得在 callback 失败后删除 pending offset。
- 不得在某表提交失败后回调同一 Heartbeat 的其他已成功表 offset。
- 不得把仍有 pending 且尚未完成 3 次追加确认的第一次 commit 异常提前升级为粘滞故障。
- 不得把 close/DDL drain 失败降级为成功。
- 不得因 callback 不提供 durability ack 而退回“每次 `writeRecords` 同步提交”；at-least-once 由“数据先于 offset”保证。

## 15. 代码结构约束

实现阶段允许在 `io.tapdata.connector.paimon.service` 中增加内部协调类。职责必须按以下边界组织：

- `PaimonService`：对外入口、表 context 生命周期、配置和 Paimon I/O 编排。
- 新的微批/offset coordinator：generation、Heartbeat 依赖、callback token 和 deadline 状态；不得直接写 Paimon。
- scheduler 适配层：计算最近 deadline、登记 ingress、调用 Service 的表级 flush；不得自行修改 committed generation。
- `PaimonTableWriteContext`：继续只负责单表 writer、prepared messages、committer 和 commit state；不得保存源 offset。
- `PaimonConnector`：注入 callback、转发 Heartbeat、传播停止失败；不得实现表级提交算法。

必须删除 `ASYNC_OFFSET_CONTRACT_VERIFIED` 及其“固定 false”旁路，并完整删除基于 `firstOffsetByTable`、`committedOffsetTables`、`offsetCallbackLock`、`getFirstOffsetByTable()` 和 `commitCallback` 的旧表序推断及所有调用点。不得通过把常量改为 `true` 或只恢复 setter 赋值来激活这套死代码。

代码兼容当前模块 Java 8；不得引入仅高版本 Java 可用的 API。

实现 diff 必须保持 Feature-local：生产代码变更只允许出现在 `PaimonConfig`/`spec.json` 的既有微批配置与 capability、`PaimonConnector` 的 callback/processControl 接线、`PaimonService` 的相关提交入口，以及新增的 package-private 协调类中。`PaimonTableWriteContext`、`PaimonCommitStateStore` 及 writer/router 只能通过现有接口复用，不得修改其语义或持久化格式。不得为了代码整洁改写不相关方法；每个既有文件 hunk 都必须在评审中说明与本 Spec 条款的直接对应关系。

`PaimonService` 的 batch/stream read、schema discovery、Catalog options/storage/CatalogFactory 语义以及 `PaimonConnector` 的既有非微批 capability 注册方法不得出现行为改动。`PaimonService.init()` 只允许出现与第 6.2 节直接对应的生命周期成功发布、scheduler adapter 初始化和初始化失败清理 hunk，不得改写 Catalog 配置或创建逻辑。若测试需要可控 clock/scheduler/retry strategy，只能通过 package-private 注入点增加，不得改变 public PDK 接口或生产默认行为。

证据链：`C-08`、`C-09`、`DEC-01`。

## 16. 测试策略

### 16.1 测试原则

- 时间测试必须注入 fake clock 和可控 scheduler，不得用 `Thread.sleep` 等待真实 30 秒。
- 并发测试必须用 latch/barrier 明确控制交错，不得依赖概率性竞态。
- 单元测试验证 coordinator 状态机；Service 测试验证锁、Paimon context 和生命周期编排；本地 Paimon integration test 验证实际 snapshot 数量和 pending commit 行为。
- 每个失败测试都必须同时断言：首个故障被保留、后续入口失败、offset 未推进、close 没有吞错。
- 现有 paimon-connector 测试必须作为完整回归集运行；除本 Spec 明确改变的微批 snapshot/offset 时机断言外，不得修改既有测试期望来迁就实现。
- 既有 `Paimon*BucketWriterStrategyTest`、`PaimonWriteSemanticContractResolverTest`、`PaimonDmlImageValidatorTest`、`PaimonServiceDynamicBucketIntegrationTest`、`PaimonServiceCreateTableValidationTest`、`PaimonServicePrimaryKeyChangeTest`、`PaimonTableWriteContext*Test`、`PaimonCommitStateStoreTest` 和 `DateTimeConversionTest` 必须保持通过，作为非目标功能回归门禁。
- 对缺少既有自动化覆盖的读端、schema discovery 和连接测试路径，必须用 diff function-context 审查证明对应方法没有实现 hunk；不能以“本次没计划修改”替代检查结果。

### 16.2 必须新增或扩展的测试

建议按职责使用以下测试类名；实现可以合并文件，但不得删减场景：

#### `PaimonMicroBatchCommitTest`

- 同表 3 次小 batch 跨调用累计，阈值前 snapshot 数不变，达到阈值只新增 1 个 snapshot。
- 两张表分别计数；A 达阈值只提交 A，B 不提交。
- `batchAccumulationSize<=0` 每次非空 CDC 调用提交。
- 配置缺失或显式反序列化为 `null` 时分别回落到 `100000/30000/true`，不得进入旧实现的 null 即时提交/关闭时间能力分支。
- `enableAsyncCommit=false` 不创建 scheduler，但下一次调用达到时间阈值时提交。
- CDC-only 服务启动后先空闲超过 interval，第一批小 batch 只初始化表级时间基准而不立即提交；从该批到达起满 interval 后由 scheduler 提交。
- 该表已有一次成功 commit 后再空闲超过 interval，下一批 CDC 数据在当前调用内提交。
- initial batch 即使达到数量/时间阈值也不提交。
- INITIAL_SYNC_CDC 表的 `afterInitialSync` 成功后空闲超过 interval，首个小 CDC batch 在当前调用内立即提交；测试同时断言 initial 记录数没有进入 CDC 累计计数。
- 当前 batch commit 结果不明但形成 pending 时，最多 3 次重试复用同一 messages/identifier；确认成功后 source batch 不被再次写入。
- 三次追加确认耗尽时首个异常成为 sticky cause，三次 retry 异常按顺序成为 suppressed；pending 不被丢弃。
- context 无 pending 时直接调用 `retryPendingCommit()` 返回 `nextCommitIdentifier-1`，且不调用 committer、不推进 identifier、不保存状态；Service 不得把该返回值用于发布 generation、清计数、更新时间或 callback。

#### `PaimonAsyncCommitSchedulerTest`

- 低流量 CDC 表没有后续 write 时在 deadline 到达后提交。
- initial-only 表永不被扫描提交。
- 写线程与 scheduler 同时到期时只产生一次 commit。
- DDL drain 与 scheduler 竞争时，同表串行且 DDL 前完成 drain。
- scheduler 异常形成 sticky failure、停止重调度，下一次所有规定入口均抛出。
- scheduler 第一次 commit 异常且有 pending 时在同一 task 内最多确认 3 次；中途成功不 sticky，耗尽后才 sticky。
- `commitIntervalMs<=0` 或 `enableAsyncCommit=false` 不创建 scheduler。
- 默认配置下，Service 初始化、纯 initial、连接测试、元数据操作和源端读取均不创建 worker；首次出现 scheduler-eligible 的未提交 CDC 状态时才创建且只创建一个 daemon worker。首批 CDC 已在调用内提交且无 pending 时不创建 worker。
- deadline 重新安排不会人为多等待一个完整 interval。
- fake clock 到达 `commitIntervalBaseTimeMs+commitIntervalMs` 时任务即具备提交资格；不得等待旧全局固定相位的下一轮扫描。

#### `PaimonOffsetBarrierCoordinatorTest`

- A/B 表调用顺序互换，Heartbeat 都必须等 A/B committed generation 全部满足。
- A 已提交、B 未提交时禁止 callback；B 成功后自动 callback。
- sourceLane S1 只被 A 接收、B 只接收 S2 时，S1 Heartbeat 的 `requiredGenerationByTable` 只包含 A，不得为 B 填入 generation `0`；S2 同理。
- 同一 sourceLane 的 H1 仍被表依赖阻塞时到达 H2，只保留并最终 callback H2；不比较 offset 内容。H1 已经 callback 成功后才到达 H2 时，两者按到达顺序分别 callback。
- 两个 sourceLane 的依赖和 reservation 相互独立；coordinator 只保证每个 lane 至多一个 inFlight，并返回带唯一 token 的待执行决策，不调用或串行化外部 Consumer。Consumer 全局不并发由 Service/集成测试验证。
- 用 latch 固定 H1 callback-start 后再送入 H2：H1 保持 inFlight、H2 成为 pending，H1 成功只清自身，随后 H2 callback；两次严格按 H1、H2 顺序。
- H1 callback 失败时保留失败 inFlight 和最新 H2 pending、形成 sticky failure、禁止其他 lane 和 H2 callback。
- Heartbeat 无表依赖时立即 callback。
- 纯 CDC、零 DML 的合法 Heartbeat 依赖集合为空：callback 成功且 Paimon snapshot 数不增加。
- CDC Heartbeat callback payload 的 streamOffset、sourceTime、nodeIds、stage 和 nullable eventTime 与输入一致；非空 `referenceTime` 不得丢失，输入为 `null` 时不得伪造。
- INITIAL_SYNC Heartbeat no-op，不登记、不提交、不 callback。
- CDC Heartbeat 分别缺失 stage、streamOffset、sourceTime、nodeIds，或 sourceTime 不是 `Long`、stage 未识别时，均在登记屏障前 sticky fail；引擎 callback 计数保持 0。
- malformed/missing nodeIds 的任一 CDC DML 在写入前失败。

#### `PaimonMicroBatchOffsetIntegrationTest`

- 使用本地 Paimon 创建真实目标表 A、B，设置 `batchAccumulationSize=3` 并注入 fake clock/可控 scheduler。
- A 的一个 CDC batch 包含 sourceLane S1、S2 的 3 条记录并达到数量阈值，A 只新增 1 个 snapshot；B 的一个 CDC batch 包含 S1、S2 的 2 条记录但未达到数量阈值，B snapshot 数不变。
- 分别送入 S1、S2 的有效 Heartbeat；两者均依赖 A/B 已接收 generation，A 已提交而 B 未提交时 callback 次数保持 0。
- fake clock 到达 B 的 deadline 并执行 scheduler 后，B 只新增 1 个 snapshot；S1、S2 callback 各执行一次且 Consumer 不并发，每个 payload 的 streamOffset、sourceTime、nodeIds、stage、eventTime 与对应 Heartbeat 完全一致。
- 该测试必须经过 `PaimonService.writeRecords`、表级 generation 发布、真实 `PaimonTableWriteContext` commit、scheduler 和 `PaimonConnector.processControl` 接线；不得只直接调用 coordinator 伪造 committed generation。

#### 扩展 `PaimonServiceInitialSyncPendingTest`

- initial 多次 write 不产生阈值 snapshot，`afterInitialSync` 只强制提交一次。
- initial `TapRecordEvent.info` 不含 `batchOffset` 时不尝试收集或 callback offset。
- 多表并发 `afterInitialSync` 的 Paimon I/O 可独立进行。
- initial commit 在有 pending 时先完成统一的 3 次确认；确认耗尽或无幂等路径的失败向引擎传播并形成 sticky failure，首个 CDC Heartbeat 前停止不会推进 offset。
- `bufferedRecordCount` 和 CDC `accumulatedRecordCount` 均为零但存在 pending commit 时仍重试。

#### 扩展 `PaimonServiceTableDdlCacheInvalidationTest`

- DDL 只 flush 目标表，其他表计数和 deadline 不变。
- initial 数据已写入目标表 writer、`bufferedRecordCount>0`、CDC `accumulatedRecordCount=0` 且尚未执行 `afterInitialSync` 时，DDL 必须先把这些 initial 数据提交到 snapshot，再关闭 context 和执行 DDL action。
- pending commit 在 DDL action 前完成。
- DDL pending commit 复用统一的 3 次确认协议；确认耗尽时 DDL action 不执行。
- drain、callback 或 context close 失败时 DDL action 不执行且异常传播。
- DDL offset 不直接 callback，后续 Heartbeat 才推进。

#### 扩展 `PaimonConnectorStopTest`

- `onStop` 先阻止新入口，等待 in-flight scanner/write，然后 flush 全表。
- initial 数据已写入 writer、CDC `accumulatedRecordCount=0` 且尚未执行 `afterInitialSync` 时，正常 `onStop` 必须依据 `bufferedRecordCount` 提交该表；测试在重启前直接验证对应 snapshot 已包含这些记录，不能用重启后的重放掩盖漏 drain。
- 成功 flush 后释放安全 Heartbeat，再关闭 context。
- 既有 sticky failure 时仍 best-effort drain，但绝不 callback。
- H1 已 reservation 为 inFlight 但 Consumer 尚未开始，随后 close 先切到 STOPPING：全表 drain 成功时全局 drain 先 callback H1，再 callback 同 lane pending H2，二者各一次且顺序固定。
- H1 已 reservation 但 Consumer 尚未开始，A/B 表 drain 成功、之后 C 表 drain 失败：STOPPING 后 Consumer callback 次数为 0；A/B generation 可发布，H1/H2 在最终资源清理前保持未确认。
- H1 的 Consumer-start 先于 STOPPING 线性化且被 latch 阻塞时，close 等待 H1 完成且不重复 callback；即使之后 C drain 失败，也只允许已经开始的 H1 结果，STOPPING 后不得开始 H2 或其他 callback。
- 多个 close 错误保留首个根因和 suppressed；重复 close 幂等。
- scheduler 不使用强制中断结束正在进行的 Paimon commit。
- close 等待 in-flight 时中断：清除中断后继续等到 commit 退出，完成全表 drain/context close/资源清理，最后恢复 interrupt 并抛聚合异常；第二次 close 不执行 I/O，只重抛已保存结果。

#### 扩展 `PaimonSpecTest`

- `spec.json` 必须声明且只声明一个 `flush_offset_callback` capability。
- 三个配置默认值分别为 `100000`、`30000`、`true`。
- 英文、简体中文、繁体中文 placeholder 均显示 `100000`。

#### Connector callback wiring 测试

- `onStart` 把 context callback 注入 Service。
- callback 为 null 时 connection test/元数据操作/源端能力不失败，且不会创建 scheduler worker。
- 首个 offset-bearing 目标 CDC DML 在写入前拒绝 null callback；首个有效 CDC Heartbeat 在登记屏障前拒绝 null callback。
- `processControl` 只把 Heartbeat 委托给 Service，并传播 Service Throwable；其他 control event no-op。
- 初始化全部成功前 Service 保持 `NEW`；配置校验、Catalog 或 scheduler adapter 初始化失败时不进入 `RUNNING`、不启动 worker，并以原始初始化异常为主因完成资源清理。
- 初始化前直接 `close()` 不创建 Catalog/worker、不执行 commit/callback；初始化失败后的重复 `close()` 不重复清理，只重抛保存的聚合异常。
- callback 在 `RUNNING/STOPPING/FAILED` 不可替换或清空；close 完成 callback drain/抑制后允许释放强引用，之后不再发生 callback。

#### 纯 INITIAL_SYNC 方案 B 运行时验收

- 必须使用锁定引擎与待验 Paimon Connector 构造真实纯 INITIAL_SYNC 任务；该验收可以使用外部测试环境，但不得通过修改引擎代码或伪造 Connector 可见的 initial `batchOffset` 实现。
- 正常运行至全部表完成时任务成功结束，源端每条数据至少存在于目标；不得因 callback 模式跳过未完成记录。
- 分别在“某表已有 initial writer buffer 但尚未执行 `afterInitialSync`”“部分表已完成”和“全部表已完成但任务尚未被标记完成”三个时点，对进程崩溃、强制停止、正常手动停止后再启动逐一验收。恢复起点允许早于已完成位置、最坏允许从头开始；测试不得把扩大重放判为失败。正常手动停止还必须在重启前证明 stop drain 已把当时所有完整接收的 writer buffer 提交；崩溃和强制停止不要求事后补 flush。
- append-only 验收数据必须带测试专用稳定事件 ID。对每个重启场景，断言“源端唯一事件 ID 集合是目标事件 ID 集合的子集”；目标允许包含同一 ID 的多条记录，且总行数允许大于源端唯一事件数。禁止仅用总行数 `>=` 判断不丢数，因为重复事件不能替代缺失事件。测试报告必须同时记录重放起点、缺失 ID 集合（必须为空）、重复 ID/行数和新增 snapshot 数。
- 使用主键目标时只断言不丢失源端最终键集合；不得把主键合并后的表面去重当作 exactly-once 证明。
- INITIAL_SYNC_CDC 必须分别在首个有效 CDC Heartbeat callback 前后重启：callback 前允许重放 initial；callback 后只能依据引擎实际已持久化进度恢复，不得由 Connector 推测 callback 已获得 durability ACK。

### 16.3 验证命令

实现阶段至少执行：

```bash
mvn -pl connectors/paimon-connector -DskipTests compile
mvn -pl connectors/paimon-connector test
git diff --function-context -- connectors/paimon-connector/src/main
git diff --check
```

如仓库私有 `SNAPSHOT` 依赖导致 Maven 无法解析，必须记录完整 blocker，并继续执行所有不依赖该解析的静态检查和已可运行的定向测试；不得把未执行测试报告为通过。

### 16.4 Spec 核实记录

2026-07-27 已执行：

- `jq empty connectors/paimon-connector/src/main/resources/spec.json`：通过，当前 JSON 语法有效。
- 对本文执行 `git diff --no-index --check /dev/null <spec-file>`：通过，无 whitespace error。
- 证据 ID/引用闭包检查：新增 `H-05`～`H-07`、`C-10`、`E-07`、`DEC-02` 后共 29 个证据 ID；均已定义并被引用，无未定义 ID；本文相对源码链接均存在。
- 第一轮 fresh-context 只读对抗审查识别并修正 8 项缺口：CDC Heartbeat payload、lane pending/inFlight、stop callback 抑制、pending 3 次确认、close 中断、initial→CDC 证据链、stop 前已预约 callback、空依赖 Heartbeat。
- 三方等价性补充后的第二轮 fresh-context 审查提出 2 个 HIGH、2 个 MEDIUM finding。源码复核后的裁决为：纯 INITIAL_SYNC/global capability 冲突成立；initial→CDC 时间基准 finding 属于合同误读，已用两种场景消除歧义并保持已确认语义；eventTime 丢失和跨层多表/多 sourceLane 测试缺口成立，已补充确定合同与测试。用户随后通过 `DEC-02` 明确选择方案 B，把纯 initial 冲突转化为已接受的 at-least-once 取舍，而非未解决缺陷。用户已明确跳过此前那轮跨模型复核。
- 对 `E-07` 再次逐项核实：`flushOffset` 初值为 `false`，锁定引擎中只有 Heartbeat 平台分支和 `flushOffsetCallback` 把它置为 `true`；完成表事件只更新 `SyncProgress.batchOffsetObj` Map；定时保存与关闭保存均先检查该标志；纯 INITIAL_SYNC 没有后续 stream 事件。因此方案 B 接受的准确风险是“内存进度未触发持久化”，不是“完成表 offset 没有写入内存”。
- 历史 `821ff7e...` 源码再次核实：旧首次时间基准在第一批被累计的表数据到达时初始化；新合同明确区分 CDC-only 首批到达基准与 INITIAL_SYNC_CDC 的 `afterInitialSync` 成功基准，并拒绝旧固定相位延迟。
- 方案 B 最终稿的定向对抗审查提出 2 个 HIGH、1 个 MEDIUM finding，均成立并已闭合：新增全部阶段 `bufferedRecordCount`，保证 initial 尚未执行 `afterInitialSync` 时 DDL/正常 stop 仍强制 drain；append-only 重启验收改为逐事件 ID 集合包含关系；补齐 initial writer buffer 未完成时的 stop 单元测试与运行时重启时点。另一轮复核未发现额外问题。用户随后批准进入 Plan；本轮未执行额外跨模型复核。
- Plan 审查识别并闭合 5 项一致性缺口：Service 构造后保持 `NEW` 且只在初始化全部成功后发布 `RUNNING`；scheduler worker 改为首次出现 scheduler-eligible 未提交 CDC 状态时惰性启动；callback 运行期不可清空增加终止清理例外；外部 Consumer 串行化验收从纯 coordinator 移到 Service/集成层；任务依赖固定为 `T2 -> T3` 且 `T1+T2+T3+T4 -> T5`。
- GLM 审查提出的 8 项经源码逐项复核：采纳 M1、M3、L1、L3、L4、L5，并将 L2 作为保持现有空 pending no-op 的防误实现合同；M2“`firstOffsetByTable` 当前生产路径无界增长”不成立，因为唯一 setter 强制 callback 为 `null`，受非空 guard 保护的 Map 写入不可达。本轮据此补全 `C-01/C-02/C-04/H-04/H-06`、三方对比、协议、测试和任务验收，不改变已批准设计或实施范围。
- `mvn -pl connectors/paimon-connector -Dtest=PaimonSpecTest test`：测试未运行，依赖解析阶段失败。缺失的内部依赖为 `tapdata-pdk-runner:2.5-SNAPSHOT`、`tapdata-pdk-api:2.0.8-SNAPSHOT`、`sql-core:1.0-SNAPSHOT`、`pdk-error-code:2.0-SNAPSHOT`。该结果是当前环境 blocker，不是测试通过，也不是本文档导致的编译失败。

## 17. 可观测性

必须保留或增加以下结构化信息，且不得记录 offset payload、密钥或记录内容：

- 表 commit：`tableKey`、触发原因（size/time/scheduler/DDL/initial/stop/pending-retry）、记录数、目标 generation、耗时。
- Heartbeat：`sourceLane` 的脱敏稳定标识、version、依赖表数、blocked/ready/callback-success 状态。
- sticky failure：首个根因和发生阶段，后续日志引用同一根因，不重复覆盖。
- scheduler：启动、停止、重新安排 deadline 和异常终止。
- close：drain/offset/close 各阶段结果及 suppressed 错误数量。

## 18. 验收标准

全部条件同时满足才可声明实现完成：

1. 在默认配置下，连续几十条或几条的小 batch 不再每次新增 Paimon snapshot；同表累计到 `100000` 或 `30000ms` deadline 才提交，强制 flush 场景除外。
2. 低流量表无后续 write 时仍由 scheduler 提交。
3. 多表 Heartbeat 在所有依赖表 commit 前从不 callback；表调用顺序改变不影响结果。
4. 所有回调的 offset 都只覆盖已经成功进入 Paimon snapshot 的数据。
5. 任一异步失败可在下一次规定入口或 `onStop` 被引擎观察，且失败后没有 offset callback。
6. initial sync、DDL、pending commit、stop 和 CDC callback failure 场景全部通过确定性测试。
7. 当前 `PaimonTableWriteContext` 的 commit recovery、bucket mode、DDL cache invalidation 和停止错误传播测试不回归。
8. 只改动 `connectors/paimon-connector/`。
9. 模块 compile/test 通过；若仅被外部私有依赖阻塞，报告必须精确区分“未运行”和“失败”。
10. 文档、配置、日志和代码不得使用 exactly-once 表述；统一声明 connector-managed at-least-once。
11. 除本 Spec 明确列出的微批 snapshot/offset/失败/关闭协作，以及 `DEC-02` 接受的纯 INITIAL_SYNC 完成前重启扩大重放外，现有 Paimon Connector 的 DML 映射、类型转换、动态 bucket、Schema/DDL action、Catalog、commit state 格式、读端、连接测试和元数据行为均通过既有测试或定向回归证明未改变。
12. 最终 diff 中每个生产代码 hunk 都能映射到本 Spec 的具体条款；不存在无关重构、依赖升级、公共 API 变更或跨模块修改。
13. 第 4.6 节三方对比中的四项有效 CDC 微批能力均通过测试：跨调用累计、数量阈值、调用内时间阈值、无后续写入时的后台时间提交；所有标记为“刻意不等价”的历史错误均有反向回归测试，最终报告不得使用“与旧实现完全一致”或“逐时序等价”的表述。
14. 方案 B 的纯 INITIAL_SYNC 运行时验收通过：任务正常完成时不丢数据；完成前三个指定时点发生三类重启时允许扩大重放。append-only 目标允许重复，但必须以稳定事件 ID 证明源端唯一事件集合是目标事件集合的子集；主键目标必须证明源端最终键集合不缺失。报告必须明确该结果是 `DEC-02` 接受的 at-least-once 行为，不能表述为保持当前断点恢复效率。
15. 发布说明必须显式披露方案 B：静态 capability 影响所有同步类型；纯 INITIAL_SYNC 完成前重启可能从更早位置、最坏从头重放，并可能为 append-only 目标制造重复数据和额外 snapshot。
