# Implementation Plan：Paimon Connector 微批提交与多表 at-least-once

## 1. 计划状态

- 阶段：Spec-driven Development Phase 2（Plan）。
- 状态：Draft，待用户审阅批准后进入 Phase 3/4；本计划不授权修改生产代码。
- 事实源：[Paimon Connector 微批提交与多表 at-least-once Spec](../connectors/paimon-connector/docs/paimon-micro-batch-commit-at-least-once-spec.md)。
- 当前 Connector 基线：`ddb1e7d7bb3c94468447fcfca9d0fb283062703c`。
- 历史 Feature 基线：`821ff7e33633a54e6abb5f0919d505d42e1098a5`。
- 锁定引擎基线：`f91bfe4a66ea99362440ca87c36b4c1883ca4cd9`。
- 允许范围：`connectors/paimon-connector/` 及本次计划/验证文档。
- 禁止范围：Tapdata 引擎、PDK/common-lib、Apache Paimon、其他 Connector、公共 PDK API及依赖版本。

## 2. 目标与完成定义

在不改变 Tapdata 引擎的前提下，以方案 B 恢复 Paimon Connector 的 CDC 微批：同表跨 `writeRecords` 累计，默认按 `100000` 条或 `30000ms` 提交；无后续写入时由后台线程按表级 deadline 提交；DDL 前 drain 目标表，停止时 drain 全表；多表、多 sourceLane 的数据 snapshot 必须先于覆盖它的 offset callback。

实现完成必须同时满足：

1. 小 batch 不再每次创建 Paimon snapshot；数量、调用内时间和后台时间三种触发均有测试。
2. Paimon commit 成功前不推进覆盖其数据的 CDC offset；跨表调用顺序不影响结果。
3. 复用当前 `PaimonTableWriteContext` 的 stable commit user、pending messages、identifier 和 state store，不恢复旧 writer/committer。
4. initial 不参与 CDC 阈值，但其 writer buffer 对 DDL/stop drain 可见。
5. scheduler、写入、DDL、callback、stop 的并发与失败均遵循 Spec 的生命周期、锁序和粘滞故障合同。
6. 纯 INITIAL_SYNC 按 `DEC-02` 验收：完成前重启允许更早/最坏从头重放和 append-only 重复，但逐事件证明不丢数据。
7. 非微批行为通过既有定向测试和 function-context diff 证明没有改变。

## 3. 当前代码事实与改动落点

| 现状 | 源码锚点 | 计划落点 |
| --- | --- | --- |
| `ASYNC_OFFSET_CONTRACT_VERIFIED=false` 关闭 scheduler，并使 CDC 每次提交 | `PaimonService.java:82-87,272-320,1276-1285` | 在安全状态机、提交 helper 和 callback 接线完成后删除固定旁路；不得直接改为 `true`。 |
| 当前按表 context、表锁、pending commit 和 sticky failure 已存在 | `PaimonService.java:108-165,1191-1208,1339-1377`；`PaimonTableWriteContext.java:185-247` | 保留并复用；新增协调状态不拥有 Paimon writer/committer。 |
| DDL drain 只检查 CDC 共用 count，count 为零直接跳过 | `PaimonService.java:986-1015,2329-2363` | 分离全阶段 `bufferedRecordCount` 与 CDC `accumulatedRecordCount`。 |
| `close()` 先 `flushAll()` 再关闭 context，但没有统一 ingress/scheduler/callback 线性化 | `PaimonService.java:3288-3325` | 新生命周期 gate；有序 shutdown、全局 drain、异常聚合和幂等 close。 |
| callback setter 丢弃 callback，`processControl` 是 no-op；旧 `firstOffsetByTable`、`committedOffsetTables`、`offsetCallbackLock`、getter、写入收集分支、提交调用点和 `commitCallback` 仍完整存在但不可达 | `PaimonService.java:140-153,210-214,1116-1147,1179-1208,1227-1248,1261-1318,1339-1369,2329-2363,3327-3382`；`PaimonConnector.java:484-488` | 完整删除旧表序 callback 机器及调用点；以 Heartbeat generation 屏障替代。不得只恢复 setter 赋值。 |
| `spec.json` 没有 `flush_offset_callback`；`PaimonConfig.java` 注释和三种 JSON placeholder 仍写 `10000` | `PaimonConfig.java:87-97`；`spec.json:11-20,491-542,694-697,751-754,808-811` | 激活切片中恢复且只恢复一个 capability；Java 注释和三种配置文案统一为 `100000`。 |

## 4. 架构决策

### 4.1 `PaimonMicroBatchCoordinator`

新增 package-private `PaimonMicroBatchCoordinator`，只维护纯协调状态，不访问 Catalog、writer 或 committer：

- 表状态：`bufferedRecordCount`、CDC `accumulatedRecordCount`、时间基准、accepted/committed/pending generation、sourceLane 依赖和 CDC eligibility。
- lane 状态：不可变 Heartbeat payload、`pending`、`inFlight`、version/token 和 `consumerStarted`。
- 决策输出：当前 batch 是否因 size/time 提交、表级最近 deadline、哪些 lane 已 ready、callback reservation/complete/fail。
- DDL 后表 generation 和锁身份保持到 Service close；只清 writer 派生状态与 deadline。

### 4.2 `PaimonAsyncCommitScheduler`

新增 package-private `PaimonAsyncCommitScheduler`：

- worker 被激活后，生产路径使用且只使用一个 daemon `ScheduledExecutorService`。
- Service 初始化只建立无工作线程的 adapter；首次出现 scheduler-eligible 的未提交 CDC 状态后才幂等、惰性创建唯一 worker。首批 CDC 已在调用内提交且无 pending、纯 initial、连接测试、元数据操作和源端读取均不创建 worker。
- 只安排最近表级 deadline 的 one-shot task；状态变化后重算，不使用旧版全局固定相位 `scheduleAtFixedRate`。
- 到期 task 取得当时全部到期表的稳定快照并逐表调用 Service 提供的 flush action，不直接修改 generation。
- 暴露 package-private clock/executor 注入点，测试使用 fake clock 和可控 executor，不使用真实 `sleep(30000)`。

### 4.3 `PaimonServiceLifecycle`

新增 package-private `PaimonServiceLifecycle`，实现 `NEW -> RUNNING -> STOPPING/FAILED -> CLOSED`：

- 所有写入性入口和 scheduler task 使用同一 ingress token 登记/注销。
- callback reservation 与 Consumer-start 分离；Consumer-start 与 `STOPPING` 切换共用一个线性化 gate。
- close 等待已开始 ingress/callback，不持有表锁、coordinator 锁或 callback 执行锁。
- 中断时完成清理、恢复 interrupt 标记并抛聚合异常；重复 close 不重复 I/O。
- `PaimonService` 构造完成 coordinator/lifecycle/scheduler adapter 内部状态后保持 `NEW`；`PaimonConnector.onStart` 先注入可用 callback，再调用 `init()`。配置校验、Catalog 创建和 adapter 初始化全部成功后才原子发布 `RUNNING`；任一步失败都保存首因、best-effort 清理并以 suppressed 聚合后进入 `CLOSED`。callback 在 `RUNNING/STOPPING/FAILED` 不可替换或清空，仅在终止清理且不会再发生 Consumer-start 时释放强引用。该边界保留 package-private 测试注入方式，不要求无关测试启动真实 Catalog。

### 4.4 `PaimonService` 仍是唯一 Paimon I/O 编排者

`PaimonService` 增加一个统一的表锁内提交 helper，所有触发原因共用：

- `SIZE`、`CALL_TIME`、`SCHEDULER`、`INITIAL`、`DDL`、`STOP`、`PENDING_RETRY`。
- helper 先确认旧 pending，再决定是否写/prepare/commit；成功后一次性发布 coordinator 状态。
- 结果不明时最多追加 3 次同 pending 确认，保留 1000ms 生产间隔；不重写已进入 writer 的 source batch。
- `retryPendingCommit()` 的空 pending 返回保持现有幂等 no-op；它不构成本轮 commit 成功证据，不得发布 generation、清计数、更新时间或 callback。
- callback 始终在表锁和 coordinator 锁外执行，并由 Service 级 callback 执行锁串行化。

### 4.5 方案 B 的激活顺序

静态 capability 是最后激活的生产切片。此前可以加入并测试内部协调类与 Service 的 callback 模式，但 `PaimonConnector` 不注入 callback，当前生产路径继续同步提交。只有表状态、pending、scheduler、DDL/initial 和 stop/close 接线全部完成后，才同时：

1. 删除中间阶段的 null-callback 同步降级，并确认固定 false 旁路，以及旧 `firstOffsetByTable`、`committedOffsetTables`、`offsetCallbackLock`、getter、`commitCallback` 与全部调用点均已清除；
2. 在 `PaimonConnector.onStart` 注入 callback，并在 `processControl` 转发 Heartbeat；
3. 在 `spec.json` 恢复唯一 `flush_offset_callback` capability；
4. 按 Spec 对目标 CDC 做 preflight fail-fast。

这不是最终动态开关；最终产物只有方案 B 的静态 capability 合同。

## 5. 依赖图与实施顺序

```text
已批准 Spec
    |
    +--> T1 配置默认值/文案
    |
    +--> T2 表与 offset 纯状态协调器 --> T3 deadline scheduler 基础
    |
    +--> T4 生命周期 gate

T1 + T2 + T3 + T4 --> T5 CDC 写入与统一 commit helper
T5 --> T6 initial / pending / DDL drain
T3 + T5 + T6 --> T7 scheduler 接入
T4 + T6 + T7 --> T8 stop/close 协作
T2 + T5 + T6 + T7 + T8 --> T9 capability + Connector callback 激活

T6 + T7 + T8 + T9
    |
    +--> T10 两表/两 lane 跨层集成与非回归
            |
            +--> T11 锁定引擎纯 INITIAL_SYNC/INITIAL_SYNC_CDC 运行时验收
                    |
                    +--> T12 全量回归、diff 审计和验证报告
```

## 6. 分阶段计划与 Checkpoint

### Phase A：无行为激活的基础组件

- T1：规范化三个配置默认值，并修正 Java 注释和三种 JSON 文案。
- T2：实现表状态、generation 和 lane offset 屏障的纯状态协调器；只产出 callback reservation/token 决策，不执行 Consumer。
- T3：在 T2 deadline 合同固定后，实现惰性启动、最近 deadline 的可控 scheduler；T3 完成后才能把 adapter 接入 Service 初始化。
- T4：实现 Service 生命周期与 callback Consumer-start gate。

Checkpoint A：新增基础组件的定向测试通过；Connector 仍未声明 capability，当前生产 offset 行为未改变。

### Phase B：Service 微批纵向切片

- T5：接入 CDC 跨调用累计、size/call-time 判断和统一 commit helper。
- T6：统一 pending 确认，固定空 pending no-op 的防误发布约束，并接入 initial、DDL 与全阶段 buffer drain。
- T7：把 coordinator eligibility/deadline 变化通知接到 scheduler worker，接入后台 deadline 提交和异步 sticky failure。
- T8：接入 STOPPING、全表 drain、callback-suppressed/global drain、幂等 close 与中断语义。

Checkpoint B：Service 的 callback 模式定向测试证明阈值、pending、scheduler、initial/DDL 和 close 合同；Connector 仍未声明 capability，生产路径尚未激活。

### Phase C：方案 B 激活和跨层正确性

- T9：一次性激活方案 B capability、callback 注入和 Heartbeat 转发。
- T10：完成两表/两 sourceLane 跨 Service、真实 Paimon commit、scheduler、Connector Heartbeat 的集成测试，并运行非目标回归。

Checkpoint C：所有并发测试由 latch/barrier 固定交错；任一失败都保留首因、停止后续 callback 且不吞 close 错误。

### Phase D：锁定引擎验收与收尾

- T11：在不修改引擎的环境中执行方案 B 的纯 INITIAL_SYNC 和 INITIAL_SYNC_CDC 重启矩阵。
- T12：执行模块全量测试、function-context diff、配置/文档核验并生成最终验证报告。

Checkpoint D：Spec 第 18 节 15 项验收全部有证据；外部运行环境缺失或私有依赖未解析时，任务保持未完成，不得把 blocker 表述为通过。

## 7. 并行与串行边界

- 可并行：T1、T2、T4；三者不修改同一生产文件。T3 依赖 T2 已固定的 deadline 查询合同，因此在 T2 后执行。
- 必须串行：T5～T9 都会接触 `PaimonService.java` 或共享状态合同，按依赖顺序执行。
- 可并行准备但不得提前判定通过：T10 的测试数据/fixture 与 T8/T9；实际运行必须等待生产接线完成。
- 必须最后执行：T11 锁定引擎验收和 T12 全量回归。

## 8. 代码风格与实现约束

- Java 8；不引入新依赖，不修改公共 PDK API。
- 新协调类和测试使用 4 空格；修改 `PaimonService` 时保持现有文件的 tab 风格，禁止整文件格式化。
- 共享状态转换必须由同一锁或明确的 atomic/volatile 保护；不得用 `ConcurrentHashMap` 替代跨字段原子性。
- 同表 I/O 保持以下现有锁形态，外部 callback 必须在锁外：

```java
Object lock = commitLocks.computeIfAbsent(tableKey, ignored -> new Object());
synchronized (lock) {
    // validate -> retry pending -> write -> publish state -> decide -> commit
}
// callback runs after releasing the table/coordinator locks
```

- 不修改 `PaimonTableWriteContext`、`PaimonCommitStateStore` 的语义或持久化格式；若现有接口无法满足计划，停止实现并先修订 Spec/Plan。
- 不在 batch/stream read、schema discovery、Catalog options/storage/CatalogFactory、writer/router、bucket strategy 或非微批 capability 方法中产生行为改动；`PaimonService.init()` 只允许生命周期成功发布、scheduler adapter 初始化和初始化失败清理 hunk。

## 9. 验证命令

每个任务执行其定向测试；每个 Checkpoint 至少执行：

```bash
mvn -pl connectors/paimon-connector -DskipTests compile
mvn -pl connectors/paimon-connector test
jq empty connectors/paimon-connector/src/main/resources/spec.json
git diff --function-context -- connectors/paimon-connector/src/main
git diff --check
```

关键定向命令：

```bash
mvn -pl connectors/paimon-connector \
  -Dtest=PaimonConfigTest,PaimonSpecTest test

mvn -pl connectors/paimon-connector \
  -Dtest=PaimonMicroBatchCoordinatorTest,PaimonOffsetBarrierCoordinatorTest test

mvn -pl connectors/paimon-connector \
  -Dtest=PaimonAsyncCommitSchedulerTest,PaimonServiceLifecycleTest test

mvn -pl connectors/paimon-connector \
  -Dtest=PaimonMicroBatchCommitTest,PaimonServiceInitialSyncPendingTest,PaimonServiceTableDdlCacheInvalidationTest test

mvn -pl connectors/paimon-connector \
  -Dtest=PaimonConnectorCallbackTest,PaimonConnectorStopTest,PaimonMicroBatchOffsetIntegrationTest test
```

当前环境已知 Maven 依赖解析 blocker：`tapdata-pdk-runner:2.5-SNAPSHOT`、`tapdata-pdk-api:2.0.8-SNAPSHOT`、`sql-core:1.0-SNAPSHOT`、`pdk-error-code:2.0-SNAPSHOT`。实施前必须先尝试解析；若仍缺失，记录完整输出并继续静态检查，但 Checkpoint 的 compile/test 不得勾选为通过。

## 10. 风险与缓解

| 风险 | 影响 | 确定缓解 |
| --- | --- | --- |
| 全局 capability 同时影响纯 INITIAL_SYNC | 完成前重启可能扩大重放并产生重复/snapshot | 按 `DEC-02` 明示为唯一非回归例外；不伪造 initial offset；T11 执行三时点、三重启类型的逐事件验收。 |
| callback 没有 durability ACK | Paimon 成功后 offset 保存失败会重放 | 坚持 data-before-offset，只声明 at-least-once；callback 失败后 sticky fence。 |
| 多表按 `HashMap` 分组、表序不稳定 | 旧表序队列可能提前推进 offset | 以 sourceLane + required generation 屏障取代表顺序推断。 |
| commit 结果不明 | 旧实现会重写 source batch；资源重建后计数从零重新累计，但数据仍可能重复写入/提交 | 复用同一 pending messages/identifier，追加确认最多 3 次，不重进 writer；空 pending no-op 不发布成功状态。 |
| initial 不计 CDC count | DDL/stop 可能漏掉 initial writer buffer | 独立 `bufferedRecordCount`；DDL/stop 只用全阶段 buffer/pending 决定 drain。 |
| scheduler/write/DDL/close 竞态 | 重复 snapshot、资源早关、offset 越界 | 表锁 + coordinator + lifecycle gate + callback 执行锁；使用确定性 latch 测试。 |
| Service 初始化失败或非目标操作提前启动 worker | 半初始化实例被视为 RUNNING、资源泄漏或源端/元数据路径增加线程 | 构造后保持 NEW；全部初始化成功后发布 RUNNING；worker 只在出现 scheduler-eligible 的未提交 CDC 状态时惰性启动。 |
| close 被中断 | 提前关闭底层资源或吞错误 | 清除中断后完成不可中断清理，末尾恢复 interrupt 并抛聚合异常。 |
| 私有依赖或锁定引擎环境不可用 | 无法形成编译/运行时证据 | 明确标记 blocker；不降低验收标准，不把未运行写成通过。 |

## 11. 文件预算

预计新增生产文件 3 个：

- `PaimonMicroBatchCoordinator.java`
- `PaimonAsyncCommitScheduler.java`
- `PaimonServiceLifecycle.java`

预计修改生产文件 4 个：

- `PaimonConfig.java`
- `PaimonService.java`
- `PaimonConnector.java`
- `spec.json`

测试按职责新增/扩展，详见 [tasks/todo.md](todo.md)。任何单个任务不得修改超过 5 个文件；若实现证明需要第 4 个生产类、修改 `PaimonTableWriteContext` 语义或跨出 Connector 模块，立即停止并修订 Spec/Plan。

## 12. 实施门禁

本计划没有未决产品问题。进入实现前仍需用户明确批准本 Plan 和 [tasks/todo.md](todo.md)。批准后按任务顺序执行；每个任务先写/扩展测试，再实现最小生产改动，Checkpoint 未通过不得进入下一阶段。
