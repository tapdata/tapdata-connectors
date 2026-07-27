# Task List：Paimon Connector 微批提交与多表 at-least-once

> 状态：Draft，待用户批准。任务依赖按编号执行；本文件不表示生产代码已经修改。

## Task 1：规范化微批配置合同

**描述：** 统一 Java 有效默认值、字段注释、JSON 默认值和三种语言文案；显式 `null` 与缺失配置都回落到 `100000/30000/true`。本任务不恢复 capability，不改变当前生产 offset 分支。

**Acceptance criteria：**

- [ ] `PaimonConfig` 三个 getter 对缺失/显式 `null` 返回 `100000/30000/true`，显式 `<=0/false` 保持原值。
- [ ] `PaimonConfig` 的 batch accumulation 注释显示默认 `100000 records`，不得继续保留 `10000`。
- [ ] `spec.json` 三种语言的 batch placeholder 均显示 `100000`，JSON 默认值保持 `100000/30000/true`。
- [ ] 配置与现有非微批属性的测试无回归。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonConfigTest,PaimonSpecTest test`
- [ ] `jq empty connectors/paimon-connector/src/main/resources/spec.json`

**Dependencies：** 无。

**Files likely touched（4）：**

- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/config/PaimonConfig.java`
- `connectors/paimon-connector/src/main/resources/spec.json`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/PaimonConfigTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/PaimonSpecTest.java`

**Estimated scope：** M（4 files）。

## Task 2：建立表状态与多 sourceLane offset 协调器

**描述：** 新增不执行 Paimon I/O 的 `PaimonMicroBatchCoordinator`，固定双计数、generation、Heartbeat pending/inFlight 和 callback token 合同。

**Acceptance criteria：**

- [ ] initial 只增加 `bufferedRecordCount`；CDC 同时增加 buffer/CDC count 并发布 accepted generation/sourceLane 依赖。
- [ ] Heartbeat 只在所有 required generation committed 后 ready；`requiredGenerationByTable` 只包含实际接收过该 sourceLane 数据的表，不为其他表填 generation `0`；表调用顺序不影响结果；不同 lane 的 reservation 独立且每 lane 至多一个 inFlight，coordinator 不调用或串行化外部 Consumer。
- [ ] H1/H2 替换、reservation、token 清理和 callback failure 均符合 Spec 第 6、9 节。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonMicroBatchCoordinatorTest,PaimonOffsetBarrierCoordinatorTest test`
- [ ] 并发场景只使用 latch/barrier，不使用概率性等待。

**Dependencies：** 无；可与 Task 4 并行。Task 3 必须等待本任务固定 deadline 查询合同后开始。

**Files likely touched（3）：**

- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonMicroBatchCoordinator.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonMicroBatchCoordinatorTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonOffsetBarrierCoordinatorTest.java`

**Estimated scope：** M（3 files）。

## Task 3：建立最近 deadline scheduler

**描述：** 新增单线程、one-shot、可重排的 `PaimonAsyncCommitScheduler`；初始化只建立无工作线程的 adapter，首次出现 scheduler-eligible 的未提交 CDC 状态后才惰性启动唯一 daemon worker；它只计算/触发表级 flush action，不拥有提交状态。

**Acceptance criteria：**

- [ ] 配置不满足或尚无 scheduler-eligible 未提交 CDC 状态时不创建 worker；首次出现后幂等启动唯一 daemon worker，并发首批 CDC 也不能创建第二个 worker；首批已在调用内提交且无 pending 时不启动。
- [ ] 新表、commit 或 DDL 改变 deadline 后重排最近任务；到期时重新校验，不增加旧固定相位的额外 interval。
- [ ] fake clock/可控 executor 能确定验证空闲、取消、重排和异常终止，不真实等待 30 秒。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonAsyncCommitSchedulerTest test`

**Dependencies：** Task 2；可与 Task 4 并行。

**Files likely touched（2）：**

- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonAsyncCommitScheduler.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonAsyncCommitSchedulerTest.java`

**Estimated scope：** S（2 files）。

## Task 4：建立 Service 生命周期与 Consumer-start gate

**描述：** 新增 `PaimonServiceLifecycle`，统一初始化发布、ingress、状态迁移、STOPPING/FAILED/CLOSED、Consumer-start 线性化和中断等待。

**Acceptance criteria：**

- [ ] 构造后保持 NEW；只有初始化全部成功才进入 RUNNING；初始化失败保存首因并经 FAILED/CLOSED 完成清理，新入口只在 RUNNING 登记。
- [ ] close 等待已开始 ingress/callback，未开始 Consumer 的 reservation 不越过 STOPPING。
- [ ] 中断等待会完成清理、恢复 interrupt；重复 close 使用同一保存结果且不重复 I/O。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonServiceLifecycleTest test`

**Dependencies：** 无；可与 Task 2 并行。

**Files likely touched（2）：**

- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonServiceLifecycle.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonServiceLifecycleTest.java`

**Estimated scope：** S（2 files）。

## Checkpoint A：基础状态组件

- [ ] Tasks 1～4 的定向测试通过。
- [ ] `mvn -pl connectors/paimon-connector -DskipTests compile`
- [ ] `spec.json` 尚未声明 `flush_offset_callback`；当前生产 offset 行为没有提前激活。
- [ ] 共享状态均有锁/atomic/volatile 说明，无 `Thread.sleep(30000)`。

## Task 5：接入 CDC 跨调用累计与统一 commit helper

**描述：** 在 `PaimonService` 中复用唯一 `PaimonTableWriteContext`，接入 coordinator 的双计数、generation、size/call-time 决策、初始化成功发布和表锁内 commit helper。本任务删除固定 false 旁路，但保留仅用于分阶段交付的 null-callback 同步保护：测试直接注入 callback 验证微批，生产 Connector 尚不注入，因而激活前仍保持当前同步路径；Task 9 必须删除该临时保护。

**Acceptance criteria：**

- [ ] 同表三次小 CDC batch 跨调用累计，阈值前不 commit、达到阈值只 commit 一次；`batchAccumulationSize<=0` 每个非空 CDC batch 提交；两表分别计数；CDC-only/INITIAL_SYNC_CDC 使用各自时间基准，空 batch 无副作用。
- [ ] 非空 batch 在写入前完成同 stage、sourceLane 和 offset metadata 校验；commit 成功才发布 generation/清双计数/更新时间；callback 在全部内部锁外执行。
- [ ] `ASYNC_OFFSET_CONTRACT_VERIFIED` 已删除而不是改为 `true`；Service 在配置、Catalog 和 scheduler adapter 初始化成功后才从 NEW 进入 RUNNING，初始化失败清理并传播首因；Connector 未注入 callback 时仍走临时同步保护，不在生产中提前缓存未提交 CDC。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonMicroBatchCommitTest,PaimonOffsetBarrierCoordinatorTest,PaimonServiceLifecycleTest test`
- [ ] 测试断言阈值前后真实或 mock commit 次数，不只检查字段。

**Dependencies：** Tasks 1、2、3、4。

**Files likely touched（5）：**

- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonMicroBatchCoordinator.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonMicroBatchCommitTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonOffsetBarrierCoordinatorTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonServiceLifecycleTest.java`

**Estimated scope：** M（5 files）。

## Task 6：统一 pending 确认并补齐 initial/DDL drain

**描述：** 把现有 pending commit 语义抽成所有 commit 入口共用的表锁内协议；使用 `bufferedRecordCount` 保证 initial 未执行 `afterInitialSync` 时 DDL 仍先提交数据。

**Acceptance criteria：**

- [ ] 原失败调用后最多 3 次 `retryPendingCommit()`，复用同 messages/identifier/target generation；当前 source batch 不重写；生产 delay 保持 1000ms 并以可注入策略测试中断。
- [ ] `retryPendingCommit()` 在 pending 为空时保持现有 no-op：精确返回 `nextCommitIdentifier-1`，不调用 committer、不推进 identifier、不保存状态；Service 不得据此发布 generation、清计数、更新时间或 callback。
- [ ] initial 不增加 CDC count/generation，但 `afterInitialSync`、DDL 和强制 flush 能看到全部阶段 buffer；DDL 只 drain 目标表且 initial buffer 先进入 snapshot。
- [ ] drain/pending/DDL action 失败保留首因且 action 不越过失败；DDL event 自身不 callback，等待后续有效 CDC Heartbeat。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonServiceInitialSyncPendingTest,PaimonServiceTableDdlCacheInvalidationTest,PaimonTableWriteContextTest test`

**Dependencies：** Task 5。

**Files likely touched（5）：**

- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonMicroBatchCoordinator.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonServiceInitialSyncPendingTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonServiceTableDdlCacheInvalidationTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextTest.java`

**Estimated scope：** M（5 files）。

## Task 7：接入后台时间提交和异步失败传播

**描述：** 将 coordinator 的 scheduler-eligibility/deadline 变化通知和 scheduler worker 接到 Service 的统一 commit helper；无后续写入的 CDC 表在自身 deadline 到达后提交，任何异步终止错误形成 sticky failure。

**Acceptance criteria：**

- [ ] 首次出现 scheduler-eligible 未提交 CDC 状态时惰性启动唯一 worker，低流量表无后续 write 时到期提交；首批已同步提交、initial-only 和非目标操作不启动 worker或被 scheduler 提交。
- [ ] write/scheduler、DDL/scheduler 竞态均只产生一次同表 commit；不同表不被全局 Paimon I/O 锁串行化。
- [ ] pending 确认耗尽或 scheduler Throwable 形成首个 sticky failure，停止重调度，并在下一引擎可见入口抛出。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonAsyncCommitSchedulerTest,PaimonMicroBatchCommitTest test`

**Dependencies：** Tasks 3、5、6。

**Files likely touched（5）：**

- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonAsyncCommitScheduler.java`
- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonMicroBatchCoordinator.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonAsyncCommitSchedulerTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonMicroBatchCommitTest.java`

**Estimated scope：** M（5 files）。

## Task 8：完成 stop/close 线程安全和失败聚合

**描述：** 将 lifecycle gate 接入所有入口、scheduler 和 callback；按 Spec 顺序完成 STOPPING、等待、callback-suppressed 全表 drain、全局 callback drain、context close 和资源清理。

**Acceptance criteria：**

- [ ] `writeRecords`、Heartbeat、DDL、`afterInitialSync` 和 scheduler 共用 ingress gate；STOPPING 后拒绝新入口，正常 stop 提交尚未 `afterInitialSync` 的 initial buffer且重启前 snapshot 可见。
- [ ] STOPPING 后不开始普通 callback；全表 drain 任一失败则不执行晚于 STOPPING 的 Consumer-start；已开始 callback 只等待不重复。
- [ ] scheduler 不用 `shutdownNow` 中断 Paimon commit；close 中断、多错误和重复调用符合 Spec 第 12 节，终止 drain/抑制完成后释放 callback 强引用且不再执行 callback。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonConnectorStopTest,PaimonServiceLifecycleTest test`
- [ ] 所有竞态测试用 latch 固定 Consumer-start 与 STOPPING 先后。

**Dependencies：** Tasks 4、6、7。

**Files likely touched（5）：**

- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonServiceLifecycle.java`
- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonAsyncCommitScheduler.java`
- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonMicroBatchCoordinator.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/PaimonConnectorStopTest.java`

**Estimated scope：** M（5 files）。

## Checkpoint B：Service 安全基础闭合

- [ ] Tasks 5～8 的定向测试通过。
- [ ] callback 直连测试证明阈值前 snapshot 不变、阈值后仅新增一次；两表任一未提交时 callback 计数为 0。
- [ ] pending、scheduler、initial/DDL 和 close 的失败测试均形成 sticky failure 且不推进 offset。
- [ ] `spec.json` 仍未声明 `flush_offset_callback`；生产路径尚未提前激活。

## Task 9：激活方案 B capability 与 Connector callback

**描述：** 在 Service 写入、scheduler、DDL/initial 和 close 安全基础全部完成后，一次性恢复静态 capability、注入 callback、转发 Heartbeat，并删除固定 false 旁路、旧表序 offset 队列和中间同步降级。

**Acceptance criteria：**

- [ ] `spec.json` 恰好声明一个 `flush_offset_callback`；其他 capability 不变；Task 5 的 null-callback 同步保护已删除，最终不存在运行时伪动态模式。
- [ ] 旧 `firstOffsetByTable`、`committedOffsetTables`、`offsetCallbackLock`、`getFirstOffsetByTable()`、`commitCallback` 及其全部调用点均已删除，不得通过恢复 setter 赋值复活旧表序推断。
- [ ] `onStart` 创建 Service 后先原样注入 callback、再调用 `init()`；初始化全部成功才进入 RUNNING，失败时清理并传播首因；非目标操作允许 null，目标 offset-bearing CDC 在写入/登记前拒绝 null；callback 在 RUNNING/STOPPING/FAILED 不可替换或清空，仅允许终止清理释放。
- [ ] `processControl` 仅处理 Heartbeat：INITIAL_SYNC no-op；CDC payload 原样保留 streamOffset/sourceTime/nodeIds/stage/nullable eventTime 并进入 generation 屏障。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonConnectorCallbackTest,PaimonSpecTest,PaimonOffsetBarrierCoordinatorTest test`
- [ ] `jq empty connectors/paimon-connector/src/main/resources/spec.json`
- [ ] `rg -n 'ASYNC_OFFSET_CONTRACT_VERIFIED|firstOffsetByTable|committedOffsetTables|offsetCallbackLock|getFirstOffsetByTable|commitCallback' connectors/paimon-connector/src/main` 无旧实现命中。

**Dependencies：** Tasks 2、5、6、7、8。

**Files likely touched（5）：**

- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/PaimonConnector.java`
- `connectors/paimon-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-connector/src/main/resources/spec.json`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/PaimonConnectorCallbackTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/PaimonSpecTest.java`

**Estimated scope：** M（5 files）。

## Task 10：完成两表两 lane 跨层集成与非目标回归

**描述：** 使用本地真实 Paimon 表贯通 DML metadata、Service generation、实际 snapshot、scheduler 和 Connector Heartbeat；随后运行现有 bucket/schema/DDL/commit state/read-side diff 门禁。

**Acceptance criteria：**

- [ ] A/B 两表、S1/S2 两 lane 在 B 未提交时均不 callback；B 到期提交后每 lane 各 callback 一次且 Consumer 不并发。
- [ ] callback payload 五项字段与 Heartbeat 完全一致；A/B snapshot 各只按自身阈值新增。
- [ ] 既有 bucket mode、动态桶、DML image、主键变更、commit state、DDL cache invalidation、日期转换测试不回归。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -Dtest=PaimonMicroBatchOffsetIntegrationTest test`
- [ ] `mvn -pl connectors/paimon-connector test`
- [ ] `git diff --function-context -- connectors/paimon-connector/src/main` 不含 Spec 禁止路径的方法 hunk。

**Dependencies：** Tasks 6～9。

**Files likely touched（3）：**

- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonMicroBatchOffsetIntegrationTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonMicroBatchCommitTest.java`
- `connectors/paimon-connector/src/test/java/io/tapdata/connector/paimon/PaimonConnectorCallbackTest.java`

**Estimated scope：** M（3 files）。

## Checkpoint C：Connector 模块正确性

- [ ] Tasks 9～10 的定向和全量模块测试通过。
- [ ] 任一失败用例同时断言首因、后续 fence、offset 不推进和 close 不吞错。
- [ ] 默认小 batch snapshot 数显著少于 `writeRecords` 调用数。
- [ ] 生产代码 diff 仅位于 Plan 文件预算内。

## Task 11：执行锁定引擎的方案 B 重启矩阵

**描述：** 使用引擎提交 `f91bfe4...` 与待验 Connector 构造真实纯 INITIAL_SYNC/INITIAL_SYNC_CDC 任务；不修改引擎、不伪造 initial offset。

**Acceptance criteria：**

- [ ] 在“initial writer buffer 未完成”“部分表完成”“全部表完成但任务未标记完成”三个时点，分别验证崩溃、强停、正常手动停后重启。
- [ ] append-only 以稳定事件 ID 证明 `source IDs ⊆ target IDs`，缺失集合为空；记录重复 ID/行、重放起点和额外 snapshot。
- [ ] 主键表最终键集合无缺失；INITIAL_SYNC_CDC 在首个有效 CDC Heartbeat callback 前后分别重启并记录实际恢复点。

**Verification：**

- [ ] 运行时报告包含 3×3 重启矩阵、源/目标事件集合、callback/snapshot 时间线和完整环境 revision。
- [ ] 正常手动停止在重启前证明当时完整接收的 writer buffer 已进入 snapshot。

**Dependencies：** Checkpoint C；需要可运行的锁定引擎环境。

**Files likely touched（1）：**

- `connectors/paimon-connector/docs/paimon-micro-batch-validation-report.md`

**Estimated scope：** M（1 report + external runtime）。

## Task 12：全量收尾、diff 审计与发布说明

**描述：** 汇总自动化和运行时证据，逐条映射 Spec 第 18 节，更新状态文档；不得用 exactly-once 或“与旧实现完全一致”表述。

**Acceptance criteria：**

- [ ] Spec 第 18 节 15 项均有命令输出或运行时证据；未运行项保持未完成。
- [ ] 发布说明明确静态 capability 影响所有同步类型，以及纯 INITIAL_SYNC 完成前重启可能扩大重放/重复；结构化日志覆盖 commit/Heartbeat/scheduler/close 且不输出 offset payload、密钥或记录内容。
- [ ] 每个生产 hunk 映射到 Spec 条款；无跨模块修改、依赖升级、公共 API 变化或无关重构。

**Verification：**

- [ ] `mvn -pl connectors/paimon-connector -DskipTests compile`
- [ ] `mvn -pl connectors/paimon-connector test`
- [ ] `jq empty connectors/paimon-connector/src/main/resources/spec.json`
- [ ] `git diff --function-context -- connectors/paimon-connector/src/main`
- [ ] `git diff --check`

**Dependencies：** Task 11。

**Files likely touched（4）：**

- `connectors/paimon-connector/docs/paimon-micro-batch-commit-at-least-once-spec.md`
- `connectors/paimon-connector/docs/paimon-micro-batch-validation-report.md`
- `tasks/plan.md`
- `tasks/todo.md`

**Estimated scope：** M（4 files）。

## Checkpoint D：完成门禁

- [ ] 所有 Task 和 Checkpoint 已完成且有证据。
- [ ] 模块 compile/test 真正执行通过；若私有依赖仍缺失，本 Checkpoint 不得通过。
- [ ] 锁定引擎运行时矩阵通过，不丢数据；允许项仅为 `DEC-02` 定义的重放/重复。
- [ ] 用户完成最终代码与验证报告 review。
