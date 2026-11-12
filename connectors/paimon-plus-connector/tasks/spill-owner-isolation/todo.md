# 任务清单：Paimon Spill 进程级命名空间与租约回收协议（Plan v2）

> 规范：[SPEC V0.3](../../src/doc/specs/SPEC-paimon-spill-owner-isolation.md)；计划：[plan.md](plan.md)；证据：[REVIEW](../../src/doc/reviews/REVIEW-paimon-spill-owner-isolation-2026-11-12.md)。\
> 规则：任务 Txx 即本地 PR #xx，按 plan §2 的流程交付。勾选必须附带命令、测试数和合并提交；失败也如实记录。

命令约定（在 worktree 根目录执行，需先 `export JAVA_HOME=<JDK 17>`）：

- `TEST <类>` = `$MVN -o -B -pl connectors/paimon-plus-connector -am -DskipTests=false -Dtest='<类>' -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test`
- `PACKAGE` = `$MVN -o -B -pl connectors/paimon-plus-connector -am -DskipTests=false package`
- 其中 `$MVN` 为 Maven 3.9 的二进制路径（plan §2）。

路径前缀：`main/` = `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/`；`test/` = 对应的 `src/test/java/...`。

---

## 已完成

### T01：导入 V0.2 基线文档 —— 本地 PR #1
- [x] 原样导入 Spec V0.2、plan v1、todo v1，作为 V0.3 修订的 diff 基线
- [x] 分支 `pr/spill-owner-isolation/01-baseline-docs`，合并提交 `2e2f66dd`，只含文档

### T02：平台特征化测试与现场复现 —— 本地 PR #2
- [x] 新增 `PaimonSpillPlatformContractTest`（C1–C10）、`PaimonSpillFieldFailureReproductionTest`（R1–R3）、`SpillProbeProcess`、`IsolatedConnectorLoader`；生产代码零改动
- [x] `TEST PaimonSpillPlatformContractTest,PaimonSpillFieldFailureReproductionTest`：13 项，0 失败、0 跳过（macOS / JDK 17.0.20 / apfs）
- [x] 变异检验：`OwnerLock.tryAcquire` 冲突后不关闭 channel 时，R1、R2 失败，R3 通过；已还原
- [x] 分支 `pr/spill-owner-isolation/02-platform-contract-tests`，合并提交 `213f9c49`

### T03：Spec V0.3、Plan v2、证据文档 —— 本地 PR #3
- [x] Spec V0.3 吸收评审问题 1–7 及 Consider/Nit 项（处置表见证据 §7），并补充状态机、安全性/活性论证、平台前提与交付流程
- [x] 新增证据文档：现场栈逐帧核实、Paimon 1.3.2 与 Paimon Flink Connector 源码核实、取证程序
- [x] 全量基线（集成分支 `213f9c49`）：`PACKAGE` 成功，paimon-plus-connector 1255 项，0 失败、0 错误、1 跳过（`PaimonRustFsIntegrationTest`，依赖外部服务），耗时 129 s
- [x] 分支 `pr/spill-owner-isolation/03-spec-plan-v0.3`，合并提交 `c5600c59`，只含文档

### T03.1：生命周期图与内核证据清单 —— 本地 PR #3.1
- [x] 新增 `SPILL-所有权隔离-修改前后对比与生命周期.md`：内核 spill 调用链、目录布局、修改前后完整生命周期、故障与修复后时序、租约状态机、逐项对比，以及 Paimon 内核、Paimon Flink Connector、Flink Runtime、JDK 证据清单
- [x] 7 张 Mermaid 图经 mermaid 11.17.2 `parse` 全部通过；用故意写错的图确认校验器能检出错误
- [x] 分支 `pr/spill-owner-isolation/03.1-lifecycle-diagrams`，合并提交 `e4032180`，只含文档

### T03.2：集成分支更名为 hotfix 并推送 —— 本地 PR #3.2
- [x] `feature/paimon-spill-owner-isolation` 更名为 `hotfix/paimon-spill-owner-isolation`，Spec §9 与 plan §2 中的分支名与远端规则同步更新
- [x] 推送到 fork 远端 `czy006`（与既有 `hotfix/paimon-*` 分支约定一致），不推送 `pr/*` 主题分支，不推送 `origin`；合并提交 `bb245dca`

---

## T04：PaimonJvmIdentity —— 本地 PR #4

**Description**：按 Spec §4.2 实现 jvmId：反射调用 `RuntimeMXBean#getPid`，Java 8 退回 `getName()` 中 `@` 之前的部分，再与 `getStartTime()` 一起做 SHA-256，取前 32 位十六进制并加 `j` 前缀；每个 ClassLoader 惰性计算一次并缓存。提供包内可见的纯函数 `derive(pid, startTime)` 与可注入的 provider。

**Acceptance criteria**：
- [x] 固定期望值：`derive(12345, 1700000000000L)` 等于测试中写死的常量；格式符合正则 `j[0-9a-f]{32}`
- [x] 两个隔离 ClassLoader 结果相同；子进程结果不同；子进程在 `jdk.net.hosts.file` 切换前后计算结果不变（AC-01）
- [x] pid 获取失败或 `getName()` 格式不符时抛出带原因的 `IllegalStateException`；生产代码不出现主机名相关调用（AC-08 的身份部分）
- [x] 证据：`TEST PaimonJvmIdentityTest,PaimonSpillPlatformContractTest` 18 项通过；变异（把 `getName().hashCode()` 混入派生输入）被 `identityIgnoresHostNameResolution` 与 `currentIsDerivedFromProcessIdAndStartTimeAndStable` 检出，已还原

**Verification**：`TEST PaimonJvmIdentityTest`；变异检验：把主机名混入派生输入，hosts 切换用例应失败。\
**Dependencies**：T03\
**Files**：`main/util/PaimonJvmIdentity.java`（新）、`test/util/PaimonJvmIdentityTest.java`（新）、`test/util/SpillProbeProcess.java`（新增 `jvm-id` 模式）\
**Scope**：S

## T05：PaimonSpillAudit —— 本地 PR #5

**Description**：实现 Spec §4.7。SLF4J logger 名为 `io.tapdata.connector.paimon.spill`；三类事件（跳过、准入拒绝、旧布局清单）按（根目录, 原因）限流；sink 可注入；主机名只读环境变量 `HOSTNAME`。

**Acceptance criteria**：
- [x] 事件字段齐全，不含连接配置；限流窗口内同一（根目录, 原因）只输出一次（AC-23 单元部分）
- [x] sink 抛出异常时由审计层吞掉并降级，不传播（AC-18 审计部分）
- [x] 代码中没有 `InetAddress` 或 `getName()` 调用
- [x] 证据：`TEST PaimonSpillAuditTest` 5 项通过；变异（取消限流判断）被 `rateLimitedEventsAreEmittedOncePerKeyAndWindow` 检出，已还原；PR #4 合并提交 `1fdfd5a2`

**Verification**：`TEST PaimonSpillAuditTest`；变异检验：去掉限流判断，限流用例应失败。\
**Dependencies**：T03（可与 T04 并行）\
**Files**：`main/util/PaimonSpillAudit.java`（新）、`test/util/PaimonSpillAuditTest.java`（新）\
**Scope**：S

### 检查点 A（T04–T05 之后）
- [x] 新增测试全绿，`TEST PaimonSpillDirCleanerTest` 仍为 18 项全绿（生产行为未变）：6 个类 47 项通过

---

## T06：PaimonSpillLease（owner 协议）—— 本地 PR #6

**Description**：按 Spec §4.4 实现 Provision 与 Retire：

- 根目录规范化并去重；
- 子根与命名空间不得是符号链接；
- `mkdir(<lease>)`、`O_EXCL` 创建锁文件并 tryLock、`mkdir(<io>)`；第 1–3 步遇 ENOENT 时换新 leaseId 重试，最多 3 次；
- 多根部分失败时回滚已就绪的根；
- Retire 顺序为删除 `<io>` → 持锁 unlink → 释放锁 → rmdir，最后一步遇 ENOENT 视为完成。

提供 Provision 阶段钩子（“已加锁、未返回”）供 T11 使用。

**Acceptance criteria**：
- [x] 布局与命名符合 Spec §4.1；重复或互为别名的根被去重（AC-20）
- [x] 用钩子在 `mkdir(<lease>)` 之后删除空 lease 或空命名空间，Provision 重试后成功；Retire 前在锁释放后外部删除空 lease，结果为完成（AC-17 单进程部分）
- [x] `<io>` 删除失败时保留锁并返回不完整；多根部分失败时不残留锁（AC-18 owner 部分）
- [x] 证据：`TEST PaimonSpillLeaseTest` 12 项通过，旧清理器回归（PaimonSpillDirCleanerTest 18、进程集成 3、现场复现 3）不变；变异 (a) Retire 末步 ENOENT 视为失败、(b) Provision 遇 ENOENT 不重试，均被检出并还原；PR #5 合并提交 `4bc8fe9e`

**Verification**：`TEST PaimonSpillLeaseTest`；变异检验：把 Retire 最后一步对 ENOENT 的容忍改为失败，竞态用例应失败。\
**Dependencies**：T04、T05\
**Files**：`main/util/PaimonSpillLease.java`（新）、`test/util/PaimonSpillLeaseTest.java`（新）\
**Scope**：M

## T07：测试抽象层（旧布局上全绿）—— 本地 PR #7

**Description**：新增 `SpillTestSupport`，按**当前平铺布局**实现：`leaseIOManager(root)`、`ownerLockOf(spillDir)`、`ownerLockHeld(spillDir)`（以子进程 tryLock 判定）、`ownerIndexContains(spillDir)`。把下列测试中的 `registerLiveDirs` 调用、旁置锁文件路径断言和对 `OWNER_LOCKS` 的反射，替换为对该工具类的调用；断言的语义保持不变。

不修改生产代码。本 PR 修改 9 个文件，超过 5 个文件的上限；理由是所有改动都是同一种访问方式的替换，需要逐行确认的只有替换本身。

**Acceptance criteria**：
- [x] 测试源码中只有 `SpillTestSupport` 直接引用 `registerLiveDirs`、`OWNER_LOCKS` 与 `.tapdata-owner.lock`（清理器自身的 4 个测试与子进程探针除外，按计划在 T08/T10 改写）
- [x] 断言数量与语义不变（以 diff 复核）；`PACKAGE` 全绿：1280 项（T03 基线 1255 + PR #4–#6 新增 25），0 失败，1 跳过（既有）
- [x] 生产代码 diff 为空
- [x] 补充：`BlockingSpillIOManager.owned` 与 Factory 测试 Fixture 的根目录参数；Factory/Preflight 测试改为截获生产代码实际调用的 `IOManager.create(roots)`；PR #6 合并提交 `ca5457b7`

**Verification**：`TEST` 下列 8 个类；`PACKAGE`。\
**Dependencies**：T06（与生产代码无关，可以提前；放在 T08 之前即可）\
**Files**：
- `test/util/SpillTestSupport.java`（新）
- `test/write/PaimonFinalCompactionIntegrationTest.java`
- `test/write/PaimonCompactionSpillLifecycleIntegrationTest.java`
- `test/write/PaimonFinalPrepareBucketModeIntegrationTest.java`
- `test/write/PaimonNativeStopContractTest.java`
- `test/write/PaimonTableWriteContextFactoryTest.java`
- `test/service/PaimonServiceSyncStopIntegrationTest.java`
- `test/service/PaimonServiceNativeMaintenanceIntegrationTest.java`
- `test/service/PaimonDynamicBucketPreflightCleanupTest.java`

**Scope**：M（机械替换）

## T08：owner 侧翻转，R1–R3 改为回归 —— 本地 PR #8

**Description**：

- 外观 `resolveAndCreateIOManager(configuredTmpDirs, ownerLabel, scope)` 改为走 lease，并登记 owner 索引；`releaseAfterClose` 改为经索引执行 Retire。
- 释放非空且未登记的路径时，返回不完整（D5）。
- IOManager 构造后若某根未物化，保留资源（D6）。
- Factory 与 Preflight 传入 owner 标签。
- `SpillTestSupport` 改为 v2 布局实现。
- 删除 `registerLiveDirs`、`registerCreatedIOManager`、`registerPaths` 与 `OWNER_LOCKS` 的注册路径。
- 把 `PaimonSpillFieldFailureReproductionTest` 改名为 `PaimonSpillOwnerIsolationRegressionTest`：步骤不变（loader B 与子进程 C 仍运行**旧平铺清理器**），结论反转。

**Acceptance criteria**：
- [x] 写上下文与预检只产生 v2 布局；`closeForStop` 成功后不留 lease（且空的命名空间与子根一并移除）
- [x] AC-03（旧清理器版本）：C 删除数为 0，A 建 channel 成功，外部 tryLock 失败；AC-21；jvmId 失败时 Context 初始化失败且不产生 lease（AC-08，由 PaimonSpillLeaseTest 覆盖，外观只传播异常）
- [x] `PACKAGE` 全绿：1298 项，0 失败，1 跳过（既有）。相对 T07 的 1280：+14 回收器（PR #9）、+2 清理器（删 2 个旧 owner 用例，增 4 个）、+2 lease
- [x] 首次全量有 3 个类失败，均为测试依赖旧布局或旧回退：PhysicalTableOwnerTest 的 Fixture 虚构 spill 路径（D5 后改为空列表）；ContextIntegrationTest 按平铺布局查找（等价改写）；DynamicBucketIntegrationTest 断言停止后根目录为空（改为 owner 退役时清理空命名空间与子根，并把 `createDirectories` 纳入 ENOENT 重试；Spec §4.3/§4.4 同步）
- [x] 变异：(a) Retire 跳过 unlink → 9 项失败；(b) 未登记路径静默成功 → 1 项失败；(c) 不清理空命名空间 → 1 项失败；均已还原。PR #9 合并提交 `80b90fd5`

**Verification**：`TEST PaimonSpillDirCleanerTest,PaimonTableWriteContextFactoryTest,PaimonDynamicBucketPreflightCleanupTest,PaimonSpillOwnerIsolationRegressionTest`；`PACKAGE`；变异检验：Retire 跳过 unlink，相关用例应失败。\
**Dependencies**：T06、T07\
**Files**：
- `main/util/PaimonSpillDirCleaner.java`
- `main/write/PaimonTableWriteContextFactory.java`
- `main/service/PaimonDynamicBucketPreflight.java`
- `test/util/SpillTestSupport.java`
- `test/util/PaimonSpillOwnerIsolationRegressionTest.java`（由 R1–R3 改写）
- `test/util/PaimonSpillDirCleanerTest.java`
- `test/util/PaimonSpillDirCleanerProcessIntegrationTest.java`

**Scope**：M

### 检查点 B（T06–T08 之后）
- [x] `PACKAGE` 全绿；v2 布局与“结束后根目录无残留”由 `PaimonTableWriteContextIntegrationTest.keyDynamicRocksDbIndexMustLiveInsideTheSpillDirectory` 与 `PaimonServiceDynamicBucketIntegrationTest` 自动断言，取代人工检查
- [x] AC-03 回归通过：现场缺陷在 owner 侧已不可复现（`PaimonSpillOwnerIsolationRegressionTest` 3 项）

---

## T09：PaimonSpillReclaimer —— 本地 PR #9

**Description**：按 Spec §4.5–4.6 实现回收器：命名空间按名称排除、正则校验、符号链接拒绝、不带 CREATE 打开、fileKey 复核、ENOENT/ENOTEMPTY 容忍、Overlapping 跳过、每次调用预算、文件系统准入、审计在锁外统一写出。

**Acceptance criteria**：
- [x] 本 JVM 命名空间的打开次数为 0，且不列出其内容（AC-02 同 ClassLoader 部分）；jvmId 碰撞时跳过（AC-07 部分）；两个线程并发回收同一 ORPHANED lease 时无异常逃逸、至多一次成功（AC-05 部分）
- [x] AC-08 的输入校验；AC-18 中 `<io>` 删除失败保留锁文件；预算生效
- [x] AC-19：nfs4、fuse.*、未知类型、探针异常时打开次数为 0，告警一次
- [x] 证据：`TEST PaimonSpillReclaimerTest` 14 项通过（含外部进程持锁、mtime 极早仍保留，AC-07；源码不含时间输入的结构检查，INV-03）；变异 (a) 去掉本命名空间排除、(b) 去掉 fileKey 复核，均被检出并还原；本 PR 先于 PR #8 合入（Plan 允许并行），PR #7 合并提交 `57edbcef`

**Verification**：`TEST PaimonSpillReclaimerTest`；人工检查回收器源码中没有 mtime 与时钟调用；变异检验：去掉自身命名空间排除，AC-02 用例应失败。\
**Dependencies**：T06（可与 T07、T08 并行）\
**Files**：`main/util/PaimonSpillReclaimer.java`（新）、`test/util/PaimonSpillReclaimerTest.java`（新）\
**Scope**：M

## T10：Service 切换，删除平铺扫描 —— 本地 PR #10

**Description**：

- `PaimonService` 的初始化清理改为调用 Reclaim（根目录收集逻辑沿用）；对旧平铺目录只输出限流清单。
- 删除 plan §7 中剩余的旧代码。
- 新增 `LegacyFlatSpillCleanerFixture`：从 `e3f68298` 冻结旧清理器的扫描逻辑，并注明来源；`SpillProbeProcess` 的 `legacy-cleanup` 模式改为调用该副本。
- 回归测试中 loader B 的部分改为调用新回收器。
- 原平铺扫描的单元测试按意图迁移到回收器测试，或者删除，并逐项列出。

**Acceptance criteria**：
- [ ] 生产代码中 `grep -E "tapdata-owner\.lock|LIVE_DIRS|newestModified|DEFAULT_STALE_GRACE_MS|cleanupStaleSpillDirs"` 无结果
- [ ] 启动时只回收外部命名空间中的 ORPHANED lease；旧目录与旧锁文件不被打开（AC-22 部分）
- [ ] `PACKAGE` 全绿；被删除或迁移的测试有逐项清单

**Verification**：`TEST PaimonServiceStaleSpillCleanupTest,PaimonServiceSyncStopIntegrationTest,PaimonSpillReclaimerTest,PaimonSpillOwnerIsolationRegressionTest`；`PACKAGE`。\
**Dependencies**：T08、T09\
**Files**：
- `main/service/PaimonService.java`
- `main/util/PaimonSpillDirCleaner.java`
- `test/util/LegacyFlatSpillCleanerFixture.java`（新）
- `test/util/SpillProbeProcess.java`
- `test/service/PaimonServiceStaleSpillCleanupTest.java`

以及按需修改的 `test/util/PaimonSpillDirCleanerTest.java`。\
**Scope**：M

### 检查点 C（T09–T10 之后）
- [ ] `PACKAGE` 全绿；与用户复核删除的代码与测试清单

---

## T11：跨 ClassLoader 验收 —— 本地 PR #11

**Description**：用 `IsolatedConnectorLoader` 与驱动类，在两个 ClassLoader 中执行 Provision、Reclaim 与建 channel。只使用纯 Java 的 IOManager，不加载 RocksDB JNI。

**Acceptance criteria**：
- [ ] AC-02（跨 ClassLoader）与 AC-03（新回收器版本）：loader B 对 A 命名空间的打开次数为 0；随后子进程 C 运行新回收器，删除数为 0；A 建 channel 成功
- [ ] AC-04：钩子停在“已加锁、未返回”时，同 ClassLoader 并发回收的打开次数为 0；AC-05：两个 loader 并发回收同一 ORPHANED lease
- [ ] AC-15：loader A 中保留一个活跃使用者线程，只丢弃 PDK 侧引用并施加 GC 压力，外部 tryLock 仍失败

**Verification**：`TEST PaimonSpillIsolatedClassLoaderIntegrationTest`\
**Dependencies**：T10\
**Files**：`test/util/PaimonSpillIsolatedClassLoaderIntegrationTest.java`（新）、`test/util/SpillLoaderDriver.java`（新）\
**Scope**：M

## T12：多进程验收 —— 本地 PR #12

**Description**：把 `PaimonSpillDirCleanerProcessIntegrationTest` 改写为 `PaimonSpillProcessIntegrationTest`，用 stdin/stdout 握手控制竞态顺序。

**Acceptance criteria**：
- [ ] AC-06：两个回收者进程处理同一 ORPHANED lease，至多一个完成，全程不新建锁文件；AC-07：活跃 lease 的 mtime 改为很早，仍不删除
- [ ] AC-09：owner 子进程被 SIGKILL 后由父进程回收；子进程 1 被杀后由子进程 2 回收
- [ ] AC-16：迟到回收者复核失败，不删除任何内容；AC-17（跨进程）：Provision 与 Retire 的 ENOENT 竞态均得到预期结果

**Verification**：`TEST PaimonSpillProcessIntegrationTest`\
**Dependencies**：T10\
**Files**：`test/util/PaimonSpillProcessIntegrationTest.java`（新）、`test/util/PaimonSpillDirCleanerProcessIntegrationTest.java`（删除）、`test/util/SpillProbeProcess.java`\
**Scope**：M

## T13：生命周期回归 —— 本地 PR #13

**Description**：在 V1.2 的生命周期测试中补充 lease 层面的断言：凡是保留路径，用 `SpillTestSupport.ownerLockHeld` 证明锁仍被持有；凡是完成路径，断言 lease 不存在。

**Acceptance criteria**：
- [ ] AC-10、AC-11：RETAINED 与“已取消但仍在运行”时锁保持，反复回收不删除
- [ ] AC-13：多根部分失败、DDL/rebuild、有限读失败时锁保持
- [ ] AC-14：`rocksdb-*` 位于 `<io>/paimon-io-*` 内；AC-12 全部回归通过

**Verification**：`TEST PaimonCompactionSpillLifecycleIntegrationTest,PaimonBoundedStopTest,PaimonTableWriteContextFactoryTest,PaimonFiniteReadLifecycleTest,PaimonServiceDdlCompactionFailureTest,PaimonDynamicBucketPreflightCleanupTest`\
**Dependencies**：T10\
**Files**：上述 6 个测试类中的 5 个（`PaimonServiceDdlCompactionFailureTest` 只运行、不修改）\
**Scope**：M

## T14：旧布局并存与残留 —— 本地 PR #14

**Acceptance criteria**：
- [ ] AC-22：新回收器不打开旧锁文件、不删除旧目录，并输出清单告警；冻结的旧清理器不进入 `tapdata-paimon-v2/`；回退场景中旧扫描不删除新目录
- [ ] AC-25：多表、多根反复启停后无 lease 残留、无 channel 泄漏；预算生效

**Verification**：`TEST PaimonSpillLegacyCoexistenceTest,PaimonWriteBufferSpillIntegrationTest`\
**Dependencies**：T10\
**Files**：`test/util/PaimonSpillLegacyCoexistenceTest.java`（新）、`test/write/PaimonWriteBufferSpillIntegrationTest.java`\
**Scope**：S

### 检查点 D（T11–T14 之后）
- [ ] 除 AC-24 外，全部 AC 都有通过的测试（plan §6）；`PACKAGE` 全绿

---

## T15：文档与发布说明 —— 本地 PR #15

**Acceptance criteria**：
- [ ] 中、英、繁三种语言的连接器文档内容一致：部署规范（REQ-11）、升级后重启引擎、旧目录一次性清理步骤、网络盘不自动回收
- [ ] 在 Spec 中追加实现记录（源码位置、测试映射、各 PR 的合并提交）；发布说明写明不提供全局 fencing，现场结论以 OQ-04 为准
- [ ] AC-23 人工检查：在真实 Engine 中确认审计日志出现在引擎日志；无法验证时如实标注

**Verification**：人工审阅；`git diff --check`\
**Dependencies**：T11–T14\
**Files**：`src/main/resources/docs/paimon_{zh_CN,en_US,zh_TW}.md`、Spec\
**Scope**：S

## T16：Linux + JDK 17 验收 —— 本地 PR #16

**Acceptance criteria**：
- [ ] 在 OQ-06 确定的环境中，`PaimonSpillPlatformContractTest`、`PaimonJvmIdentityTest`、`PaimonSpillLeaseTest`、`PaimonSpillReclaimerTest`、`PaimonSpillIsolatedClassLoaderIntegrationTest`、`PaimonSpillProcessIntegrationTest` 全部通过（AC-24）
- [ ] 同一环境下 `PACKAGE` 通过；记录 OS、内核、JDK 与 `FileStore.type()` 的实际值（供 OQ-05）
- [ ] 未覆盖的 OS 与文件系统组合写明“不宣称支持”

**Dependencies**：T15、OQ-06\
**Scope**：S

### 检查点 E：完成
- [ ] AC-01–25 全部通过或有书面原因；`git log --first-parent --merges` 可以复查 PR #1–#16
- [ ] 用户复核后，再决定是否推送远端
