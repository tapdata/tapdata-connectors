# 任务清单：Paimon Spill 进程命名空间与死亡 owner 回收

> 状态：Draft，待用户批准 [plan.md](plan.md)；本文件不表示生产代码已修改。  
> 规范：[SPEC V0.2](../../src/doc/specs/SPEC-paimon-spill-owner-isolation.md)。任务按编号与依赖执行；每完成一项在此勾选并记录命令、测试数和结果。

命令约定（在仓库根目录执行）：

- `TEST <类名列表>` 表示 `mvn -B -pl connectors/paimon-plus-connector -am -DskipTests=false -Dtest='<类名列表>' -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test`
- `PACKAGE` 表示 `mvn -B -pl connectors/paimon-plus-connector -am -DskipTests=false package`

路径前缀：`main/` = `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/`，`test/` = `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/`。

---

## Task 0：基线与验收环境

**Description：** 在未改动的 develop 上跑一遍定向测试和全量 package，记录基线；确认 AC-21 使用的 Linux + JDK 17 环境（P-OQ-1）。

**Acceptance criteria：**
- [ ] 记录 `java -version`、`mvn -version` 与基线提交
- [ ] 记录 Spec §11 两组定向测试和 `PACKAGE` 的通过数、失败数；已有失败如实列出，不归因于本次改动
- [ ] P-OQ-1 有结论（环境与执行方式）

**Verification：**
- [ ] `TEST PaimonSpillDirCleanerTest,PaimonSpillDirCleanerProcessIntegrationTest,PaimonServiceStaleSpillCleanupTest`
- [ ] `TEST PaimonBoundedStopTest,PaimonStopControllerTest,PaimonNativeStopContractTest,PaimonCompactionSpillLifecycleIntegrationTest,PaimonTableWriteContextFactoryTest,PaimonFiniteReadLifecycleTest,PaimonConnectorStopTest`
- [ ] `PACKAGE`

**Dependencies：** None  
**Files likely touched：** 本文件  
**Estimated scope：** XS

---

## Task 1：PaimonJvmIdentity 与隔离 ClassLoader 测试工具

**Description：** 新增只读 jvmId 计算（AD-4），并提供一个测试工具：用 platform ClassLoader 作父加载器，从测试 classpath 隔离加载 Connector 类。该工具在 T08 复用。

**Acceptance criteria：**
- [ ] `current()` 返回 `j` + 32 位小写十六进制；只调用 `ManagementFactory.getRuntimeMXBean()` 的 `getName()` / `getStartTime()`，不调用 `getPlatformMBeanServer`
- [ ] 两个隔离 ClassLoader 中取得的值相同；子 JVM 取得的值不同（AC-01）
- [ ] 获取失败时抛出带原因的 `IllegalStateException`，不回退（为 AC-08 预留包内可见的提供器重载）

**Verification：**
- [ ] `TEST PaimonJvmIdentityTest`
- [ ] 生产代码以 `<release>8` 编译通过

**Dependencies：** T00  
**Files likely touched：**
- `main/util/PaimonJvmIdentity.java`（新）
- `test/util/PaimonJvmIdentityTest.java`（新）
- `test/util/IsolatedConnectorLoader.java`（新，测试工具）

**Estimated scope：** S

---

## Task 2：PaimonSpillAudit

**Description：** 统一审计出口：SLF4J logger `io.tapdata.connector.paimon.spill`，事件包括创建、退役、保留、回收开始/完成/失败、跳过、文件系统门禁、旧布局清单。跳过、门禁、旧布局三类按（根目录, 原因）限流。提供包内可见的 sink 接缝供测试捕获。

**Acceptance criteria：**
- [ ] 事件字段包含 REQ-10 所列：协议版本、pid、hostname、jvmId 摘要、leaseId、tableTag、ClassLoader 标识、根目录、文件系统类型、动作来源、结果、异常
- [ ] 不输出连接配置；同一（根目录, 原因）的告警在限流窗口内只输出一次
- [ ] sink 抛出异常时被吞掉并降级，不向调用方传播（AC-17 的一部分）

**Verification：**
- [ ] `TEST PaimonSpillAuditTest`

**Dependencies：** T00  
**Files likely touched：**
- `main/util/PaimonSpillAudit.java`（新）
- `test/util/PaimonSpillAuditTest.java`（新）

**Estimated scope：** S

### Checkpoint A（T01–T02 之后）
- [ ] 模块编译通过，T01/T02 新测试通过
- [ ] 生产行为未变（`PaimonSpillDirCleanerTest` 仍全绿）

---

## Task 3：PaimonSpillLease（owner 侧）

**Description：** 按 plan §3.1 实现多根 lease 的创建、创建期回滚与关闭后退役。包内可见接缝：jvmId 提供器、创建阶段钩子（“已加锁、未返回”），供 T08 的 AC-04 使用。

**Acceptance criteria：**
- [ ] 布局符合 REQ-01；`owner.lock` 用 `CREATE_NEW` 创建；子根或命名空间目录是符号链接时失败；父目录消失时换 UUID 有界重试
- [ ] 多根中任一根失败：已建成的根立即退役并删除，不留锁；成功时每个根各持一把锁
- [ ] 退役顺序为删 io → unlink 锁文件 → 释放锁 → rmdir lease；io 删除失败时保留锁并返回失败（AC-17 owner 部分）

**Verification：**
- [ ] `TEST PaimonSpillLeaseTest`（顺序用钩子断言，不用 sleep）

**Dependencies：** T01、T02  
**Files likely touched：**
- `main/util/PaimonSpillLease.java`（新）
- `test/util/PaimonSpillLeaseTest.java`（新）

**Estimated scope：** S

---

## Task 4：外观接入 lease，Factory 与 Preflight 传 owner 标签

**Description：** `PaimonSpillDirCleaner.resolveAndCreateIOManager(configuredTmpDirs, ownerLabel, scope)` 改为：

1. 创建 lease；
2. 以各根的 `io/` 为根调用 `IOManager.create`，并物化目录；
3. 要求每个根恰好有一个 `paimon-io-*`；
4. 写入 owner 索引（AD-3）。

`releaseAfterClose` 改为通过索引退役 lease。传入未登记的路径时仍按“无可释放”处理，与现有语义一致。Factory 与 Preflight 分别传 tableKey 和 `preflight:` + tableKey。`registerLiveDirs` 在本任务中暂时保留，只供测试使用，T05 删除。

**Acceptance criteria：**
- [ ] 写上下文与预检创建的 `paimon-io-*` 位于 `<root>/tapdata-paimon-v2/<jvmId>/<tag>-<uuid>/io/` 下；`withConfinedTempDirs` 返回这些路径
- [ ] `closeForStop` 成功后 lease 目录不存在；构造回滚与预检失败路径的“保留/完成”判定与改动前一致
- [ ] jvmId 获取失败时 Context 初始化失败，且根目录下没有新目录（AC-08 部分）

**Verification：**
- [ ] `TEST PaimonSpillDirCleanerTest,PaimonTableWriteContextFactoryTest,PaimonDynamicBucketPreflightCleanupTest`
- [ ] 上述测试中原来按旧锁文件路径（`resolveSibling("." + name + ".tapdata-owner.lock")`）断言的地方，改为断言 lease 锁文件

**Dependencies：** T03  
**Files likely touched：**
- `main/util/PaimonSpillDirCleaner.java`
- `main/write/PaimonTableWriteContextFactory.java`
- `main/service/PaimonDynamicBucketPreflight.java`
- `test/util/PaimonSpillDirCleanerTest.java`
- `test/write/PaimonTableWriteContextFactoryTest.java`
- `test/service/PaimonDynamicBucketPreflightCleanupTest.java`

**Estimated scope：** M（3 个生产文件 + 3 个测试文件的断言更新）

---

## Task 5：迁移 registerLiveDirs 测试，删除 owner 侧旧注册

**Description：** 新增测试工具 `SpillTestSupport.leaseIOManager(Path root)`，内部走生产的 lease 创建路径；把 6 个测试中的 `IOManager.create` + `registerLiveDirs` 机械替换为它，不修改原断言。之后删除 `registerLiveDirs`、`registerCreatedIOManager`、`registerPaths`，以及 `OWNER_LOCKS` 的注册路径。

**Acceptance criteria：**
- [ ] 生产代码与测试代码中不再出现 `registerLiveDirs` / `registerCreatedIOManager`
- [ ] 6 个迁移的测试断言不变且全部通过（AC-12 回归）
- [ ] 改动只限于调用替换和工具类

**Verification：**
- [ ] `TEST PaimonFinalCompactionIntegrationTest,PaimonCompactionSpillLifecycleIntegrationTest,PaimonFinalPrepareBucketModeIntegrationTest,PaimonNativeStopContractTest,PaimonServiceSyncStopIntegrationTest,PaimonServiceNativeMaintenanceIntegrationTest`
- [ ] `PACKAGE`

**Dependencies：** T04  
**Files likely touched：**
- `test/util/SpillTestSupport.java`（新）
- `test/write/PaimonFinalCompactionIntegrationTest.java`
- `test/write/PaimonCompactionSpillLifecycleIntegrationTest.java`
- `test/write/PaimonFinalPrepareBucketModeIntegrationTest.java`
- `test/write/PaimonNativeStopContractTest.java`
- `test/service/PaimonServiceSyncStopIntegrationTest.java`
- `test/service/PaimonServiceNativeMaintenanceIntegrationTest.java`
- `main/util/PaimonSpillDirCleaner.java`

**Estimated scope：** M（文件多，但都是同一处调用的机械替换）

### Checkpoint B（T03–T05 之后）
- [ ] `PACKAGE` 全绿，测试数与 T00 基线对比后记录差异原因
- [ ] 人工检查：跑一次 `PaimonWriteBufferSpillIntegrationTest`，确认运行期间只出现 v2 布局，结束后无 lease 残留
- [ ] 旧平铺扫描仍在运行，但只作用于根目录下的 `paimon-io-*`

---

## Task 6：PaimonSpillReclaimer（GC 侧）

**Description：** 按 plan §3.1 实现回收器。包内可见接缝：jvmId 提供器、文件系统类型探针、锁文件打开器（可计数）、审计 sink。原有平铺扫描的符号链接、深层目录、部分删除失败、只统计普通文件字节数等测试意图，迁移为本类的用例。

**Acceptance criteria：**
- [ ] 本 JVM 命名空间的打开次数为 0，且不列出其内容（AC-02 同 ClassLoader 部分）；jvmId 碰撞时跳过（AC-07）
- [ ] 缺锁文件时只 rmdir 空 lease；符号链接、名称不符、未知子根一律跳过（AC-08）；io 删除失败时保留锁文件，审计 sink 异常不影响结果（AC-17）
- [ ] 文件系统门禁：nfs4、fuse.*、未知类型、探针异常时返回 0、打开次数为 0、告警一次（AC-18）；同 JVM 两线程并发回收同一死亡 lease 时，无异常逃逸且至多一次成功（AC-05 部分）

**Verification：**
- [ ] `TEST PaimonSpillReclaimerTest`
- [ ] 回收器源码中没有 mtime、`System.currentTimeMillis` 或 grace 参数（代码审阅）

**Dependencies：** T03（可与 T04/T05 并行）  
**Files likely touched：**
- `main/util/PaimonSpillReclaimer.java`（新）
- `test/util/PaimonSpillReclaimerTest.java`（新）

**Estimated scope：** M

---

## Task 7：PaimonService 切换到回收器，删除平铺扫描

**Description：** `PaimonService.cleanupStaleSpillDirs` 沿用现有的根目录收集逻辑（全局与表级 diskTmpDir、符号链接根跳过），改为调用 `PaimonSpillReclaimer`。对每个根统计旧平铺 `paimon-io-*` 的数量并通过审计限流告警，但不打开、不删除。删除 plan §7 所列的旧代码，并逐项列出被删除或改写的旧测试用例。

**Acceptance criteria：**
- [ ] 启动时只回收 v2 布局中其他 JVM 已死亡的 lease；旧平铺目录与旧锁文件不被打开，也不被删除
- [ ] plan §7 列出的旧代码全部删除；生产代码中 `grep -E "tapdata-owner\.lock|LIVE_DIRS|newestModified|DEFAULT_STALE_GRACE_MS"` 无结果
- [ ] 回收结果和旧布局告警同时写入 SLF4J 审计与任务 TapLog 摘要

**Verification：**
- [ ] `TEST PaimonServiceStaleSpillCleanupTest,PaimonSpillDirCleanerTest,PaimonSpillReclaimerTest,PaimonServiceSyncStopIntegrationTest`
- [ ] `PACKAGE`

**Dependencies：** T05、T06  
**Files likely touched：**
- `main/service/PaimonService.java`
- `main/util/PaimonSpillDirCleaner.java`
- `test/service/PaimonServiceStaleSpillCleanupTest.java`
- `test/util/PaimonSpillDirCleanerTest.java`
- `test/service/PaimonServiceSyncStopIntegrationTest.java`

**Estimated scope：** M

### Checkpoint C（T06–T07 之后）
- [ ] `PACKAGE` 全绿
- [ ] 与用户复核删除的旧代码与旧测试清单
- [ ] 确认 `PaimonSpillDirCleaner` 行数下降，新类各自职责单一

---

## Task 8：隔离 ClassLoader 集成验收

**Description：** 用 T01 的隔离加载工具，在两个 ClassLoader 中分别加载 Connector 与 Paimon 类，通过测试源码中的驱动类调用创建、回收、建 channel 等操作。只使用纯 Java 的 IOManager，不加载 RocksDB JNI。

**Acceptance criteria：**
- [ ] AC-02：loader B 回收时对 loader A 命名空间的打开次数为 0
- [ ] AC-03：A 持有真实 IOManager 的 lease，B 扫描后再由子进程 C 扫描；C 删除数为 0，A 建 channel 成功，子进程 tryLock 失败。AC-04：创建钩子停在“已加锁、未返回”时，同 ClassLoader 的并发回收打开次数为 0
- [ ] AC-05：两个 loader 并发回收同一个死亡 lease。AC-15：丢弃 loader A 的引用并施加 GC 压力，lease 仍保留时子进程 tryLock 仍失败

**Verification：**
- [ ] `TEST PaimonSpillIsolatedClassLoaderIntegrationTest`

**Dependencies：** T07  
**Files likely touched：**
- `test/util/PaimonSpillIsolatedClassLoaderIntegrationTest.java`（新）
- `test/util/SpillLoaderDriver.java`（新，在各隔离 loader 内执行）
- `test/util/IsolatedConnectorLoader.java`（按需补充）

**Estimated scope：** M

---

## Task 9：多进程验收

**Description：** 把 `PaimonSpillDirCleanerProcessIntegrationTest` 改写为 `PaimonSpillProcessIntegrationTest`，复用其中的子进程启动、就绪与关闭模式；新增基于 stdin/stdout 的握手，用于控制并发顺序。

**Acceptance criteria：**
- [ ] AC-06：两个 GC 子进程回收同一死亡 lease，至多一个完成，全程不新建 `owner.lock`。AC-07：子进程持有的活跃 lease 被设为很旧的 mtime 后，父进程回收仍不删除
- [ ] AC-09：SIGKILL owner 子进程后，父进程回收其 lease；子进程 1 被杀后由子进程 2（新 JVM）回收
- [ ] AC-16：子进程先打开旧锁文件 → owner 退役 → 子进程 tryLock 成功但复核失败，什么也不删

**Verification：**
- [ ] `TEST PaimonSpillProcessIntegrationTest`

**Dependencies：** T07  
**Files likely touched：**
- `test/util/PaimonSpillProcessIntegrationTest.java`（由旧类改名改写）
- `test/util/PaimonSpillDirCleanerProcessIntegrationTest.java`（删除）

**Estimated scope：** M

---

## Task 10：生命周期回归断言

**Description：** 在现有 V1.2 生命周期测试中补充 lease 层面的断言：凡是“保留”的路径，lease 锁必须仍被持有（用子进程 tryLock 证明）；凡是“完成”的路径，lease 目录必须不存在。

**Acceptance criteria：**
- [ ] AC-10/11：FAILED_RETAINED 与“已取消但 worker 仍运行”时，lease 锁仍被持有，重复回收不删除
- [ ] AC-13：Factory 多根部分失败、DDL/rebuild、有限读 close 失败，未证明关闭的 lease 保持持锁
- [ ] AC-14：`rocksdb-*` 实际路径位于 `lease/io/paimon-io-*` 内，所有 bucket 策略与预检一致；AC-12 原有回归全绿

**Verification：**
- [ ] `TEST PaimonCompactionSpillLifecycleIntegrationTest,PaimonBoundedStopTest,PaimonTableWriteContextFactoryTest,PaimonFiniteReadLifecycleTest,PaimonServiceDdlCompactionFailureTest,PaimonDynamicBucketPreflightCleanupTest`

**Dependencies：** T07  
**Files likely touched：**
- `test/write/PaimonCompactionSpillLifecycleIntegrationTest.java`
- `test/service/PaimonBoundedStopTest.java`
- `test/write/PaimonTableWriteContextFactoryTest.java`
- `test/service/PaimonFiniteReadLifecycleTest.java`
- `test/service/PaimonDynamicBucketPreflightCleanupTest.java`

**Estimated scope：** M

---

## Task 11：旧布局并存与重复运行无残留

**Description：** 冻结一份 develop `e3f68298` 的平铺扫描逻辑作为测试夹具，证明新旧两版扫描互不进入对方的布局；再用真实写入反复启停，证明没有残留。

**Acceptance criteria：**
- [ ] AC-19：新回收器不打开旧锁文件、不删除旧目录，并输出告警；旧扫描夹具不进入 `tapdata-paimon-v2/`；回退场景中旧扫描不删除新目录
- [ ] AC-22：多表、多根反复启停后，所有根下无 lease 残留，进程打开的 channel 数回到基线

**Verification：**
- [ ] `TEST PaimonSpillLegacyCoexistenceTest,PaimonWriteBufferSpillIntegrationTest,PaimonSpillOptionsIntegrationTest`

**Dependencies：** T07  
**Files likely touched：**
- `test/util/PaimonSpillLegacyCoexistenceTest.java`（新）
- `test/util/LegacyFlatSpillCleanerFixture.java`（新，冻结副本并注明来源提交）
- `test/write/PaimonWriteBufferSpillIntegrationTest.java`

**Estimated scope：** S

### Checkpoint D（T08–T11 之后）
- [ ] plan §6 中除 AC-21 外的每个 AC 都有通过的测试
- [ ] `PACKAGE` 全绿，测试数与 T00 基线的差异有说明

---

## Task 12：文档、发布说明与 Spec 实现记录

**Description：** 更新连接器文档：部署规范（REQ-11）、升级后重启引擎的建议、旧平铺目录的一次性清理步骤、网络盘不自动回收的说明。在 Spec 末尾追加实现记录（源码位置、测试映射、执行证据）。在真实 Engine 中人工确认审计日志出现在引擎日志里。

**Acceptance criteria：**
- [ ] `paimon_zh_CN.md` / `paimon_en_US.md` / `paimon_zh_TW.md` 三种语言内容一致
- [ ] 发布说明写明：本次修复本地目录所有权，不提供全局表 fencing；对 2026-11 现场故障的结论以 OQ-04 为准
- [ ] AC-20 人工检查结果有记录；未能在真实 Engine 验证的，如实标注

**Verification：**
- [ ] 人工审阅文档；`git diff --check`

**Dependencies：** T08–T11  
**Files likely touched：**
- `connectors/paimon-plus-connector/src/main/resources/docs/paimon_zh_CN.md`
- `connectors/paimon-plus-connector/src/main/resources/docs/paimon_en_US.md`
- `connectors/paimon-plus-connector/src/main/resources/docs/paimon_zh_TW.md`
- `connectors/paimon-plus-connector/src/doc/specs/SPEC-paimon-spill-owner-isolation.md`

**Estimated scope：** S

---

## Task 13：Linux + JDK 17 验收（AC-21）

**Description：** 在 T00 确定的 Linux 环境中执行目录/锁相关测试和全量 package，并打印各根目录的实际 `FileStore.type()`，用于 OQ-05。

**Acceptance criteria：**
- [ ] `PaimonSpillIsolatedClassLoaderIntegrationTest`、`PaimonSpillProcessIntegrationTest`、`PaimonSpillReclaimerTest`、`PaimonSpillLeaseTest` 在 Linux + JDK 17 上通过
- [ ] `PACKAGE` 在同一环境通过；记录 OS、内核、JDK、文件系统类型
- [ ] 未覆盖的 OS/文件系统组合写明“不宣称支持”

**Verification：**
- [ ] `TEST PaimonSpillIsolatedClassLoaderIntegrationTest,PaimonSpillProcessIntegrationTest,PaimonSpillReclaimerTest,PaimonSpillLeaseTest`
- [ ] `PACKAGE`

**Dependencies：** T12、P-OQ-1  
**Files likely touched：** 本文件；Spec 实现记录  
**Estimated scope：** S

### Checkpoint E：完成
- [ ] AC-01–22 全部通过或有书面原因
- [ ] `git diff --check` 干净，`git status` 只包含计划内文件
- [ ] 用户审阅后决定是否建分支并提交
