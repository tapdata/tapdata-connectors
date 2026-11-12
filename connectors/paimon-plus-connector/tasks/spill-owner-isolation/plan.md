# 实施计划：Paimon Spill 进程命名空间与死亡 owner 回收

> 日期：2026-11-12；状态：Draft，待用户审阅批准；批准前不修改生产代码。  
> 规范：[SPEC-paimon-spill-owner-isolation.md V0.2](../../src/doc/specs/SPEC-paimon-spill-owner-isolation.md)（2026-11-12 已批准）。任务唯一事实源为 [todo.md](todo.md)。  
> 基线：tapdata-connectors develop `e3f6829863dab17a40904f98639e55224f625f53`；Paimon 1.3.2；生产代码 `<release>8`，测试 `<testRelease>11`。  
> 存放位置：模块 `tasks/plan.md`、`tasks/todo.md` 是 9 月 V1.2 的已完成记录，被 V1.2 Spec 与评审文档链接；根目录 `tasks/` 是另一项未完成的微批计划（80 个未勾选任务）。两者都不修改，本计划使用独立子目录。

## 1. Overview

把 Paimon spill 目录从根目录下的平铺 `paimon-io-*` 改为 `<root>/tapdata-paimon-v2/<jvmId>/<tableTag>-<UUID>/{owner.lock, io/}`。规则如下：

- **owner 侧**：在自己的 lease 内创建 IOManager；关闭证明通过后按“删 io → unlink 锁文件 → 释放锁 → 删 lease”的顺序退役。
- **GC 侧**：按目录名跳过本 JVM，只回收其他 JVM 中 OS 锁已释放的 lease；不读 mtime；文件系统不在白名单时不回收。
- **旧平铺目录**：不再触碰，只输出告警清单。

V1.2 的停止屏障、提交与取消语义保持不变。

## 2. 现状与接缝（已核对 develop 源码）

| 位置 | 现状 | 本次处理 |
| --- | --- | --- |
| `util/PaimonSpillDirCleaner.java`（624 行） | 外观 API + static `LIVE_DIRS`/`OWNER_LOCKS` + `OwnerLock` + 平铺扫描 + mtime 判定 | 保留外观 API；注册与扫描逻辑移入新类；删除平铺扫描 |
| `write/PaimonTableWriteContextFactory.java:198/268` | `resolveAndCreateIOManager(tmpDirs, scope)`；回滚时 `releaseAfterClose` | 传入 tableKey 作为 owner 标签；其余不变 |
| `service/PaimonDynamicBucketPreflight.java:92/144` | 同上 | 同上 |
| `write/PaimonTableWriteContext.java:466-470` | `ioManager.close` 后 `releaseAfterClose(spillDirs, true, controller)` | **不改**：外观内部改为 lease 退役 |
| `service/PaimonService.java:307/337-357` | 每次 onStart 扫描根目录，日志写任务 TapLog | 改为调用回收器；旧布局只告警；审计写 SLF4J |
| 测试 | 20 个测试文件直接构造 `PaimonTableWriteContext(…, ioManager, spillDirs, …)`；6 个用 `registerLiveDirs` | Context 构造签名不变；6 个文件机械迁移到 lease 创建 |

## 3. Architecture Decisions

| ID | 决策 | 理由 |
| --- | --- | --- |
| AD-1 | `PaimonSpillDirCleaner` 保留为外观：`resolveTmpDirs`、`splitTmpDirRoots`、`saturatedAdd`、`resolveAndCreateIOManager`、`withConfinedTempDirs`、`releaseAfterClose`、`IOManagerBuildResult`、`ReleaseResult` 保持语义 | 三处调用点与 Context 构造签名不变，避免 20 个测试文件改动 |
| AD-2 | 新增四个聚焦类，均在 `util` 包：`PaimonJvmIdentity`（身份）、`PaimonSpillLease`（owner 侧）、`PaimonSpillReclaimer`（GC 侧）、`PaimonSpillAudit`（审计） | owner 与 GC 是 Spec 要求的两个独立入口（REQ-04）；不再往 624 行的文件里堆逻辑 |
| AD-3 | owner 索引：每个 ClassLoader 一份 static Map，键为规范化 `paimon-io-*` 路径，值为 lease root；只供本 ClassLoader 的 owner 退役使用，GC 永不读取 | `releaseAfterClose(List<String>)` 无需改签名；强引用保证锁对象可达性不短于 lease（INV-02）；不作为删除判定（INV-01/03） |
| AD-4 | jvmId = `"j"` + SHA-256(`RuntimeMXBean.getName()` + `"|"` + `getStartTime()`) 前 32 位十六进制，每个 ClassLoader 计算一次并缓存 | REQ-02；值只依赖 JVM，跨 ClassLoader 必然相同 |
| AD-5 | lease 名 = SHA-256(owner 标签) 前 12 位十六进制 + `-` + UUID；写上下文标签为 tableKey，预检标签为 `preflight:` + tableKey | 表级归因且不暴露表名（REQ-01）；GC 只识别符合该格式的目录 |
| AD-6 | 多根：同一 leaseId 在每个根下各建一个 lease、各持一把锁；IOManager 以所有 `io/` 为根创建；任一根缺少 `paimon-io-*` 即构建失败 | REQ-01 全有或全无 |
| AD-7 | 测试接缝只用包内可见的构造器或重载（jvmId 提供器、文件系统类型探针、锁文件打开器、审计 sink、创建阶段钩子），不用可变 static 覆盖 | 避免测试间串扰；生产入口使用默认实现 |
| AD-8 | 审计用 `org.slf4j.Logger`（logger 名 `io.tapdata.connector.paimon.spill`）。slf4j-api 1.7.32 由 paimon-core 传递，shade 时排除，运行时由 Engine 提供，与 Paimon 自身日志走同一绑定；同时保留任务 TapLog 摘要 | REQ-10；不新增依赖 |
| AD-9 | 文件系统白名单常量：ext2/ext3/ext4、xfs、btrfs、zfs、tmpfs、overlay、apfs；大小写不敏感匹配 `FileStore.type()`；获取失败或 fileKey 为 null 视为不支持 | REQ-07、INV-08；最终清单见 OQ-05 |
| AD-10 | 实施顺序先 owner 侧、后 GC 侧。过渡期旧平铺扫描仍在运行，但它只列出根目录下的 `paimon-io-*`，看不到 `tapdata-paimon-v2/` | 每个任务结束时系统都可用，且新布局不受旧扫描影响 |

### 3.1 关键算法

**lease 创建**（`PaimonSpillLease.create`，对每个根执行）：

1. 校验子根与命名空间目录不是符号链接，然后 `createDirectories(<root>/tapdata-paimon-v2/<jvmId>)`。
2. `createDirectory(lease)`；如果因父目录刚被外部 GC 删除而报 `NoSuchFileException`，换新 UUID 重试，最多 3 次。
3. 以 `CREATE_NEW` 打开 `owner.lock` 并 tryLock，失败即创建失败。
4. 创建 `io/`。
5. 若任一根失败：已建成的根此时还没有 IOManager，也就没有异步使用者，立即退役并删除。

**owner 退役**（`retireAfterClose`，只在 IOManager 关闭成功后调用）：

1. 不跟随符号链接删除 `io/` 整棵树；
2. 持锁状态下 unlink `owner.lock`；
3. 释放锁并关闭 channel；
4. rmdir lease。

任一步失败都保留锁并返回失败（沿用 ReleaseResult 语义）。

**GC**（`PaimonSpillReclaimer.reclaim(roots)`）：

1. 对每个根：子根不存在或是符号链接则跳过；文件系统不在白名单则审计一次并跳过。
2. 列出命名空间目录：名称等于本 jvmId，或不符合 `j[0-9a-f]{32}`，或是符号链接，一律跳过；本 JVM 的命名空间**不列出其内容**。
3. 对符合格式的 lease：
   - 读取 `owner.lock` 的 fileKey（NOFOLLOW）；锁文件不存在时，只尝试 rmdir 空 lease。
   - 以 READ+WRITE、不带 CREATE、NOFOLLOW 打开锁文件并 tryLock；返回 null、OverlappingFileLockException 或任何异常都跳过。
   - 持锁后复核 fileKey 不变。
   - 删除 `io/` 树；失败则保留锁文件。
   - unlink 锁文件，关闭 channel，rmdir lease。
4. 处理完一个命名空间后尝试 rmdir（非空自然失败）。
5. 每个 lease 的审计事件先收集，在 channel 关闭后统一写出。

## 4. 依赖图

```text
T00 基线
 ├─ T01 PaimonJvmIdentity ─┐
 └─ T02 PaimonSpillAudit ──┤
                           ├─ T03 PaimonSpillLease
                           │    └─ T04 外观接入 lease（Factory / Preflight）
                           │         └─ T05 迁移测试 + 删除 owner 侧旧注册
                           └─ T06 PaimonSpillReclaimer（依赖 T03 的布局常量）
                                └─ T07 Service 切换 + 删除平铺扫描（依赖 T05）
                                     ├─ T08 隔离 ClassLoader 集成验收
                                     ├─ T09 多进程验收
                                     ├─ T10 生命周期回归断言
                                     └─ T11 旧布局并存与残留验收
                                          └─ T12 文档 / Spec 实现记录
                                               └─ T13 Linux + JDK 17 验收
```

可并行的部分：T01 与 T02；T06 在 T03 完成后可与 T04/T05 并行；T08–T11 在 T07 后互相独立。

## 5. Task List（详细内容见 todo.md）

### Phase 0：准备
- [ ] T00：基线测试与 Linux 验收环境确认

### Phase 1：基础（不改变行为）
- [ ] T01：PaimonJvmIdentity 与隔离 ClassLoader 测试工具
- [ ] T02：PaimonSpillAudit（SLF4J + sink 接缝 + 限流）

### Checkpoint A
- [ ] 模块编译通过；新增单测通过；生产行为未变

### Phase 2：owner 侧切换到新布局
- [ ] T03：PaimonSpillLease（创建、回滚、退役）
- [ ] T04：外观接入 lease，Factory 与 Preflight 传 owner 标签
- [ ] T05：迁移 `registerLiveDirs` 测试，删除 owner 侧旧注册

### Checkpoint B
- [ ] 模块 `package` 全绿
- [ ] 新写入只产生 v2 布局；正常关闭后无 lease 残留
- [ ] 旧平铺扫描只作用于旧目录

### Phase 3：GC 侧切换
- [ ] T06：PaimonSpillReclaimer
- [ ] T07：PaimonService 切换到回收器，删除平铺扫描与 mtime 判定

### Checkpoint C
- [ ] 模块 `package` 全绿
- [ ] `grep` 确认生产代码不再打开 `.tapdata-owner.lock`，不再引用 `LIVE_DIRS` 或 `newestModified`
- [ ] 与用户复核删除的旧代码清单

### Phase 4：验收证明
- [ ] T08：隔离 ClassLoader 集成验收（AC-02/03/04/05/15）
- [ ] T09：多进程验收（AC-03 外部部分、AC-06/07/09/16）
- [ ] T10：生命周期回归断言（AC-10/11/12/13/14）
- [ ] T11：旧布局并存与重复运行无残留（AC-19/22）

### Checkpoint D
- [ ] AC-01–20、AC-22 均有通过的测试映射（§6）
- [ ] 模块 `package` 全绿

### Phase 5：文档与目标平台
- [ ] T12：连接器文档、发布说明、Spec 实现记录
- [ ] T13：Linux + JDK 17 验收（AC-21）

### Checkpoint E：完成
- [ ] AC-01–22 全部通过或有书面原因
- [ ] `git diff --check` 干净；只改动计划内文件
- [ ] 用户审阅后再决定提交

## 6. 验收映射

| AC | 任务 | 证明方式 |
| --- | --- | --- |
| AC-01 | T01 | 两个隔离 ClassLoader 值相同；子 JVM 值不同 |
| AC-02 | T06、T08 | 用计数打开器断言：本 JVM 命名空间的打开次数为 0（同 ClassLoader 与跨 ClassLoader 各测一次） |
| AC-03 | T08、T09 | A 持锁、B 扫描、子进程 C 扫描；C 删除数为 0，A 的真实 IOManager 创建 channel 成功，子进程 tryLock 失败 |
| AC-04 | T08 | 创建阶段钩子停在“已加锁、未返回”，同 ClassLoader 并发回收的打开次数为 0 |
| AC-05 | T06、T08 | 同 JVM 两线程或两 ClassLoader 并发回收同一死亡 lease：无异常逃逸，至多一次成功 |
| AC-06 | T09 | 两个 GC 子进程回收同一死亡 lease；全程无 `owner.lock` 被创建 |
| AC-07 | T06、T09 | 回收器无时钟输入；子进程持有的活跃 lease 被设为很旧的 mtime 后仍不删除；jvmId 碰撞时跳过 |
| AC-08 | T01、T04、T06 | 缺锁、符号链接、名称不符、未知子根；身份失败时 Context 初始化失败且不产生 lease |
| AC-09 | T09 | SIGKILL owner 子进程后父进程回收；子进程 1 被杀，子进程 2（新 JVM）回收 |
| AC-10 | T10 | FAILED_RETAINED 后重复回收，锁仍被持有 |
| AC-11 | T10 | 取消后忽略中断的 worker：不关闭 rewriter、不删目录，子进程 tryLock 失败 |
| AC-12 | T05、T10 | 现有 V1.2 回归套件全绿 |
| AC-13 | T10 | Factory 多根部分失败、DDL/rebuild、有限读失败：未证明关闭的 lease 保持持锁 |
| AC-14 | T10 | `rocksdb-*` 实际路径位于 `lease/io/paimon-io-*` 内；所有 bucket 策略与预检一致 |
| AC-15 | T08 | 丢弃 loader A 引用并施加 GC 压力，lease 仍保留时子进程 tryLock 失败 |
| AC-16 | T09 | 握手：子进程先打开旧锁文件 → owner 退役 → 子进程 tryLock 成功但复核失败，什么也不删 |
| AC-17 | T03、T06 | io 删除失败保留锁文件；审计 sink 抛异常不影响释放与回收结果 |
| AC-18 | T06 | nfs4、fuse.*、未知类型、探针异常：返回 0，打开次数为 0，告警一次 |
| AC-19 | T11 | 新 GC 不打开旧锁文件；冻结的旧扫描器副本不进入 v2；回退场景不误删 |
| AC-20 | T02、T06、T12 | 单测断言审计字段；在真实 Engine 中人工确认日志出现在引擎日志 |
| AC-21 | T13 | Linux + JDK 17 真实子进程执行指定测试集 |
| AC-22 | T11 | 多表、多根反复启停后无 lease 残留、无 channel 泄漏 |

## 7. 删除的旧代码（T05/T07 执行，Checkpoint C 复核）

`PaimonSpillDirCleaner` 中的以下内容：

- `LIVE_DIRS`、`OWNER_LOCKS`、`OwnerLock`
- `registerLiveDirs`、`registerCreatedIOManager`、`registerPaths`、`lockFile`
- `OWNER_LOCK_SUFFIX`、`DEFAULT_STALE_GRACE_MS`
- `cleanupStaleSpillDirs`（含 `DeleteAction` 重载）、`newestModified`

`SPILL_DIR_PREFIX` 和递归删除访问器移入新类继续复用。测试中专门针对平铺扫描的用例改写为回收器用例或删除，T07 逐项列出。

## 8. Risks and Mitigations

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| `RuntimeMXBean.getName()` 格式依实现而定；容器 pid 相同、启动时间同一毫秒导致 jvmId 碰撞 | 低：碰撞只造成残留 | name 含 hostname；AC-07 覆盖碰撞即跳过 |
| `FileStore.type()` 字符串因内核和挂载方式而异（bind mount、overlay） | 中：误判为不支持会留残留；误判为支持会在网络盘上回收 | 白名单 + 未知即关闭；T13 在 Linux 上打印实际类型；OQ-05 由运维确认 |
| 隔离 ClassLoader 测试重复加载 RocksDB JNI 会报 UnsatisfiedLinkError | 中：测试不稳定 | 跨 ClassLoader 测试只用纯 Java 的 IOManager；RocksDB 路径断言（AC-14）放在单 ClassLoader 测试 |
| 测试迁移误改原有断言 | 中 | T05 只做机械替换，不改断言；完成后跑全量 package |
| 旧平铺残留不再自动回收，占用磁盘 | 中 | 限流告警列出路径；文档给出升级并重启后的一次性清理步骤 |
| Engine 的 slf4j 绑定把审计日志丢弃 | 中：归因再次失败 | 同时写任务 TapLog 摘要；T12 在真实 Engine 中人工确认 |
| 升级后不重启，旧 ClassLoader 仍运行旧扫描 | 中：旧目录仍有旧风险 | 发布说明要求升级后重启；新目录不受影响 |
| 网络/FUSE 共享卷上不回收导致残留累积 | 中 | 文档建议每个引擎独占 diskTmpDir（REQ-11） |
| Linux 验收环境不可用 | 高：AC-21 无法完成 | T00 先确认环境；未完成不宣称支持 Linux |

## 9. Open Questions

| ID | 问题 | 阻塞范围 |
| --- | --- | --- |
| P-OQ-1 | AC-21 用哪个 Linux + JDK 17 环境：GitHub `ubuntu-latest`（mr-ci）、office-build 自托管 runner，还是本机 Docker？本机 Docker 当前未运行 | T13 |
| OQ-04 | 现场取证（Spec §3.4）：共享 diskTmpDir 的 JVM 数量、文件系统类型、生产锁状态、删除者日志 | 只阻塞对外宣称“已根治”，与实施并行 |
| OQ-05 | 生产 OS 与文件系统白名单最终清单（默认见 AD-9） | 发布 |
| OQ-03 | 宿主卸载是否会在 worker 结束前回收 owner channel | T08 的 AC-15 与源码审计 |

## 10. 执行约定

- 定向测试：`mvn -B -pl connectors/paimon-plus-connector -am -DskipTests=false -Dtest='<类名列表>' -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test`
- 模块全量：`mvn -B -pl connectors/paimon-plus-connector -am -DskipTests=false package`
- 每个任务完成后在 todo.md 勾选并记录命令、测试数与结果；失败也如实记录。
- 只在用户要求时提交；当前在 develop 分支，提交前先建分支。
