# 实施计划：Paimon Spill 进程级命名空间与租约回收协议

> 版本：Plan v2，2026-11-12；状态：待用户复核。获批前不修改生产代码；已合入的 PR #1–#2 只包含文档与测试。\
> 规范：[SPEC V0.3](../../src/doc/specs/SPEC-paimon-spill-owner-isolation.md)；证据：[REVIEW 2026-11-12](../../src/doc/reviews/REVIEW-paimon-spill-owner-isolation-2026-11-12.md)；任务唯一事实源：[todo.md](todo.md)。\
> 代码基线：develop `e3f68298`；集成分支 `feature/paimon-spill-owner-isolation`，位于独立 worktree `tapdata-connectors-paimon-spill-owner-isolation`。主工作区中他人的暂存与未提交改动不受影响。\
> 依赖：Paimon 1.3.2；生产代码 `<release>8`，测试 `<testRelease>11`；构建需要 JDK 17。\
> 存放位置：模块 `tasks/plan.md` 与 `tasks/todo.md` 是 V1.2 的完成记录；根目录 `tasks/` 是未完成的微批计划。两者都不修改，本计划使用独立子目录。

## 1. 目标

按 Spec V0.3 §4 实现以下内容：

- 版本化的进程级命名空间；
- 以 OS 记录锁为唯一存活证明的租约状态机；
- owner 侧的 Provision/Retire；
- 回收者侧有界、无时间输入、带文件系统准入控制的 Reclaim；
- 引擎级审计。

用回归测试把现场缺陷（R1/R2）的结果反转，并保持 V1.2 停止语义不变。每一步都以可复查的本地 PR 交付。

## 2. 交付流程（Spec §9 的执行细则）

| 环节 | 规则 |
| --- | --- |
| 编号 | 任务编号即 PR 编号：Txx ↔ 本地 PR #xx；不属于任务清单的文档增补记为 `#<前一任务号>.<序号>`（如 #3.1），不占用任务编号 |
| 分支 | `pr/spill-owner-isolation/<NN>-<slug>`，从集成分支最新提交切出 |
| 提交 | 约定式提交；中文正文写清动机；只 `git add` 计划内路径；末尾附 Co-Authored-By 行 |
| 门禁 | 定向测试全绿；`git diff --check` 干净；检查点 PR 额外要求模块 `package` 全绿 |
| 变异检验 | 生产代码 PR 必做：反转一个新增条件，确认至少一个测试失败，再用 `git checkout -- <file>` 还原；把结果写进 PR 描述 |
| 合入 | `git merge --no-ff -F <描述文件>`；合并提交正文即 PR 描述，包含范围、动机、命令与结果（测试数/失败/错误/跳过）、变异检验、未覆盖项、复查命令 |
| 记录 | 下一个 PR 在 todo.md 中补记上一个 PR 的合并提交哈希 |
| 复查 | `git log --first-parent --merges feature/paimon-spill-owner-isolation`；`git show <merge>`；`git diff <merge>^1 <merge>` |
| 禁止 | 改写已合入的历史；推送远端（除非用户要求）；把他人改动带入提交 |

构建命令（Spec §12）：

```sh
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home
MVN=/Users/conradjam/software/apache-maven-3.9.16/bin/mvn
$MVN -o -B -pl connectors/paimon-plus-connector -am -DskipTests=false -Dtest='<类名>' \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test
$MVN -o -B -pl connectors/paimon-plus-connector -am -DskipTests=false package
```

本机的 `mvn` 是 `mvn-or-mvnw` 别名，且被 jenv 固定到 JDK 11，会因 `invalid target release: 17` 失败，必须直接调用 Maven 二进制。

## 3. 架构决策

| ID | 决策 | 理由 |
| --- | --- | --- |
| AD-1 | 保留 `PaimonSpillDirCleaner` 外观 API 与 `PaimonTableWriteContext` 构造签名 | 三处调用点不变；直接构造 Context 的 20 个测试文件不受影响 |
| AD-2 | 新增 `PaimonJvmIdentity`、`PaimonSpillLease`、`PaimonSpillReclaimer`、`PaimonSpillAudit` 四个类 | owner 与回收者是两个独立入口（REQ-04）；不再扩大已有 624 行的文件 |
| AD-3 | owner 索引：每个 ClassLoader 一份 static Map，键为 `paimon-io-*` 规范路径，只供 Retire 使用 | 不改变 `releaseAfterClose(List<String>)` 签名；强引用满足 INV-02；回收者不读取，满足 INV-01/03 |
| AD-4 | jvmId 按 Spec §4.2 派生：反射调用 `RuntimeMXBean#getPid`，Java 8 退回 `getName()` 中 `@` 之前的部分，再加 `getStartTime()`；每个 ClassLoader 惰性计算一次并缓存 | D1；结果与主机名无关（C3） |
| AD-5 | lease 名 = SHA-256(owner 标签) 前 12 位十六进制 + `-` + UUID；写上下文标签为 tableKey，预检标签为 `preflight:` + tableKey | 便于归因，又不暴露表名 |
| AD-6 | 根目录先 `createDirectories` 再 `toRealPath`，按 LinkedHashSet 去重 | D3；C9 证明 Paimon 本身不去重 |
| AD-7 | 测试接缝只用包内可见的构造器或重载：jvmId 提供器、文件系统类型探针、锁文件打开器（可计数）、审计 sink、Provision 阶段钩子 | 禁止可变 static 覆盖，避免测试串扰 |
| AD-8 | 审计使用 slf4j-api 1.7.32：由 paimon-core 传递引入，shade 时排除，运行时由 Engine 提供；与 Paimon 自身日志走同一绑定 | REQ-10，不新增依赖 |
| AD-9 | 文件系统白名单常量见 Spec §4.6；`fileKey` 为 null 视为不支持 | INV-08 |
| AD-10 | 先改测试抽象层，再翻转生产实现：T07 让测试通过 `SpillTestSupport` 访问锁文件位置与锁状态，并在**旧布局**上保持全绿；T08 翻转生产代码并替换该工具类的内部实现 | 修正评审问题 2：避免中间状态测试失败 |
| AD-11 | T08 把 R1–R3 改写为 AC-03 回归测试：步骤相同，结论反转。T10 删除平铺扫描时，把“旧版本清理器”替换为冻结副本 `LegacyFlatSpillCleanerFixture` | 现场缺陷持续得到回归保护 |
| AD-12 | 过渡期安全性：T08 合入后，owner 只创建 v2 布局；旧平铺扫描（T10 前仍存在）只列出根目录下的 `paimon-io-*`，看不到 `tapdata-paimon-v2/` | 每个 PR 合入后系统都可用 |

## 4. 依赖图

```text
T01 ✔ 基线文档 ─ T02 ✔ 平台特征化测试 ─ T03 ✔ Spec V0.3 / Plan v2
                                             │
                     ┌───────────────────────┴───────────────┐
                T04 jvmId                               T05 审计
                     └──────────────┬────────────────────────┘
                               T06 Lease（owner 协议）
                                    │
                               T07 测试抽象层（旧布局上全绿）
                                    │
                               T08 owner 侧翻转 + R1–R3 改为回归
                                    │                    T09 Reclaimer（T06 后可并行）
                                    └──────────┬─────────┘
                                         T10 Service 切换 + 删除平铺扫描
                         ┌──────────┬──────────┼──────────┐
                       T11 跨CL    T12 多进程  T13 生命周期  T14 并存与残留
                         └──────────┴────┬─────┴──────────┘
                                   T15 文档与发布说明
                                         │
                                   T16 Linux + JDK 17
```

## 5. 任务与检查点（详见 todo.md）

| 阶段 | 任务（= 本地 PR） | 规模 |
| --- | --- | --- |
| 0 准备 | T01 ✔ 导入 V0.2 基线文档（`2e2f66dd`）；T02 ✔ 平台特征化测试与现场复现（`213f9c49`）；T03 ✔ Spec V0.3、Plan v2、证据文档、全量基线 | — |
| 1 基础 | T04 PaimonJvmIdentity；T05 PaimonSpillAudit | S、S |
| 检查点 A | 新增测试全绿；生产行为未变 | |
| 2 owner 侧 | T06 PaimonSpillLease；T07 测试抽象层；T08 owner 侧翻转 | M、M、M |
| 检查点 B | 模块 `package` 全绿；只产生 v2 布局；AC-03 回归通过 | |
| 3 回收侧 | T09 PaimonSpillReclaimer；T10 Service 切换并删除平铺扫描 | M、M |
| 检查点 C | 模块 `package` 全绿；与用户复核删除清单（§7） | |
| 4 验收 | T11 跨 ClassLoader；T12 多进程；T13 生命周期回归；T14 旧布局并存与残留 | M、M、M、S |
| 检查点 D | 除 AC-24 外全部 AC 有通过的测试 | |
| 5 收尾 | T15 文档与发布说明；T16 Linux + JDK 17 | S、S |
| 检查点 E | AC-01–25 全部通过；用户复核后再决定是否推送 | |

## 6. 验收映射

| AC | 任务 | AC | 任务 |
| --- | --- | --- | --- |
| AC-01 | T04 | AC-14 | T13 |
| AC-02 | T09（同 ClassLoader）、T11（跨 ClassLoader） | AC-15 | T11 |
| AC-03 | T08（旧清理器）、T11（新回收器） | AC-16 | T12 |
| AC-04 | T11 | AC-17 | T06（单进程）、T12（跨进程） |
| AC-05 | T09（线程）、T11（ClassLoader） | AC-18 | T05、T06、T09 |
| AC-06 | T12 | AC-19 | T09 |
| AC-07 | T09（碰撞）、T12（mtime） | AC-20 | T06 |
| AC-08 | T04、T08、T09 | AC-21 | T08 |
| AC-09 | T12 | AC-22 | T14 |
| AC-10 | T13 | AC-23 | T05、T15 |
| AC-11 | T13 | AC-24 | T16 |
| AC-12 | T07、T08、T10、T13 | AC-25 | T14 |
| AC-13 | T13 | | |

## 7. 计划删除的旧代码（T08/T10 执行，检查点 C 复核）

`PaimonSpillDirCleaner` 中：

- `LIVE_DIRS`、`OWNER_LOCKS`、`OwnerLock`；
- `registerLiveDirs`、`registerCreatedIOManager`、`registerPaths`、`lockFile`；
- `OWNER_LOCK_SUFFIX`、`DEFAULT_STALE_GRACE_MS`；
- `cleanupStaleSpillDirs`（含 `DeleteAction` 重载）、`newestModified`。

`SPILL_DIR_PREFIX` 与 `DeletingFileVisitor` 移入新类继续使用。依赖上述符号的测试，在 T07、T08、T10 中逐项改写；旧行为由冻结副本 `LegacyFlatSpillCleanerFixture` 承载，只用于并存测试。

## 8. 风险与缓解

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| `getPid` 反射在 Java 8 运行时不可用 | 低：生产运行在 JDK 17 | 退回解析 `getName()` 中 `@` 之前的部分；格式不符即失败（fail-closed） |
| 容器间 pid 与启动毫秒同时相同，导致 jvmId 碰撞 | 低：只造成残留 | AC-07 证明碰撞时双方互相跳过 |
| `FileStore.type()` 字符串因内核与挂载方式而异 | 中 | 白名单之外一律拒绝；T16 记录 Linux 实际值；OQ-05 |
| 隔离 ClassLoader 测试重复加载 RocksDB JNI 报 UnsatisfiedLinkError | 中 | 跨 ClassLoader 测试只用纯 Java 的 IOManager；AC-14 在单 ClassLoader 中验证 |
| T07 测试重构误改原有断言 | 中 | T07 不改生产代码，旧布局上全量 `package` 必须全绿；diff 只包含访问方式的替换 |
| 旧平铺残留不再自动回收 | 中 | 限流清单告警；文档提供升级并重启后的一次性清理步骤 |
| Engine 的 slf4j 绑定丢弃审计日志 | 中 | 同时写任务 TapLog 摘要；T15 在真实 Engine 中人工确认 |
| 升级后不重启，旧 ClassLoader 仍运行旧扫描器 | 中 | 发布说明要求重启；新目录不受影响（AD-12） |
| Linux 环境不可用 | 高（AC-24） | OQ-06 先确定环境；完成前不宣称支持 Linux |

## 9. Open Questions

| ID | 问题 | 阻塞 |
| --- | --- | --- |
| OQ-06 | Linux + JDK 17 验收环境 | T16 |
| OQ-04 | 现场取证（证据 §8） | 对外宣称“已根治” |
| OQ-05 | 文件系统白名单最终清单 | 发布 |
| OQ-03 | 宿主卸载与 owner channel 的可达性 | T11（AC-15） |
