# Spec：Paimon SYNC 停止、主动取消与超时资源保留

> Spec ID：`paimon-spill-sync-graceful-stop`；版本：V1.2 实现契约；日期：2026-09-07。
> 状态：用户已确认“最终 Compaction 不能无限等；取消后仍未终止时，STOP 可超时失败返回，保留文件与 owner，确认旧进程退出后恢复”。**已按后续开发授权实现，定向回归通过；完整构建结果见 §14。** 默认预算已实现为 180/120/30 秒，可在 Service 配置调整。设计审查记录见 §13，实现与生产 DDL 验证见 §14–15。
> 实现基线：connector `962083ff6ae2369a6d85ded223099d09b89bb3f7`，V1.2 为其上的本地工作树修改，尚未提交；Paimon 1.3.2、Hadoop 3.3.6。本地 Paimon HEAD：`82a0b0914dc2dac84816bace2afd9abe2c908ec6`。
> 本文前半部分 V1.2 条款是当前实现契约；末尾折叠保存的 V1.1 是源码与历史验收档案，其中无期限 STOP、取消一律失败、不得增加超时配置等规则不再是新版本目标。V1.1 的 636 项成绩不能证明 V1.2 通过。其余未被替代的业务恢复、SYNC、目录保护和内核语义继续有效。

## 1. 目标与不可突破的边界

将“持续等待最终 Compaction 成功或失败”改为“限时正常完成 → 主动取消 → 限时确认实际终止 → 无法证明时保留资源并失败返回”。只有取得实际执行终止和访问者归零证明后才能删除 Spill。超时是退出等待的理由，不是删除文件、释放 owner 或确认 offset 的证据。

- 只支持有效 `snapshot.expire.execution-mode=SYNC`，不静默 ALTER。
- 仅 STOP 业务屏障成功后的最终 Compaction 允许正常弃提交；业务提交、未知提交结果、callback、状态保存和清理错误仍是硬失败。
- 不使用 Thread.stop，不杀死整个 Engine JVM，不删除仍可能被使用的文件。
- 不增加 reaper、TTL 自动释放或后台持续重试恢复器；超时保留后本进程不自动恢复这些表的写入资格。
- 用户确认不同机器、S3/兼容存储、允许确认旧进程退出后接管。**当前 JVM owner 不阻止另一机器写入，本 Spec 不宣称解决跨机器互斥。** P17 E01/E02 仍为部署边界；不能把 STOP 超时异常当成可安全调度新 writer 的 ACK。
- 为了使整个 STOP 等待受限，超时观察必须独立于被阻塞的业务、prepare、commit 和 close 调用；不能只在 prepare 返回后检查时间。

## 2. 时间预算与配置

以下为已实现的可配置默认值，120 秒参考 Flink 专用 Append worker 的等待量级，**不是 Paimon 推荐的全局默认值**；总预算和取消宽限期为 connector 设计选择。

| 配置（Service 级，禁止表级覆盖） | 默认值 | 起点与用途 |
| --- | ---: | --- |
| stopTimeoutSeconds | 180 | 首次 STOP 被发布时开始；包含 scheduler、ingress、业务 drain/callback、所有表最终 prepare、提交和资源清理 |
| finalCompactionTimeoutSeconds | 120 | 每表进入 FINAL_PREPARE 前开始；包含 prepare 内 flush/等待/结果校验，不能仅计 Future.get |
| compactionCancelGraceSeconds | 30 | 对该表首次发出受控取消时开始，等待原生调用展开及 executor 实际终止 |

三个值为正整数，初始化时校验；拒绝 0、负数、溢出及隐含无限等待。使用 System.nanoTime 和有界差值计算，重复 close 不重置预算。约 5 秒生成并非阻塞交付进度事件，阶段切换即时生成事件；实际日志可见性为正常后端下的尽力目标，见 §10。

- 总截止点 `Dstop = 首次STOP + 总预算`。
- 单表正常阶段截止点 `Dfinal = min(FINAL_PREPARE开始 + 正常预算, Dstop - 取消宽限期)`；总预算不足以预留宽限期时，不再开启该表最终 prepare，直接进入取消/安全清理候选路径。业务屏障必须先成功。
- 取消阶段截止点 `Dcancel = min(首次取消 + 取消宽限期, Dstop)`。
- 任何表耗时不得重置 Dstop。若某表取消宽限期耗尽，直接将 Service 置为超时保留，不能继续为下一张表累计完整预算。
- FINAL_COMMIT 已取得提交准入后，正常 Compaction 计时器失效；该提交仍受 Dstop 的调用者等待限制，但不能因超时把未知结果改记为弃提交。
- 这些是 connector 算法等待预算，不是实时系统硬截止保证；JVM 停顿和线程饥饿仍会影响调度。监督路径不得执行外部 IO、直接调用 Log 或等待原生方法；日志阻塞只允许影响日志交付，不允许阻塞超时判定。

## 3. 状态机：分离调用者终态与资源终态

```mermaid
stateDiagram-v2
    [*] --> DRAIN
    DRAIN --> FINAL_PREPARE: 业务和callback已确认
    FINAL_PREPARE --> FINAL_COMMIT: 校验通过且提交准入胜出
    FINAL_PREPARE --> CANCEL_REQUESTED: 正常预算到期且取消准入胜出
    CANCEL_REQUESTED --> WAIT_TERMINATION
    WAIT_TERMINATION --> CLEANUP: prepare退出且executor实际终止
    FINAL_COMMIT --> CLEANUP: 精确提交确认且executor实际终止
    CLEANUP --> SUCCESS: 资源完整关闭且无硬失败
    DRAIN --> FAILED_RETAINED: 总预算耗尽
    FINAL_PREPARE --> FAILED_RETAINED: 总预算耗尽
    FINAL_COMMIT --> FAILED_RETAINED: 总预算耗尽且结果未确认
    WAIT_TERMINATION --> FAILED_RETAINED: 宽限期耗尽
    CLEANUP --> FAILED_RETAINED: 关闭证明缺失或总预算耗尽
    FAILED_RETAINED --> [*]: STOP失败返回，资源隔离至进程退出
```

`SUCCESS` 可带 `compactionDiscarded=true`。纯最终 CompactTask 普通失败仍可沿无提交路径进入 WAIT_TERMINATION。任何阶段的硬失败优先于正常/弃提交；如果仍能在预算内安全清理，返回普通 FAILED，资源确实清理完成的表可以按原规则释放 owner。若清理报错且仍有未关闭资源，即使未到截止时间，也进入 FAILED_RETAINED 并保留原始错误；普通 FAILED 只用于所有资源已确认关闭的情况。

`FAILED_RETAINED` 是不可逆的调用者失败终态；不等于执行线程已终止，不等于资源全部存在且从未被修改。重复 close 立即复用已发布结果，不开启新 worker、不再给新预算、不重试提交或清理。迟到结束不能改成 SUCCESS，不能打印正常退出。

## 4. 监督路径与实际实现接缝

保持单次 Service 停止操作和一个 close worker，由 PaimonStopController 承担协调，不增加每表定时线程。close 调用者作为监督者以最早截止点/日志间隔分段 await；多个调用者通过短临界区只执行一次取消或终态发布。不得依赖持有 Service `synchronized(this)` 或 Context monitor 的 worker 来触发超时，因为当前 closeForStop 会在这些锁内阻塞。

新增职责必须集中，而不是在各 catch 中散落超时分支：

| 组件 | 必须承担的职责 |
| --- | --- |
| PaimonStopController | 保存不可重置预算、单表 final attempt、提交/取消选择、不可逆保留态、唯一调用者结果；监督读状态和取消不取 Service/Context 长锁 |
| PaimonCompactionExecutor | 保存仍未实际退场的受控任务，按 attempt 执行取消，停止新任务准入，提供真实 termination 和异常审计 |
| PaimonTableWriteContext | prepare/校验后走提交准入；取消获胜则丢弃整次最终 messages；实际终止后才全 bucket sync 与关闭 |
| 生命周期及中央提交/callback入口 | 阻断超时发布后的新外部动作；保留已有精确 pending 和错误分类，不将未知结果改成成功 |
| 静态 retained owner 记录 | 强引用被保留的 Service/Context/IOManager/文件锁/提交状态；仅标记 token 而不保留资源不合格 |

提取 PaimonStopController 承担现有 CloseOperation 的协调职责，替换原内嵌 CloseOperation，避免重复维护两套终态。现有 PaimonServiceLifecycle 继续只负责 ingress/consumer 计数与准入，不持有 Paimon 资源。静态资源记录只保留失败 Service 的实际资源，键为本次 service owner；禁止保存所有历史成功任务，不引入周期扫描器。同一物理表在该进程不允许反复超时后生成新代，因此不会靠重试无限新增同表保留实例；保留资源数量需要可诊断。

锁纪律：Service 控制门禁只保护截止点、final decision、动作许可和结果发布；不得在其中调用 Lifecycle、executor、logger、原生库或外部回调。Executor 的 controlLock 只保护任务登记、接收和取消来源。先在 Service 门禁发布取消决定，释放门禁后再封闭 executor 并扫描已登记任务；在两步间已经接收的任务也在扫描范围内。任务提交按 executor controlLock → controller gate 顺序检查总截止点和取消意图；coordinator/Context 同样先取得自身状态锁，再进入 gate 执行纯内存发布。gate 内不反向取得这些组件锁；取消请求先释放 gate，再进入 executor。禁止形成 Service 控制锁 → executor 锁 → Service 控制锁或 Lifecycle 锁的环。

## 5. 主动取消必须有来源证明

Paimon 原生取消会吞掉 CancellationException，因此不能靠 prepare 的返回值判断“取消成功且可提交”。采用 connector 私有 STOP attempt 身份，关联 Service owner、Context 和 final identifier。

取消范围包括业务屏障完成后该 Context 中仍在运行或排队的既有 Compaction，以及最终 prepare 新触发的任务；不能只追踪创建 final attempt 之后提交的任务。授权只改变本次停止阶段的处理方式，不追溯豁免这些任务已经发生的硬失败。

1. 仅业务屏障成功、尚未取得 FINAL_COMMIT 准入的 final attempt 可以进入“可正常弃提交的主动取消”。
2. 按 §4 发布取消决定后，在 executor 短临界区封闭接收并处理已登记任务，submit 与取消扫描共用 executor 锁。每个 Future 在实际 cancel 前登记私有授权；若 cancel 返回 false，不追溯认领它先前发生的取消或异常。外部 cancel 与授权审计也在该锁内，避免唤醒 get 后丢失来源。
3. 任务登记在提交给真实 executor 之前完成；提交失败回滚登记。`Future.isDone()` 不作为移除正在执行任务的依据，取消后的 Callable 仍须保留到实际 run 退出。采用可见 run-finally 退场信息或 executor 终止后整体释放，不保存无界历史列表。
4. 对已接收任务逐个调用带私有授权的 cancel(true)；底层 shutdownNow 返回的未启动 Runnable 也必须取消其 Future，避免移出队列后原生 get 永久等待。shutdownNow 自身不等于取消每个 Future。
5. **受控取消不能让原生 get 返回 Optional.empty。** GuardedFuture.get 和 get(timeout) 捕获 CancellationException 后，在 executor 锁内核对该 Future 的实际取消归属，仅对本次私有授权取消改抛 `ExecutionException(StopCompactionCancelled)`；其中原因类型及 attempt 不能由外部构造，StopCompactionCancelled 不继承 CancellationException，也不复用普通 NativeCompactionFailure 分类。CompactFutureManager 只吞 CancellationException，因而该异常会让 prepare 展开，阻止它继续生成空 committable 后在 AbstractFileStoreWrite.prepareCommit 内联关闭 bucket writer。未经授权的取消也不能再向原生暴露可被吞掉的 CancellationException：记录硬失败并以 ExecutionException 包装私有控制失败向上传播，禁止正常弃提交。不能仅检查 Service 已进入取消态便豁免所有 Future。这是仅供已验证 Paimon CompactTask 使用的窄适配行为，偏离通用 Future.get 的取消异常形式，必须在代码注释说明，不能推广为公共通用执行器。
6. prepare 也可能尚未到 get，正在 flush 或准备再次 triggerCompaction。取消已封闭 submit 时，通过带同一 attempt 的私有受控拒绝展开调用；普通 RejectedExecutionException 仍是硬失败。禁止接收新任务或转交其他池。已经进入原生方法的内部 IO/finally 无法逐条撤销，仍按在途动作处理，不把该限制掩盖成完全无副作用。
7. 不中断整个 close worker 来代替取消 Compaction，因为它可能已在执行 commit、SYNC maintenance、callback 或文件删除。外部打断 STOP 调用者仍记硬失败；必要时进入安全保留，不变成可正常弃提交。
8. 可豁免项仅为**被记录的本次控制动作直接产生**的取消/中断或受控提交拒绝。原先已记录的控制失败、任意 Error、未知任务、独立 I/O 失败和原因不明的中断均不得被授权 token 掩盖。取消后 Callable 的迟到异常仍单独审计，不能被 FutureTask 的取消状态丢弃。
9. 即使所有 Future cancel 返回 true，仍必须确认 executor.isTerminated，并确认 final prepare 调用已返回/展开；prepare 线程自身也可能访问 writer、RocksDB 和 Spill。

公共 `PaimonCompactionExecutor.shutdownNow()` 仍将无授权强制关闭记为硬失败；主动取消仅由 `cancelForStop(FinalAttempt)` 执行。原生 Future 的外部 cancel 不能冒用该私有授权。

## 6. 提交与取消的线性化竞争

final attempt 的短状态选择为 `PREPARING → COMMIT_ADMITTED` 或 `PREPARING → CANCEL_REQUESTED`，同一次 attempt 只能有一个胜者；无需用锁覆盖整个 S3 RPC。

- 提交胜出：先审计业务增量、已有控制失败及截止状态，保存同一 identifier/messages 的 pending，再执行原有提交确认协议。计时器不得随后发起“正常弃提交”取消。总预算耗尽时，未确认的提交保留为未知结果。
- 取消胜出：即使 prepare 稍后返回非空 messages，也不能提交这些 messages，不推进 identifier 或发布最终 offset；必须丢弃整表这次最终尝试，不挑出部分 bucket 提交。
- 原生 prepare 返回的 DataIncrement 非空、消息类型不匹配仍是硬失败；不能因为正在取消而跳过消息校验。
- 总超时与动作准入共用同一 Service 级短门禁。提交、提交重试、pending 状态保存、callback 开始和破坏性清理步骤都要经过它。实现使用动作门禁与短锁内发布，不能仅靠 sticky failure 阻止后续步骤。
- 在超时之前已取得动作许可的操作视为在途，即使线程尚未进入外部库，也不能保证撤销；它可能迟到落地。超时之后不允许再取得新动作许可。未使用且可明确撤销的许可应作废；不能宣称跨外部存储与本地状态存在原子事务。
- 结果未知时不清除 pending、不换 identifier/messages、不重试盲写。同进程 FAILED_RETAINED 期间保留精确 pending，但禁止继续重试。确认旧进程退出后的重启只按 stable commitUser、nextIdentifier 和 latest same-user snapshot 对账：现有持久状态不保存 CommitMessages 或 offset，无法重建原 pending envelope，不承诺跨进程精确重放该 envelope。数据重放仍依赖既有上游 offset/at-least-once 契约；超时响应不能承诺该提交完全没有发生。

## 7. FAILED_RETAINED 的资源冻结与恢复

监督者冻结动作准入后，先完成强引用保留登记，再发布不可变终态并唤醒所有 close 调用者；超时分支抛出专用超时异常。不得先发布 finished/SUCCESS 后才登记保留资源。超时异常包含阶段、table/owner、预算和已有原因；非超时的关闭证明失败保留原异常主因。所有调用者复用相同主因。控制门禁的发布不得等待原生 IO；日志输出也不能成为发布保留态的前置阻塞条件。

保留未完整关闭 Context 的 writer/committer/IOManager、Spill 目录、owner 文件锁、Service owner 和必要的状态引用。不能在 performDrainAndCleanup.finally 无条件清空微批状态、boundTaskStateMap、Context 映射或释放 owner；不能随后关闭 Catalog/FileIO。`releaseAfterClose(spillDirs, false)` 已改为保留 live、文件锁与 marker。仅在 IO 正常关闭后释放目录保护，返回 ReleaseResult（完成状态、保留路径、异常原因）；Context/Factory/preflight 消费该结果。注册失败的 helper 不做无证明删除，而是保留 manager 和已经登记的 owner；未成功释放的引用仍保留，已完成的释放不重建。失败对象须有静态强可达引用，防止 Connector.onStop 将 service 置空后通过 GC/Cleaner 释放仍需要的资源。

迟到 worker 从被阻塞调用返回后，只允许记录诊断、释放纯 Java 协调锁并退出；不进入新的提交、callback、原生 sync、writer/committer/IO close 或 owner 释放步骤。每个阶段和动作边界重新检查不可逆保留态。已在线程内获准并开始的原生方法，其内部动作不能被 Java 门禁逐条控制；这也是只能报告结果未知而不能保证“超时后绝无副作用”的原因。

构造中的 ingress 同样纳入资源保留：超时后不得把新构造资源发布成可用 Context；迟到取得的资源必须挂入同一保留记录，不能丢失引用后做无证明回滚。已有业务/scheduler/读操作未归零时，不并发调用 writer.close 或 IOManager.close。Factory 创建每项资源前先登记构造占位，返回后绑定资源；其 catch/rollback 也必须经过控制门禁，不能在已保留后沿原 catch 继续关闭或丢失局部引用。

有限读资源单独记账：入口 permit 只证明调用者活动，不能证明 reader.close 成功。batchRead、batchCount、queryByAdvanceFilter 的 reader 创建前登记占位、返回后绑定引用；关闭成功才注销。close 抛错时保留 reader 及关联 Table/Catalog/FileIO，传播或记录硬失败，不允许仅 WARN 后关闭共享 Catalog。超时后迟到 reader 的新读取、consumer 和新关闭动作也必须被门禁拒绝；已经开始的原生读取/关闭内部动作不宣称可撤销。长期 streamRead 不属于此次协议验收范围，不得把有限读门禁成绩表述为 StreamRead 安全退出已完成。

正常清理已经完成并释放的表不重新锁住；在途 IO close 若在超时前已有完整访问者终止证明，可以继续其已授权内部操作，但返回后不能启动下一步释放。此时目录可能已部分或完全删除，仍保留 owner 并报告超时。不能承诺所有 timeout 都保留完整原始目录。

不自动降级为正常退出、不增加 reaper、不按时间释放。恢复流程为确认旧进程退出后重新启动；OS 锁随实际进程退出解除，本地 stale cleaner 按既有 marker/live/grace 规则处理。裸历史 RocksDB 和已移除配置根仍不自动回收。**恢复并不补足跨机器表锁：另一个 Engine 必须等待真实旧进程退出，现有 connector 自身无法强制这一调度条件。**

## 8. 与原生 Flink 的取舍

| 原生事实 | 本 Spec 的使用方式 |
| --- | --- |
| endInput 调用 prepareCommit(true)；普通 checkpoint false | 保留业务屏障后的最终 prepare，不把所有业务提交改为强制等待 |
| MergeTreeWriter.close 先 cancelCompaction 再 sync | 只借鉴主动取消意图；不将该 sync 当成取消后实际 termination 证明 |
| CompactFutureManager 吞掉 CancellationException、清空 Future | 必须记录私有取消来源、禁止取消后伪成功提交，并保留实际执行追踪 |
| AbstractFileStoreWrite.close 对自有池仅 shutdownNow，无 awaitTermination；注入池完全不关闭 | connector 自行等待实际终止；不能把原生 close 作为超时清理证明 |
| AppendCompactWorkerOperator 最多 awaitTermination 120 秒，超时 WARN 后继续 compactor.close | 采用两阶段停止思路；不照搬超时继续清理，改为 FAILED_RETAINED |
| CompactFutureManager 源码 TODO 提示取消可能留下 orphan files | 本地 Spill 安全不等于远端未提交文件自动回收；不在取消路径猜测并删除远端文件 |

## 9. 源码核对记录（2026-09-07）

core sources JAR SHA-256：`f8c6d7b57543fb1115dfbbeed1ce0f598d8322f2601c30838f983f64b7ddae63`。以下 11 个 core 文件与该 JAR 一致；4 个 Flink 文件与官方固定提交 `c05f7d1f1b1e5d37e64edab0f2978124d90b64f7` 原文一致。比较不代表整个本地分支等于 1.3.2。

| 入口 | 证明事实 |
| --- | --- |
| [CompactFutureManager:34](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java:34) | cancel(true)，47 行结果获取吞取消；Future 清空不证明线程终止 |
| [CompactTask:47](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/compact/CompactTask.java:47) | doCompact 在 Callable 内执行且 finally 更新指标；取消后 Callable 仍可能执行/抛错 |
| [MergeTreeWriter:251](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:251) | prepare 会 flush、触发任务和等待；343 行 close 请求取消，之后 sync |
| [AbstractFileStoreWrite:304](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:304) | bucket writer 顺序 close，自有池仅 shutdownNow；注入池的关闭权另由调用者持有 |
| [TableWriteImpl:134](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java:134) | 可注入 connector executor，不需要修改内核 |
| [TableCommitImpl](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java) | 同步提交/维护可能阻塞，不能对整个 close worker 随意中断或把结果未知当弃提交 |
| [IOManagerImpl:74](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java:74) | close 委托 Channel Manager |
| [FileChannelManagerImpl:125](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java:125) | close 删除管理的目录，因此必须在访问者退出后调用 |
| [PrepareCommitOperator:96](/Users/SL/javaProject/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/PrepareCommitOperator.java:96) | checkpoint 与 endInput 的等待参数不同 |
| [TableWriteOperator:141](/Users/SL/javaProject/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/TableWriteOperator.java:141) | 算子 close 下传到 StoreSinkWrite |
| [StoreSinkWriteImpl:172](/Users/SL/javaProject/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreSinkWriteImpl.java:172) | write.close 后 paimonIOManager.close；该方法不包含无限 termination 等待 |
| [AppendCompactWorkerOperator:105](/Users/SL/javaProject/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/AppendCompactWorkerOperator.java:105) | shutdownNow、最多等 120 秒、超时 WARN 后继续 close |

官方原文：[Flink 专用 Compaction worker](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/AppendCompactWorkerOperator.java#L105)、[原生取消与结果获取](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L34)。

JDK 依据：[ExecutorService.shutdownNow / awaitTermination](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ExecutorService.html#shutdownNow()) 明确区分尝试中断与等待实际终止，无法保证不响应中断的任务结束。[Future.cancel](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/Future.html#cancel(boolean)) 的完成状态不是 Callable 物理退出证明。禁止用私有 API 强杀线程补足这项限制。

本轮另以本地 JDK 17 运行独立并发探针：单线程池执行一个忽略中断、等待 latch 的任务，并排队第二个 Future；取消执行中任务后调用 shutdownNow。断言与实际输出如下，最后释放 latch 并确认线程池退出，探针未留下后台任务。

```text
cancelled Future done=true; executor terminated=false; drained queued Future done=false
after actual task release: executor terminated=true
```

该探针验证“取消完成不等于实际退场”与“shutdownNow 移出的队列 Future 未自动取消”两个 JDK 行为；不等于 connector 主动取消功能已实现，也不替代 B01–B22 回归门禁。

### 9.1 方法级调用链与关键代码位置

以下范围是本轮逐段阅读位置，链接落到起始行。行号随本地分支变化，应同时核对方法名、固定依赖版本与后面的 SHA-256；不能只复制行号。

| 编号 | Paimon 源码 / 方法 | 核对行范围 | 对本设计的约束 |
| --- | --- | --- | --- |
| PS01 | [PrepareCommitOperator.java#prepareSnapshotPreBarrier / endInput](/Users/SL/javaProject/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/PrepareCommitOperator.java:93) | 93–104 | checkpoint 发出 false；endInput 发出 true，并非所有 STOP 都先执行 endInput。 |
| PS02 | [TableWriteOperator.java#prepareCommit / close](/Users/SL/javaProject/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/TableWriteOperator.java:141) | 141–152 | 下传 StoreSinkWrite；close 本身不调用 endInput。 |
| PS03 | [StoreSinkWriteImpl.java#prepareCommit / close](/Users/SL/javaProject/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreSinkWriteImpl.java:144) | 144–177 | 等待参数为 this.waitCompaction || waitCompaction；write.close 后调用 paimonIOManager.close。 |
| PS04 | [TableWriteImpl.java#withCompactExecutor / prepareCommit / close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java:134) | 134–137；260–274 | 注入池；prepare/close 委托 FileStoreWrite。 |
| PS05 | [AbstractFileStoreWrite.java#prepareCommit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:185) | 185–267 | 逐 bucket 准备；238–255 空结果分支会内联 writer.close，不能只保护外层 close。 |
| PS06 | [MergeTreeWriter.java#flushWriteBuffer / prepareCommit / sync](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:209) | 209–277 | flush 后 get 最新结果并 trigger；prepare 再等待并 drain 增量；sync 不新提交任务。 |
| PS07 | [AppendOnlyWriter.java#prepareCommit / flush / sync / close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/append/AppendOnlyWriter.java:221) | 221–265 | append 同样消费 Compaction Future；close 请求取消后 sync 并清理。 |
| PS08 | [MergeTreeCompactManager.java#getCompactionResult](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactManager.java:249) | 249–268 | 不吞 ExecutionException，能传播私有受控取消原因。 |
| PS09 | [BucketedAppendCompactManager.java#getCompactionResult](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/append/BucketedAppendCompactManager.java:182) | 182–197 | 同样传播 ExecutionException，覆盖 bucketed append 分支。 |
| PS10 | [CompactFutureManager.java#cancelCompaction / innerGetCompactionResult / obtainCompactResult](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java:34) | 34–67 | cancel(true)；只吞 CancellationException；finally 清空 Future；底层调用 get。 |
| PS11 | [MergeTreeWriter.java#close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:343) | 343–375 | cancel → sync → compactManager.close → 文件清理；必须在此之前阻止未终止任务的并发访问。 |
| PS12 | [AbstractFileStoreWrite.java#withCompactExecutor / close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:147) | 147–150；304–317 | 注入池关闭权归 connector；原生自有池 shutdownNow 没有 await。 |
| PS13 | [TableCommitImpl.java#构造 / commitMultiple / maintain / close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:118) | 118–124；225–238；348–400 | SYNC 使用 direct executor；commit 后内联维护；维护捕获 Throwable 保存，不能借中断 close worker 实现仅取消 Compaction。 |
| PS14 | [IOManagerImpl.java#close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java:74) | 74–75 | 下传 Channel Manager；FileChannelManagerImpl.close:125–153 会删除目录。 |
| PS15 | [AppendCompactWorkerOperator.java#close](/Users/SL/javaProject/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/AppendCompactWorkerOperator.java:105) | 105–114 | 专用 unaware-bucket compactor 最多等 120 秒，超时 WARN 后仍 close；不是通用 Flink sink STOP 的完整安全证明。 |

```mermaid
flowchart TD
    F["Flink endInput / checkpoint · PS01"] --> FW["StoreSinkWriteImpl.prepareCommit · PS02/PS03"]
    C["Connector prepareFinalCommit(true)"] --> TW["TableWriteImpl.prepareCommit · PS04"]
    FW --> TW
    TW --> AW["AbstractFileStoreWrite 逐 bucket prepare · PS05"]
    AW --> RW["MergeTreeWriter / AppendOnlyWriter · PS06/PS07"]
    RW --> FM["CompactFutureManager → Future.get · PS08/PS09/PS10"]
    FM --> RAW["原始取消：吞异常 → 空结果"]
    RAW --> INLINE["可能进入 prepare 内联 writer.close · PS05"]
    FM --> CONTROL["V1.2 私有取消：ExecutionException → 展开 prepare"]
    CONTROL --> BARRIER["Connector 确认 prepare 退出及 executor termination"]
    BARRIER --> CLOSE["sync 全 bucket → writer → committer → IOManager"]
    CLOSE --> DELETE["FileChannelManagerImpl.close 删除 Spill"]
```

关键代码摘录（保持原文，省略上下文只为定位）：

**原生只吞 CancellationException，私有 ExecutionException 可以穿透**：[源码](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java:52)（52–58 行）。

```java
try {
    result = obtainCompactResult();
} catch (CancellationException e) {
    return Optional.empty();
} finally {
    taskFuture = null;
}
```

**prepare 内部确实存在 writer.close，不能把它当纯计算阶段**：[源码](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:252)（252–255 行）。

```java
            commitIdentifier);
}
writerContainer.writer.close();
bucketIter.remove();
```

**注入 executor 的关闭权**：[源码](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:147)（147–150 行）。

```java
public void withCompactExecutor(ExecutorService compactExecutor) {
    this.lazyCompactExecutor = compactExecutor;
    this.closeCompactExecutorWhenLeaving = false;
}
```

**Flink 专用 worker 的 120 秒分支**：[源码](/Users/SL/javaProject/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/AppendCompactWorkerOperator.java:105)（105–114 行）。

```java
public void close() throws Exception {
    if (lazyCompactExecutor != null) {
        // ignore runnable tasks in queue
        lazyCompactExecutor.shutdownNow();
        if (!lazyCompactExecutor.awaitTermination(120, TimeUnit.SECONDS)) {
            LOG.warn(
                    "Executors shutdown timeout, there may be some files aren't deleted correctly");
        }
        this.unawareBucketCompactor.close();
    }
```

**IOManager 下游的目录删除位置**：[源码](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java:139)（139–154 行）。

```java
private AutoCloseable getFileCloser(File path) {
    return () -> {
        try {
            FileIOUtils.deleteDirectory(path);
            LOG.info(
                    "FileChannelManager removed spill file directory {}",
                    path.getAbsolutePath());
        } catch (IOException e) {
            String errorMessage =
                    String.format(
                            "FileChannelManager failed to properly clean up temp file directory: %s",
                            path);
            throw new UncheckedIOException(errorMessage, e);
        }
    };
}
```

### 9.2 本轮原生取消机制探针

2026-09-07 用当前测试 classpath 的 Paimon 1.3.2 `CompactFutureManager` 运行独立探针（`/tmp/PaimonNativeCancelProbe.java`）：后台任务用 latch 保持运行并忽略 interrupt。原始 Future 取消后，内核返回 empty；将 get 的受控取消转换成私有 ExecutionException 后，内核向上传播异常，后台任务仍须单独确认终止。实际输出：

```text
raw cancel: native result acquisition returns empty; worker still active=true
translated cancel: native result acquisition propagates ExecutionException; worker still active=true
```

两个分支均在 finally 放行任务并 awaitTermination 成功。探针证明 PS10 的异常传播接缝有效，不证明整个 prepare、bucket 内联清理、提交门禁或 V1.2 的端到端协议已通过；这些由 B06/B19 等回归负责。

### 9.3 逐文件校验

逐文件 SHA-256（用于实现前复核漂移）：

| 文件（Paimon 仓库相对路径） | SHA-256 |
| --- | --- |
| `paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java` | `03d5ab04852cc05fd94e04684f30f18b7ddd1ac0c19339002f3010287a3d1be1` |
| `paimon-core/src/main/java/org/apache/paimon/compact/CompactTask.java` | `4c7e48c29634ea292d527da4a88cf98d874d480f7a3cb316fa34d17f0b0ce004` |
| `paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java` | `461117a6abbd2501cd68e60d55e5add5eded6f5230e785c044d7532b301564b6` |
| `paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java` | `b51366517a220c19389c70fdb3ca77577b2d9f3320a920bf179fc22baaea583e` |
| `paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java` | `ad17a681f7c1ca37d0f5b769504402ca743fe9497147db31df6847e4010422c4` |
| `paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java` | `e93ee9bec2bfcde80276e8ba00922e01ec7b69d279c2dfbdeac47f681f139810` |
| `paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java` | `df39caa5cd57178cffc15f759573d8537729788ef5da994192efedf365e67253` |
| `paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java` | `30b263a1270eb03b17021bd5db19a4f02b9c22c5bfbe83510251bf8121337a9b` |
| `paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/PrepareCommitOperator.java` | `cf26d300627dda4176c5617a08df9bcba54b1888ea7100e7f384792c7a7ef1c3` |
| `paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreSinkWriteImpl.java` | `692f9d69a512bcaab538bfe00c41c127416f68425a4c34cc2079c1ea357bf878` |
| `paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/TableWriteOperator.java` | `479aeb5aa09455a414797bc8bf80598114303e3f6b56afca53c47e7c9d6b272e` |
| `paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/AppendCompactWorkerOperator.java` | `920d3733bc6758bb89ac009e47aba83463f6d3b758ea33ef1089a891b7a87b48` |
| `paimon-core/src/main/java/org/apache/paimon/append/AppendOnlyWriter.java` | `6902a53592b3038344c7ccf0621da6d1ba01b73434dc3b4be2569b85a7615713` |
| `paimon-core/src/main/java/org/apache/paimon/append/BucketedAppendCompactManager.java` | `b0333fdc50bde7744ff6709697b7618143b8ab0883a35f5c33dc783fdfcaba75` |
| `paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactManager.java` | `07166260f13a702132bd0d30f9c3240918bc9e31c0d7b1f27cd85c2a3e47d792` |

## 10. INFO、终态与诊断契约

保留 `[paimon-stop]` 和原 table/owner/phase/elapsed 字段，增加 attempt、remainingMs、cancelRequested、executorTerminated、prepareReturned、inFlightAction。只能写实际已知值，不用 taskFuture.isDone 冒充 executorTerminated。

控制线程与 close worker 的 INFO 输出使用非阻塞交付：只构造不可变事件并 offer，禁止直接调用可能阻塞的 Log、阻塞 put、CallerRunsPolicy 或等待日志线程结束。使用该 connector 类加载器内共享的一个有界 daemon 分发器，不按表/STOP 新建日志线程；连续 waiting 可以合并。队列满时不阻塞 STOP，记录丢弃计数；每次停止操作的唯一终态另存本地快照供诊断（不依赖日志队列存活），事件入队不等于日志已经送达。后端永久阻塞时不能保证约 5 秒日志可见，但 STOP 判定与结果发布仍必须推进。不能以“catch RuntimeException”冒充阻塞隔离。PaimonConnector.onStop 当前 catch 中的同步 warn 也在接线范围内，统一交给停止诊断出口，避免 Service 已返回后又被日志卡住。终态顺序固定为资源保留登记 → 原子结果发布 → countDown/唤醒 → 非阻塞事件交付；完整异常栈渲染在日志线程进行，不在控制锁或发布前进行。

事件：start、phase-changed、waiting、cancel-requested、cancel-terminated、compaction-discarded、timeout-retained，以及唯一正常或失败终态。timeout-retained 明确“STOP 失败返回；资源/owner保留；需要确认旧进程退出”；不能使用“正常退出”。迟到 worker 仅输出 late-operation-finished 等诊断，不重复发布终态。正常日志后端下等待期间尽力维持约 5 秒 INFO；失败原因保留完整异常链。

## 11. 必须通过的反例与回归门禁

先补会在当前 V1.1 失败的测试，再实施。使用可注入单调时钟/短测试预算和 latch 控制交错，不让测试等待生产 180 秒。真实 Paimon 集成与双 JVM 强制退出仍必需；下表是待执行清单，不是已通过成绩。

| ID | 场景与断言 |
| --- | --- |
| B01 | 正常 final 在预算内完成，业务、identifier、offset 顺序及已有 SYNC 行为不变 |
| B02 | 最终 CompactTask 阻塞且响应中断：记录主动取消、prepare 返回、executor 真终止，整次 final 不提交，正常弃提交并完整清理 |
| B03 | task 忽略中断：cancel 成功/isDone 为真仍不能 close IO；宽限期到后 STOP 失败返回，目录/marker/owner保留 |
| B04 | B03 放行迟到线程：不发生新提交、callback、sync、IO close、owner释放或正常终态；同 JVM 新代仍拒绝 |
| B05 | submit 与取消并发、任务已入队未启动、已出队尚未进入 Callable：无漏网任务，无永久未完成 Future，无新增接收 |
| B06 | get 与 get(timeout) 的授权取消转换成私有 ExecutionException；未进入 get 而再次 trigger 时受控拒绝展开；普通取消/拒绝仍硬失败 |
| B07 | final prepare返回与计时器竞争：COMMIT_ADMITTED 与 CANCEL_REQUESTED 恰有一个胜者；取消获胜即使messages非空也不能提交 |
| B08 | commit已准入但RPC阻塞/响应丢失：总超时返回未知结果，不取消整个worker、不清pending、不重复提交或推进offset |
| B09 | 已存在控制错误/迟到Error/自中断/普通IO异常与主动取消交错：不被授权token洗成正常弃提交 |
| B10 | scheduler、ingress、业务drain、callback或构造卡住：总预算有效，资源注册不遗漏，迟到调用不能进入下一外部动作 |
| B11 | 多表串行final/多次close：总预算不按表和调用者重置，所有调用者复用唯一结果，已清理表与未清理表正确区分 |
| B12 | 控制线程遇到Service/Context monitor被占用：仍可触发取消和保留，不等待同一长锁；终态发布与日志不形成死锁 |
| B13 | writer/committer/IO close失败或阻塞：失败不伪装终止；超时前已获准IO操作可完成，但之后不进入新清理步骤 |
| B14 | 保留后Connector丢弃service引用并触发GC：资源强引用和owner锁仍在，同/另一可见磁盘JVM的cleaner跳过；进程实际死亡后才可回收 |
| B15 | KEY_DYNAMIC与preflight约束目录、全局/表级根、无marker历史目录和SYMLINK保护不回退 |
| B16 | 正常日志后端下等待节拍、取消、失败保留和唯一终态可关联；重复close/迟到worker不打印正常退出；中断标志恢复 |
| B17 | 有限读入口 batchRead/batchCount/getTableCount/discoverTables/timestampToStreamOffset 的并发关闭缺口列为前置准入修复；reader/consumer存活期间不得释放相关共享资源 |
| B18 | 真Paimon文件生成后取消可能留下远端未提交文件：不提交未知final结果、不删除历史快照引用文件；明确后续独立orphan治理边界 |
| B19 | 真内核空 committable/可清理 bucket 分支：任务仍在执行时受控取消必须让 prepare 提前展开，不能进入内联 writer.close、manager.close 或文件删除；覆盖 MergeTree 与 bucketed append |
| B20 | reader.close 抛错/阻塞，入口返回或发生GC后，reader及共享Catalog仍有引用；不得把activeIngress=0当作reader关闭成功；迟到consumer/新reader创建被拒绝 |
| B21 | Log.info 及 Connector.onStop 的 Log.warn 永久阻塞、日志队列满：取消/截止/强引用登记/终态发布均不等待logger，事件不在控制锁内交付，重复STOP结果一致 |
| B22 | 未超时但writer/committer/IO/reader关闭失败：原错误为主因，未关闭对象及未成功释放的目录保护进入FAILED_RETAINED；普通FAILED仅在资源已全部关闭时成立；无重复清理 |

B17 的六个有限入口已统一准入，reader 与 outstanding batch 通过独立资源 scope 保留；具体用例见 §14。长期 StreamRead 的停止协议仍单独设计，不一概套入口 permit。

## 12. 设计反例复核与实施状态

- 只在 Future.get 增加 timeout：无法覆盖 prepare flush/commit/close 卡住，且内核 get 可吞 cancel；已由独立监督和总预算解决设计遗漏。
- 只调用 shutdownNow：队列移出的 Future 可能未取消、执行线程仍活跃；已要求任务登记、队列Future取消、实际termination。
- 在 Context synchronized 方法中取消：与正在阻塞的 prepare 相互等待；取消控制走独立短锁。
- 取消后允许 prepare 返回的纯 Compaction messages 提交：会与定时器竞争；已要求唯一 final decision 和外部动作准入。
- STOP 超时后后台继续 finally 清理：会恢复旧生命周期问题；已要求不可逆保留、每个阶段检查和静态强引用。
- executor结束就关资源：漏掉prepare/业务/读线程；已要求全部访问者证明，及 B17 前置覆盖。
- 把正常结束的任务全部存入静态集合：产生不必要泄漏；仅保留实际失败资源及未实际退场任务，不记录成功历史。
- 凭此宣称A/B安全接管：本地协议无分布式能力；明确列为未解决的外部条件。

本轮完成的是源码事实核对、审查修订与实施规划；未修改生产/测试 Java、未执行 V1.2 回归、未提交。计划与任务清单按用户确认保存至模块 tasks 目录。预算默认值尚待确认；实现前须把 B01–B22 转为可执行门禁并复核本节假设。不得以“源码同字节”或历史636项通过替代取消协议验证，也不承诺硬实时退出或跨机器绝无双写。

## 13. 本轮五维审查与准入结论（2026-09-07）

> 本节保留实施前设计审查快照；R01–R07 的 Connector 行号为基线位置，当前实现定位与关闭状态见 §14。56 项是设计阶段基线成绩。

审查范围是 V1.2 设计及其依赖的当前 Connector/Paimon 实现；本轮没有新增生产代码。独立审查使用不同模型复核正确性与架构。初轮结论为 Request changes，以下问题已写回规范并经复核收敛；结论只升级为“可进入实施规划”，不授予新功能的代码合并/发布通过。

| ID / 严重度 | 已核实问题与具体位置 | 本次文档修正 / 实施门禁 |
| --- | --- | --- |
| R01 / Critical | PS05 的 prepare 内联关闭 + PS10 吞取消可能绕过外层 termination 屏障；PS08/PS09 确认 ExecutionException 可传播 | §5 指定私有取消异常转换，B06/B19 验证不能提前内联 close；当前代码尚无受控取消，不把潜在 V1.2 回退说成当前新增事故 |
| R02 / Required | [batchRead:2878](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java:2878)、[batchCount:3059](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java:3059)、[query:3195](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java:3195) 的 reader.close 失败只告警；B17 的 permit 不足以证明读资源关闭 | §7 增加读资源账本/占位与失败保留，B17/B20 验证；生产缺口列入前置任务 |
| R03 / Required | [Context:409](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContext.java:409) 关闭失败可保留 IO，但 [onStop:97](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/PaimonConnector.java:97) 清空 service；仅 owner token 不强引用所有资源，[Factory:244](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContextFactory.java:244) 的异常也不携带局部资源 | §3/§7 的 FAILED_RETAINED 覆盖超时及任意清理证明缺失；保留构造资源，B14/B22 验证 |
| R04 / Required | [Service:3436](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java:3436) 同步 Log 可阻塞；[publishCloseCompletion:3441](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java:3441) 在 countDown 前打印，onStop 另有同步 warn | §10 固定非阻塞日志交付及终态发布顺序，B21 验证；只承诺正常后端下 INFO 可见 |
| R05 / Required | 当前 final prepare → audit → commit 连续执行，外部增加 timer 会与 [closeForStop:337](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContext.java:337) 竞争；Service/Context monitor 均可被长 IO 占用 | §4/§6 集中控制门禁、单胜者和锁纪律，B05/B07/B08/B12 验证；不得堆砌无锁检查后盲写 |
| R06 / Required | [PaimonCommitStateStore:35](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/commit/PaimonCommitStateStore.java:35) 仅持久化 commitUser/nextIdentifier；[Context:28](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContext.java:28) 的 pending messages 仅在内存 | §6 区分进程内精确保留与跨进程 snapshot 对账，不承诺恢复未持久化的 messages/offset；B08/B18 按此边界验收 |
| R07 / Required | [releaseAfterClose:265](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/util/PaimonSpillDirCleaner.java:265) 在 deleted=false 时也移除live/文件锁引用；[OwnerLock.close:529](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/util/PaimonSpillDirCleaner.java:529) 吞关闭异常 | §7 禁止把该helper原样用于V1.2失败保留；T12固定目录保护释放证明，T13/T14消费结果，B13/B14/B22验证 |

五维结论：正确性按上述反例收敛；可读性/架构要求提取一个 StopController 并替换旧协调状态，不在大型 Service 中再铺设计时分支；安全性保留目录 marker、owner、SYNC 与未知提交保护；性能使用有界日志和未退场任务登记，不新增依赖、每表线程或历史任务无界集合。资源强保留是失败隔离成本，不宣称代码量一定少于 V1.1。

本轮执行的验证：

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home \
  mvn -o -B -pl connectors/paimon-plus-connector -am -DskipTests=false \
  -Dtest=PaimonCompactionExecutorTest,PaimonFinalCompactionTest,PaimonServiceCloseTest,PaimonCompactionSpillLifecycleIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：4 个测试类，56 项、0 失败、0 错误、0 跳过，5 个 reactor 项目成功；结束 2026-09-07 11:31:56 +08:00，耗时 28.590 秒。日志：`/tmp/paimon-stop-v12-review-tests.log`。这是当前 V1.1 基线测试，包括真实 Spill 删除竞态 fixture；没有执行新 V1.2 回归，也没有重跑全量 clean package。§9.2 原生探针单独成功。

当前实施：[实施计划](../../../tasks/plan.md)、[任务清单](../../../tasks/todo.md)。实现采用可配置的 180/120/30 秒默认预算，执行证据在下节维护。

## 14. V1.2 实现与验证记录（2026-09-07）

本节对应 `962083ff` 基线上的工作树实现。没有修改 Paimon 内核、Engine、依赖版本或预先暂存的 StreamRead Spec。最终 CompactTask 的受控取消可正常弃提交；总超时或任一资源关闭证明缺失返回 FAILED_RETAINED，资源静态强保留到进程退出。旧无限 `shutdownAndAwaitCompletion` 已移除。

| 职责 | 当前代码入口 | 实现事实 |
| --- | --- | --- |
| 监督与全 Service 总预算 | [PaimonService](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java:3264) | 唯一 close worker；调用者轮询、主动取消、期限到后强保留与失败返回 |
| 最终决策线性化 | [PaimonStopController](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonStopController.java:165) | 同一短 gate 决定 COMMIT_ADMITTED / CANCEL_REQUESTED；不在锁内进入外部 IO |
| 原生取消适配 | [PaimonCompactionExecutor](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonCompactionExecutor.java:76) | 登记未实际退场任务；取消 get 私有异常展开原生 prepare；迟到硬错误独立审计 |
| 清理证明 | [PaimonTableWriteContext](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContext.java:354) | prepare 展开 + executor 真终止 → sync → writer → committer → IO →目录保护；失败短路并保留 |
| 构造和失败资源强引用 | [PaimonStopResources](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonStopResources.java:25) | 分配前占位，返回后先 bind 再检查 frozen；Service root 在终态发布前进入静态保留 |
| 有限读和 outstanding batch | [PaimonGuardedReader](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonGuardedReader.java:7) | batch、reader 关闭成功才脱离 scope；任何关闭证明缺失都保留共享 Catalog |
| 目录与文件锁释放 | [PaimonSpillDirCleaner](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/util/PaimonSpillDirCleaner.java:279) | ReleaseResult 明确完整/残留/异常；注册失败只保留，deleted=false 不释放锁 |
| 动态桶预检 | [PaimonDynamicBucketPreflight](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonDynamicBucketPreflight.java:85) | snapshot、index open/bootstrap/end、reader 与 rollback 分步准入；索引约束目录保持 |
| 停止日志 | [PaimonStopLog](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonStopLog.java:10) | 类加载器内共享一个 daemon + 256 队列，非阻塞 offer；终态不依赖日志输出 |

有限入口 `getTableCount`、`discoverTables`、`timestampToStreamOffset`、`batchRead`、`batchCount`、`queryByAdvanceFilter` 全部持有 ingress。Paimon RecordReader.close 的接口承诺是释放所有资源；本适配器额外跟踪 outstanding batch，在提前结束或异常路径明确释放，避免无法证明的迭代器状态被丢弃。此措施是 connector 的证明约束，不声称 Paimon 接口没有 close 契约。

R01–R07 已实现。独立复核的后续问题——注册失败错误删除、outstanding batch 丢失、owner/context/coordinator 非原子发布、四处 snapshot 文件系统调用漏准入——均已修复并复核。最终生产源码复核未发现确定性 Critical/Required；该复核不替代运行测试。

完整验证命令：

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home \
  mvn -o -B -pl connectors/paimon-plus-connector -am -DskipTests=false clean package
git diff --check
```

最终完整构建通过：2026-09-07 16:34:37 +08:00，JDK 17、Maven 离线 `clean package`，5 个 reactor 项目成功；paimon-plus-connector surefire XML 共 **62 个测试类、688 项，0 failures、0 errors、0 skipped**。耗时 2 分 15 秒。日志 `/tmp/paimon-review-fixes-full.log`，报告 `connectors/paimon-plus-connector/target/surefire-reports`。

测试只在本地 Catalog/FileIO、受控故障注入和真实子 JVM 上运行。生产 S3/MinIO、真实用户数据规模与 Engine A/B 调度未执行验收。取消后可能残留的远端 orphan 文件不在 STOP 中自动删除，不将 Snapshot expiration 当成 orphan 清理。

| 门禁 | 实际测试类 / 方法（均位于本模块 src/test/java） | 断言范围 |
| --- | --- | --- |
| B01 | `PaimonFinalCompactionTest；PaimonFinalCompactionIntegrationTest；PaimonServiceSyncStopIntegrationTest` | 正常 final、业务/identifier 顺序及 SYNC 回归 |
| B02 | `PaimonBoundedStopTest.cooperativeCancellationMustDiscardFinalAttemptAndCloseOnlyAfterExit` | 受控取消、prepare 展开、实际终止后正常弃提交 |
| B03/B04 | `PaimonBoundedStopTest.ignoredInterruptMustKeepSpillAndNeverCloseAfterLatePhysicalExit；PaimonServicePhysicalTableOwnerTest` | 忽略中断仍保留；迟到线程不清理；同 JVM owner 互斥 |
| B05 | `PaimonCompactionExecutorTest.submitRacingAuthorizedCancellationMustLeaveNoAcceptedOrQueuedWorkBehind / dequeuedTaskNotYetInsideCallableMustStillBeCancelledAndActuallyExit` | 50 轮竞争、队列及已移交 Worker 尚未进入 Callable 的窗口 |
| B06 | `PaimonCompactionExecutorTest.controlledCancellationMustUnwindNativeGetButStillWaitForPhysicalExit；同类 submit race` | get/get(timeout) 取消来源、再次 submit 受控拒绝 |
| B07 | `PaimonStopControllerTest.commitAndCancellationMustHaveOneWinnerUnderConcurrentThreads；PaimonBoundedStopTest.cancellationWinningAfterNonEmptyPrepareMustNeverCommitMessages` | 50 轮决策竞争；非空结果不能绕过取消 |
| B08 | `PaimonBoundedStopTest.admittedFinalCommitMustKeepExactPendingAndRefuseLateStateSaveOrRetry；PaimonTableWriteContextTest` | RPC 结果不明、精确 pending、禁止迟到重试/状态推进 |
| B09 | `PaimonCompactionExecutorTest.authorizedCancellationMustNotHideLateErrorOrIndependentIOException；该类旧控制错误测试` | Error、独立 IO、自中断/外部取消不能洗成成功 |
| B10 | `PaimonBoundedStopTest.blockedSchedulerMustConsumeTotalBudgetWithoutEnteringResourceCleanup / blockedBusinessPrepareMustNotStartCommitOrFinalPrepareAfterDeadline / admittedCallbackMustRemainUnconfirmedAfterTotalDeadlineAndLateReturn / allocationReturningAfterTimeoutMustBindToRetainedLedgerAndRejectNextAction` | scheduler、业务 drain、callback、半构造；有限读测试补 ingress |
| B11/B12 | `PaimonStopControllerTest.totalBudgetIsNotResetAndRetainedOutcomeCannotBecomeSuccess / frozenStatePublicationMustBeRejectedAfterWaitingForApplicationLock / finalRegistrationWindowsMustNotBlockTimeoutOrPermitLatePrepare；PaimonServiceCloseTest` | 共享总预算、结果、长锁、两种 final 注册窗口；多表顺序由 Service 测试覆盖 |
| B13/B22 | `PaimonCompactionLifecycleTest；PaimonTableWriteContextTest；PaimonSpillDirCleanerTest；PaimonDynamicBucketPreflightCleanupTest；PaimonBoundedStopTest.catalogCloseTimeoutMustReturnRetainAndRefuseLatePublication` | 关闭失败短路、目录锁保留、总超时与原始失败主因 |
| B14 | `PaimonSpillDirCleanerProcessIntegrationTest；PaimonStopControllerTest.retainedRootMustKeepLateBoundScopeAliveAcrossGcAndRepeatedCompletion；PaimonBoundedStopTest.connectorStopFailureMustNotInvokeBlockingWarnAndMustKeepRetainedServiceReachable` | Connector 丢引用、GC 强达、另一 JVM cleaner 跳过及真实进程退出后回收 |
| B15 | `PaimonSpillDirCleanerTest；PaimonServiceStaleSpillCleanupTest；PaimonTableWriteContextFactoryTest；KeyDynamicBucketWriterStrategyTest` | 限定 RocksDB 根、当前全局/表级根、marker/symlink/live/owner 防护 |
| B16/B21 | `PaimonServiceCloseTest；PaimonServiceSyncStopIntegrationTest；PaimonBoundedStopTest.blockedLogBackendAndFullQueueMustNotBlockStopOrSpawnPerServiceThreads / connectorStopFailureMustNotInvokeBlockingWarnAndMustKeepRetainedServiceReachable` | INFO 等待/唯一终态，后端阻塞和队列满不拖住监督者 |
| B17/B20 | `PaimonFiniteReadLifecycleTest（12 项）` | 三个有限元数据入口、batchRead/batchCount/query reader 失败、batch 释放失败、迟到创建和 callback |
| B18/B19 | `PaimonCompactionSpillLifecycleIntegrationTest（6 项）；PaimonFinalCompactionIntegrationTest；PaimonCommitStateStoreTest` | 真实 MergeTree/Append prepare 取消、未提前内联 close、原 snapshot 文件不被误删；恢复仍按稳定用户对账 |

测试类做了合并，Plan 中原拟建的 `PaimonFinalStopDecisionTest`、`PaimonStopResourcesTest`、`PaimonStopLogDispatcherTest`、`PaimonNativeStopCancellationIntegrationTest` 等并未单独创建；对应断言实际位于上表，不能按旧类名声称执行。

## 15. 用户生产 DDL 的核对与回归

目标表 `dl_ods.ods_opera_vision.future_inhousedate`：11 字段，主键为 ConfirmNo/InhouseDate/Resort/ExtractionDate/ExtractionHour/pt_extractiondate，分区字段 pt_extractiondate，bucket=-1。完整字段、类型和 28 个表配置项（不含生产 path）保存为测试事实来源：

[FutureInhouseDateFixture](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/FutureInhouseDateFixture.java:15)

| 用户配置/结构 | Paimon 1.3.2 源码事实 | 本次处理 |
| --- | --- | --- |
| 主键包含分区字段，bucket=-1 | TableSchema.crossPartitionUpdate=false，KeyValueFileStore.bucketMode=HASH_DYNAMIC | 按真实 HASH_DYNAMIC 执行；不能套 KEY_DYNAMIC writer RocksDB 分析，但本 connector 的历史污染预检仍会用 GlobalIndexAssigner/RocksDB |
| snapshot.expire.execution-mode=async | TableCommitImpl 根据该选项选择维护 executor | 与本 connector SYNC 唯一契约冲突；写资源分配前拒绝，不静默改表。生产使用前需将有效表选项设置为 SYNC |
| write-buffer-spillable=true，128mb buffer，spill disk=5gb | 写缓存 Spill 与 MergeSorter Compaction Spill 是两条路径 | 参数不是生命周期屏障，5gb 不能证明不存在 Compaction 临时文件或任务已经退出 |
| sort-spill-threshold=10，sort-spill-buffer-size=64mb | MergeSorter.mergeSort 在输入 reader 数量 >10 时进入 spillMergeSort/spill | 真实回归保持这两个生产值，断言 MergeSorter.spill 栈与受保护目录 |
| compaction-trigger=20，optimization-interval=60min | UniversalCompaction 先查 FullCompactTrigger；其 lastFullCompaction 为空且多 run 时可立即合并 | 首次不保证等待 60min，也不保证等到 20 run；测试先确认初次两 run 合并，再产生 11 个文件供显式 full compact |
| commit.force-compact=false，changelog-producer=none | 不要求每次业务提交等待最终合并 | 保留业务 prepare(false)；STOP 在业务确认后执行 final prepare(true)，允许有来源证明的最终弃提交 |
| num-sorted-run.stop-trigger=2147483647 | 放大写入停止阈值 | 不能作为 STOP 等待期限或安全删除依据 |
| sink.parallelism=1，initial-buckets=1，max-buckets=50 | Flink 并行度参数不等于本 connector 的跨 JVM 排他所有权 | 不将它解释为 A/B Engine fencing |

这份 DDL 能确认配置及可达调用链，不能独自证明生产 `.channel` 文件由谁删除。原故障归因仍区分“源码与回归可复现的竞态”和“生产环境的实际删除者”。

### 15.1 新增固定源码证明

以下三个文件均在本轮逐字节核对：本地 Paimon HEAD、Maven 1.3.2 sources JAR 与官方 release-1.3.2 固定提交原文一致。官方 tag peeled commit 已用 git ls-remote 核验为 c05f7d1f1b1e5d37e64edab0f2978124d90b64f7；本地 Git 未缓存该远端 commit object，因此使用官方 raw 原文比对，未将本地 git show 失败误记为源码不一致。

| 记录 | 本地精确位置与命题 | SHA-256 |
| --- | --- | --- |
| PS16 | [TableSchema.crossPartitionUpdate:200](/Users/SL/javaProject/paimon/paimon-api/src/main/java/org/apache/paimon/schema/TableSchema.java:200)：主键包含全部分区键则返回 false | `ab695e219937e1852cf5abb4bede863747c6168cb3f3320ba8568772fef2eefd` |
| PS17 | [KeyValueFileStore.bucketMode:100](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java:100)：bucket=-1 且非 crossPartitionUpdate 返回 HASH_DYNAMIC | `02ddb150e59cf1c2ed3922453b15bed746749aa2703951d18a94b77c5c914828` |
| PS18 | [FullCompactTrigger.tryFullCompact:64](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/FullCompactTrigger.java:64)：lastFullCompaction=null 可立即触发 | `1d23475acf8f661c8436583aa085e5d421c642aa7aec4f7329da958a0b7ef795` |

官方固定原文：[TableSchema](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/schema/TableSchema.java#L200)、[KeyValueFileStore](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java#L100)、[FullCompactTrigger](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/FullCompactTrigger.java#L64)。Compaction Spill 条件见 [MergeSorter:110](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeSorter.java:110)；写路径的该文件与 1.3.2 source JAR 一致的证明保留在 §9。

### 15.2 三项实际回归

1. `PaimonServiceDynamicBucketIntegrationTest.reportedProductionDdlAsyncMustFailBeforeWriterOwnerOrSpillAllocation`：使用真实表 schema，确认 HASH_DYNAMIC；ASYNC 在 writer/owner/Spill 分配前拒绝。
2. `PaimonServiceDynamicBucketIntegrationTest.reportedProductionDdlSyncMustWriteAndReadPartitionedCompositeKey`：SYNC 下经 Service 写入，检查复合主键、DATE/DECIMAL/TIMESTAMP/分区字段，停止后临时根清空。
3. `PaimonCompactionSpillLifecycleIntegrationTest.reportedProductionDdlMustCancelRealHashDynamicSpillWithoutEarlyDeletion`：保持生产字段和全部上述表参数，仅 ASYNC→SYNC，Catalog path 换成本地临时路径。写入 12 个版本，确认初始两 run 合并后有 11 文件；显式 full compact 进入真实 MergeSorter.spill，STOP 受控取消后 prepare 展开但 IO 未关闭，实际线程退出后才清理；原业务 snapshot 及 11 个有效文件不被 final 提交改动。

测试显式 compact 是故障注入的触发器，不是生产新增的“每次 STOP 强制全量 compact”行为。测试使用 1 秒 final 预算加速触发取消；生产 Service 默认仍为 180/120/30 秒。没有连接用户的生产 S3 URI。

## 16. 审查缺口修复（2026-09-07）

本节是 V1.2 的收尾修正。保留原生外部调用的逐次准入、实际 executor 终止屏障、SYNC 限制和 FAILED_RETAINED 强保留，不改变业务提交与恢复语义。

| 缺口 | 修复后的行为与入口 | 回归证据 |
| --- | --- | --- |
| 逐行 gate 重复登记 | [Context.write](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContext.java:531) 删除整行 scope.run；各 bucket 策略负责原生调用准入。HASH_DYNAMIC 一行从 3 次登记降到 2 次（assign + write）。[checkAction](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonStopController.java:98) 在 RUNNING 的纯检查走 volatile 快路径，实际 call/run 仍在 gate 内检查并登记 | HashDynamicBucketWriterStrategyTest.freezeAfterAssignmentMustRejectTheNextNativeWrite；全部 bucket 策略回归 |
| worker 中断恢复 | [performClose](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java:3314) 最外层 finally 恢复 worker 曾捕获并暂清的中断；caller 仅恢复自身中断。Context 捕获的次要 InterruptedException 也不因首因不同被遗漏 | BoundedStopTest.schedulerInterruptionMustBeRestoredOnlyOnCloseWorker；ServiceCloseTest.interruptedRetryOnCloseWorkerMustFinishCleanupWithoutInterruptingCaller |
| 重复 suppressed | [PaimonFailures.append](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/util/PaimonFailures.java:9) 保留首因，跳过 self/null/已存在的同一异常对象；Service、Context、preflight、Factory、bucket/目录清理共享策略 | StopControllerTest.repeatedSecondaryFailureMustBeSuppressedOnceAndPreserveBusinessCause |
| 清理根字符串去重 | [collectTmpDirRoots](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java:360) 先拒绝配置根末级本身为符号链接，再取 canonical path；解析失败只跳过该根并记录 WARN | ServiceStaleSpillCleanupTest.canonicalAliasesMustDeduplicateWithoutFollowingSymlinkRoots；原表级根清理测试 |
| 裸 rocksdb 负例 | 即使裸 rocksdb-orphan 含旧数据且旁有 owner marker，前缀拒绝仍保证其数据与 marker 不被 cleaner 删除；不能根据名称猜测历史裸目录归属 | SpillDirCleanerTest.bareRocksdbMustRemainEvenWithOldOwnerMarker |
| 混合缩进 | 本轮触及 Java 文件的行首统一空格，保留字符串中的字符；Service 的大量 diff 含此机械变更，可配合 git diff -w 阅读 | git diff --check；全量重新编译 |
| INFO 字段与事件 | [diagnostics](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonStopController.java:281) 补齐六个字段；executor 状态读取在控制锁外，只作为观察；prepareReturned 只在实际原生 prepare 调用的 finally 发布。终态保存不可变诊断字符串；超时打印 timeout-retained 与“需要确认旧进程退出” | StopControllerTest.diagnosticsMustDistinguishPrepareReturnFromExecutorTermination；BoundedStopTest.timeoutMustLogImmutableDiagnosticsAndProcessExitInstructionOnce；ServiceCloseTest 等待 INFO |
| worker 抢先遇到期限 | [timeoutFailure](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonStopController.java:205) 缓存专用超时异常，带 phase/table/owner/耗时及三种预算；checkAction 到期抛此异常。已有业务首因优先，超时可作为次因，重复 close 不改终态 | StopControllerTest.workerDeadlineMustReportSameTimeoutWithBudgetsAndDeduplicateObservations |
| reader.close 覆盖读取首因 | batchRead、batchCount、query 使用 try-with-resources；read A + close B 对调用者保留 A 并 suppressed B。资源账本仍保留关闭失败 B 及资源，不用 A 替换资源证明缺失这一原因 | FiniteReadLifecycleTest.readAndCloseFailureMustKeepReadCauseWithSuppressedCleanup（三入口） |

清理根保持原 cleaner 的路径语义：拒绝配置根末级本身为符号链接，不新增禁止祖先路径别名的规则（例如 macOS `/tmp`、`/var`）。canonicalize 合并原本会扫描到的同一实际目录，不增加新的扫描根。异常去重针对同一首因下重复附加的直接 suppressed 对象，不声明重写调用方预先构造的任意 Throwable 图。

HASH_DYNAMIC 的 gate 减少是静态调用链计数，不宣称未经基准测试的吞吐提升。STOP 到期后，已准入的 assign 可以迟到返回，但下一次 native write 必须被拒绝；没有把多行合并成一张长期许可。

```mermaid
flowchart LR
    C[Context.write 状态检查] --> S[Strategy.write 状态检查]
    S --> G1[gate 登记 assign]
    G1 --> A[HashBucketAssigner.assign]
    A --> E1[gate 移除 assign]
    E1 --> G2[gate 重新准入 write]
    G2 --> W[TableWriteImpl.write]
    W --> E2[gate 移除 write]
    F[STOP 超时或冻结] -. 拒绝下一次准入 .-> G2
```

### 16.1 固定内核证据与适配边界

以下文件已逐字节验证：本地 `/Users/SL/javaProject/paimon`、Maven 1.3.2 sources JAR、官方固定 commit `c05f7d1f1b1e5d37e64edab0f2978124d90b64f7` raw 原文三者一致。

| 本地源码位置 | 证明和 Connector 适配 | SHA-256 |
| --- | --- | --- |
| [ExceptionUtils.firstOrSuppressed](/Users/SL/javaProject/paimon/paimon-common/src/main/java/org/apache/paimon/utils/ExceptionUtils.java:284) | 内核保留首因并避免 self-suppression；内核不去重反复传入的同一次因。Connector 为多层 STOP 观察增加 identity 去重 | `97f974e832d2c2ea8135ee6037a39941fa7c21768c6db4b53d65373c6837fef6` |
| [ExecutorUtils.gracefulShutdown](/Users/SL/javaProject/paimon/paimon-common/src/main/java/org/apache/paimon/utils/ExecutorUtils.java:63) | 内核捕获 InterruptedException 后恢复当前线程标志。Connector 为继续在有限预算内取得关闭证明，暂清标志并在 worker 最外层 finally 恢复；没有照搬 shutdownNow 后即返回作为删除证明 | `cb6264c221dd94951fcf3aeecff5f7fe95c98d467c7b637a3486c5f19d21fb35` |
| [TableWriteImpl.write](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java:157) | write(row) / write(row,bucket) 分别委托原生写入；Connector 的 STOP gate 是自有协议，放在实际原生调用边界，非 Paimon 提供的 gate API | `ad17a681f7c1ca37d0f5b769504402ca743fe9497147db31df6847e4010422c4` |

官方固定源码：[ExceptionUtils](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-common/src/main/java/org/apache/paimon/utils/ExceptionUtils.java#L284)、[ExecutorUtils](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-common/src/main/java/org/apache/paimon/utils/ExecutorUtils.java#L63)、[TableWriteImpl](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java#L157)。

### 16.2 验证记录

2026-09-07 16:34:37 +08:00，JDK 17 离线 reactor `clean package` 成功，5 个项目成功；模块 **62 类、688 项，失败/错误/跳过均 0**。相比上一轮 677 项增加 11 项，并增强原中断与日志测试。日志 `/tmp/paimon-review-fixes-full.log`。最初定向编译发现一处 reader try-with-resources 转换遗漏，修正后 81 项定向测试及上述完整构建通过；不把首次失败算作通过。

独立增量复核已完成：未发现本轮指定修复的确定性 Critical/Required 回归；复核中的父路径 symlink 与异常反向附加两项初始疑点，经基线/可达性核对后撤回。末级 symlink 的精确语义已在本节明确。

本轮没有生产 S3/MinIO、真实规模吞吐或 Engine 跨机器接管验收。静态 RETAINED 无 TTL、关闭失败后保留其后资源、长期 StreamRead 独立停止协议等既定边界保持不变。

---

<details>
<summary>V1.1 历史契约、内核证明与验收档案（非 V1.2 的等待/取消规范）</summary>

## V1.1 历史：Paimon Spill 简化与同步优雅停止

> Spec ID：`paimon-spill-sync-graceful-stop`；版本：V1.1；日期：2026-09-05。
> 范围：`connectors/paimon-plus-connector`，固定 Apache Paimon 1.3.2。
> 状态：V1.1 连接器实现及 A/B Spill 补充验收完成，完整 clean package 629 项通过；Engine 写入交接发现 2 项未修复阻断，见 P17。审查修正与证明边界见[最终修复对比与生命周期总览](../reviews/SPILL-修复前后对比与生命周期总览.md)。
> 本文获批后，替代旧 Safety、Hotfix、Simplification、Current Spec 中的停止、执行器等待、retained/reaper 和重启放行契约。旧文档及其中的测试报告保留为历史证据，不能证明本文已实现。其他既有写入、恢复、目录保护和 Hadoop FileIO 契约继续有效。

## 1. 目标与已确认决策

目标是修复连接器自身的 Spill 生命周期删除竞态，并通过只支持 SYNC 和完整等待 Compaction，删除原来为超时逃逸服务的状态、后台回收线程及所有权分支。使用者是 TapData Paimon 任务的开发与运维人员。

| 决策 | 确定行为 |
| --- | --- |
| Snapshot expiration | 只支持实际生效的 `snapshot.expire.execution-mode=SYNC`；明确拒绝 ASYNC |
| 停止等待 | 必须等到 Compaction 实际执行结束，没有 30 秒或其他总等待时限 |
| Compaction 成功 | 收集最终结果，按既有提交与恢复协议确认 Snapshot 提交，然后清理并正常退出 |
| Compaction 失败 | 仅限业务 drain 已成功后的最终停止阶段：放弃该表这次最终 Compaction 提交，等待全部任务实际退出并完成资源清理，然后 INFO 记录正常退出 |
| 其他失败 | 连接器可观察的业务写入、业务 drain、提交结果不确定、状态持久化、offset callback、资源关闭等错误保持失败语义，不能套用上述豁免；Paimon 内部维护错误的既有传播限制见 §4 |
| 同进程重启 | 旧 Context 完整关闭前保留物理表 owner；不能让旧 Compaction 与新代 Context 并存 |
| 日志 | 开始、阶段切换、每 5 秒等待进度、结果及总耗时使用 INFO；失败原因保留异常链 |
| 实施范围 | 修改连接器，引用并适配 Paimon 1.3.2 原生机制；不修改 Paimon 内核、业务路由、offset 协议或依赖版本 |

“完成 Compaction”是等待已有任务以及本次最终 `prepareCommit(true, identifier)` 按原生策略触发的任务结束。它不要求强制全量合并所有文件，也不要求循环触发 Compaction 直至 LSM Tree 只剩一个文件。

本文不增加停止超时配置。永久阻塞时持续等待和输出进度；若宿主强杀进程，则属于非优雅停止，不能输出本协议的正常退出结果。

## 2. 事实基线与事故证据

### 2.1 版本边界

| 项目 | 本次核对值 |
| --- | --- |
| Connector Git HEAD | `30c8ae4021154680b2ccb627b5576152661f10d7`，工作区已有未提交修改，分析以实际文件为准 |
| Connector 依赖 | Paimon `1.3.2`、Hadoop `3.3.6`、Engine 提供 RocksDB JNI `7.3.1` |
| 本地 Paimon HEAD | `/Users/SL/javaProject/paimon`，`76711fc8e0f3d474e628eb7b7fc7bcdec92d2066` |
| 源码校验 | 本次核对的 24 个关键 Java 文件与本机 Maven 缓存中 Paimon 1.3.2 对应 sources JAR 内容逐字节一致；完整复核命令及证明记录见 §14 |
| core sources JAR SHA-256 | `f8c6d7b57543fb1115dfbbeed1ce0f598d8322f2601c30838f983f64b7ddae63` |

24 个核对文件：`CoreOptions`、`HadoopFileIO`、`TableWriteImpl`、`TableCommitImpl`、`AbstractFileStoreWrite`、`CompactFutureManager`、`CompactTask`、`MergeTreeWriter`、`MergeSorter`、`MergeTreeCompactManager`、`MergeTreeCompactTask`、`AppendOnlyWriter`、`BucketedAppendCompactManager`、`RecordWriter`、`DynamicBucketIndexMaintainer`、`IOManagerImpl`、`FileChannelManagerImpl`、`PostponeBucketWriter`、`PartitionExpire`、`FileStoreCommitImpl`、`DataIncrement`、`FileRewriteCompactTask`、`CommitMessage`、`CommitMessageImpl`。这不代表整个本地 Paimon 分支等同于 1.3.2。

### 2.2 附件证明了什么

- [完整异常栈](/Users/SL/.codex/attachments/bb6a19d8-a47e-4f62-a5e7-edaf6074a547/pasted-text.txt)显示 `pr_operators` 在 `MergeTreeCompactTask → MergeTreeCompactRewriter → MergeSorter.spill → IOManagerImpl.createBufferFileWriter → RandomAccessFile` 链路上发生 `FileNotFoundException`。
- [TapData 日志](/Users/SL/Downloads/TapData日志.txt)显示 `2026-09-01 00:46:19` 起出现 channel 打开失败，随后出现 ingress fencing、停止报错与任务重启。该异常已在这次停止清理之前出现，不能据此断言这一次 close 就是删除者。
- 两张截图分别显示 channel 路径不存在的任务错误，以及对应 `paimon-io-*` 父目录已经不存在的 `ls` 结果。

附件证明了 Compaction 正在访问不存在的 Spill 路径，没有给出删除者身份。本文针对源码可以证明、测试能够确定重现的连接器内部 use-after-delete 竞态建立约束；不把“目录缺失”直接推断为某个 cleaner、外部脚本或操作人员所为。附件内容只作为故障数据。

修复不能通过捕获 `FileNotFoundException` 后重建同名目录来继续写入：已经丢失的 sorted runs 无法靠 `mkdir` 恢复。外部删除活跃目录、磁盘故障、进程强杀不在“保证不发生目录删除竞态”的控制范围内；出现此类问题仍须按发生阶段报告真实结果。

## 3. Spill 调用链与资源所有权

```mermaid
flowchart TD
    A[PDK 写入 / Scheduler / DDL] --> B[Service 生命周期准入与表锁]
    B --> C[Context 与 BucketWriterStrategy]
    C --> W[TableWriteImpl / FileStoreWrite]
    W --> M[Bucket RecordWriter]
    M --> BUF[内存缓冲区 BinaryExternalSortBuffer]
    BUF --> S1[前台写入 Spill：有序 runs 落盘]
    M --> E[连接器持有的 Compaction Executor]
    E --> T[CompactTask / MergeTreeCompactTask]
    T --> S2[MergeSorter：多路归并中的 Spill]
    C --> IDX[动态桶索引 bootstrap / preflight]
    IDX --> S3[外部排序 Spill；RocksDB 另有自身资源]
    S1 --> IO[Context 的 IOManager]
    S2 --> IO
    S3 --> IO
    IO --> DIR[本地 paimon-io-UUID / channel 文件]
    T --> R[CompactResult]
    R --> M
    W --> P[prepareCommit 收集 CommitMessage]
    P --> COM[StreamTableCommit：Snapshot 提交]
    COM --> SYNC[SYNC maintenance：在提交调用线程执行]
    COM --> FS[表存储 FileIO]
    SYNC --> FS
```

1. **前台缓冲 Spill**：内存排序产生本地 sorted runs，通过 IOManager 管理 channel；它与前台 write、flush、prepare 生命周期绑定。
2. **Compaction Spill**：`MergeSorter` 在读取多个 sorted runs 时按阈值落盘，运行于 Compaction 执行器。关闭 `write-buffer-spillable` 不等于关闭这条 Spill 路径。
3. **动态桶初始化 Spill**：索引 bootstrap/preflight 也可能使用外部排序及 IOManager。关闭时必须先排空前台准入，不能只等 Compaction。
4. **提交与维护**：Compaction 生成文件及结果，Snapshot 可见性由 Committer 决定。维护包含 snapshot/partition/tag 操作；部分维护具有提交能力，不能等同于无害的本地删除。
5. **本地删除所有权**：IOManager 关闭会递归删除其 `paimon-io-*` 目录。目录 live 登记、owner 文件锁和 stale cleaner 负责保护删除边界；它们不替代线程终止证明。

## 4. Paimon 内核依据与不能照搬的行为

| 源码 | 核对结论与设计约束 |
| --- | --- |
| [CoreOptions](/Users/SL/javaProject/paimon/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java:435) | expiration 默认 SYNC，但默认值不能替代最终配置校验 |
| [TableCommitImpl 构造与维护](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:117) | SYNC 使用 direct executor；维护在提交调用内执行，准入 drain 可覆盖其生命周期 |
| [TableWriteImpl](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java:134) | 支持注入 Compaction executor；注入后关闭权归调用方 |
| [AbstractFileStoreWrite.prepareCommit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:185) | 按 bucket 逐个收集并消费 increment；中途失败没有返回部分成功的完整列表，不具备原子 prepare 语义 |
| [MergeTreeWriter.prepareCommit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:252) | `waitCompaction=true` 等待并收集结果；其 flush 仍可能触发新任务，不能先 shutdown executor 再调用 |
| [AppendOnlyWriter.prepareCommit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/append/AppendOnlyWriter.java:221) | 同样存在 flush、触发和等待阶段，不能只覆盖主键表 |
| [CompactFutureManager](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java:47) | 原生等待使用 `Future.get()`；仅吞掉 CancellationException，ExecutionException 传播；finally 清空 `taskFuture` |
| [MergeTreeWriter.close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:343) | 原生 close 先 cancel 再 sync；cancel 不是工作线程结束证明，不能直接据此删除共享 Spill 目录 |
| [AbstractFileStoreWrite.close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:304) | 不等待原生 executor 终止；某个 bucket close 抛错会中断后续关闭，失败路径须先消费所有已结束的 Compaction Future |
| [FileChannelManagerImpl.close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java:125) | 递归删除本地 Spill 目录；删除许可必须在调用之前取得 |

SYNC 只意味着维护没有脱离提交调用继续执行，不意味着所有维护异常都会在当前提交同步抛出。1.3.2 的维护 runnable 会捕获异常、记录 ERROR 并保存 `maintainError`；后续维护入口可能再抛出。本文保留原生错误传播行为，不宣称 SYNC 提供更强的事务保证。

因此，最后一次 maintenance 即使写出 ERROR，当前 commit 仍可能正常返回，且原生 close 不检查 maintainError；此时连接器可能输出 SUCCESS。“正常退出”证明连接器可观察操作及资源收尾满足本文契约，不保证内核隐藏的维护操作全部成功。这是保持现有原生行为的明确边界，不属于 Compaction 失败豁免。不得用额外空 commit、日志抓取或反射探针伪造更强保证；若将来要求最后一次维护错误也必须向宿主抛出，需要另行调整内核/API 范围。

`RecordWriter.compactNotCompleted()` 不能作为纯观察轮询接口：`MergeTreeWriter` 和 `AppendOnlyWriter` 的实现会尝试触发 Compaction。本文采用明确的最终 prepare 与执行器终止屏障，不使用该方法构造等待循环。

## 5. 配置准入：从默认值收紧为能力约束

统一校验函数检查 `FileStoreTable.coreOptions().snapshotExpireExecutionMode()`，仅接受 `CoreOptions.ExpireExecutionMode.SYNC`。

校验位置必须覆盖：

1. 连接器配置加载和最终 table properties 合并后的新建表选项；未配置时写入或保留明确的 SYNC 语义。
2. 已有表实际加载后的有效 options；不能只检查连接器 UI 配置。
3. Context Factory 的最终入口，在创建 writer、committer、IOManager 或启动任何任务之前再次校验。
4. 临时 DDL committer、表刷新/重建等所有绕过常规 Context 的构造入口。

ASYNC、无效枚举值均快速失败，错误包含表标识、配置键、实际值和“仅支持 SYNC”。不静默覆盖已有表的 ASYNC 元数据，不增加未获批准的 ALTER TABLE。运行中的 Context 使用其构造时的 immutable options；后续加载新表对象仍须重新校验。

删掉对 `TableCommitImpl.getMaintainExecutor()` 的捕获、等待和 retained 判定。保留原生 committer.close；共享的文件操作线程池等资源仍按 Paimon 原生所有权使用，不能因为没有独立 maintenance 线程就擅自关闭全局池。

## 6. 停止协议：完整等待后一次性完成

### 6.1 状态和不变量

保留现有 Service 的 NEW、RUNNING、STOPPING、FAILED、CLOSED 和错误传播语义；正常停止路径为 `RUNNING → STOPPING → CLOSED`。Context 只需“可写、关闭中、关闭已完成”的生命周期状态，加一个不可变的终态结果；业务失败状态与关闭状态不混为一谈。

终态结果区分 `SUCCESS`、`SUCCESS_COMPACTION_DISCARDED`、`FAILED`；另有独立的资源清理完成证明。业务 drain、commit、状态保存或 callback 失败时，仍返回 FAILED 和原异常；若之后执行器、writer、committer、IOManager 均完整关闭且目录收尾完成，允许按旧 token 释放 owner，由新 Service 按既有恢复协议重建。只有资源安全尚未证明时才保留 fence。不能用 FAILED 自动否定已取得的资源证明，也不能用 CLOSED 状态代替证明。永久等待不伪造终态。

- **I1 准入屏障**：STOPPING 后拒绝外部新建/写入 Context、普通 commit、DDL 和普通 callback 准入；已经准入的操作必须完成。唯一 close worker 仍可通过现有 stop-drain 专用许可完成业务 flush、pending 恢复及已确认业务对应的剩余 callback，随后才发布前台归零证明。
- **I2 业务提交屏障**：最终 Compaction 豁免阶段开始前，业务 buffer 已 drain，精确 pending 已确认，必要的 offset callback 已完成，没有既有 sticky failure。
- **I3 无破坏性超时**：close 调用者始终等到同一个 close operation 完成。5 秒只决定进度日志间隔，不产生超时返回、cancel、shutdownNow、资源删除或 owner 释放。
- **I4 删除许可**：连接器调用 strategy/writer 的最终 close 和 IOManager.close 前，所有准入访问者已退出，连接器持有的 Compaction executor 已通过 `awaitTermination` 正向终止。
- **I5 提交隔离**：只有尚未进入外部提交、且已证明属于最终停止 Compaction 的失败可以放弃；业务数据和结果不确定的提交不能放弃。
- **I6 同 JVM 代次互斥**：旧 Context 仍在等待、提交或清理时继续持有物理表 owner 和目录保护；完整关闭后才按旧 token 条件释放，旧关闭者不能释放新 token。跨 Engine 写入交接不由该静态 owner 保证，具体缺口见 P17。

### 6.2 架构时序

```mermaid
sequenceDiagram
    participant Caller as PDK close 调用者
    participant Stop as 唯一 close worker
    participant Ctx as Context / RecordWriters
    participant Exec as Compaction Executor
    participant Commit as Committer + SYNC maintenance
    participant IO as IOManager / Spill
    Caller->>Stop: beginStopping；启动或复用同一次关闭
    par 进度观察
        loop 关闭未完成，每 5 秒
            Caller->>Caller: INFO table / phase / elapsed
        end
    and 实际关闭
        Stop->>Stop: 等 scheduler、已准入操作和 callback 收敛
        Stop->>Ctx: 业务 drain；恢复并确认 pending
        Ctx->>Commit: 既有业务提交协议
        Commit-->>Ctx: 提交确认；SYNC 维护调用已返回
        Stop->>Ctx: 最终 prepareCommit(true, nextId)
        Ctx->>Exec: 等待已有及本次原生触发的任务
        Exec-->>Ctx: 结果或明确的 Compaction 异常
        alt 最终准备成功且有结果
            Ctx->>Commit: 使用精确 envelope 提交 / filterAndCommit 恢复
            Commit-->>Ctx: 确认提交与状态记录
        else 仅最终 Compaction 失败
            Stop->>Stop: 标记该表最终提交放弃，记录原因
        end
        Stop->>Exec: shutdown；不取消已提交任务
        Stop->>Exec: awaitTermination，直到实际终止
        Exec-->>Stop: 所有任务退出
        Stop->>Ctx: sync 全部剩余 bucket Future；关闭策略与 writer
        Stop->>Commit: close
        Stop->>IO: close，删除 Spill；释放目录保护
        Stop->>Stop: 关闭其余自有资源；条件释放物理 owner
    end
    Stop-->>Caller: 缓存唯一终态结果
    Caller->>Caller: 成功时 INFO 正常退出；失败时返回原始错误
```

图中业务屏障或最终提交失败走硬失败清理路径，不进入“失败即正常退出”的分支；清理仍须遵守实际终止屏障。

### 6.3 唯一关闭执行者与日志观察者

保留 Service 现有 close operation 去重和稳定线程组的 close worker，减少对 Engine 任务线程组中断的依赖。所有普通 close 调用者等待同一 completion，读取同一终态异常，不能分别重试提交或重复关闭资源。

将当前“前台无限等待 + 后台 30 秒”的两段 caller 等待合并为一个无总时限的 completion 等待。调用者每 5 秒读取不需要获取表锁的进度快照并输出 INFO；实际 Future.get、SYNC commit 或资源 close 阻塞也能被观察。多调用者通过同一个时间戳合并心跳，避免每个调用者重复打印。

close worker 自身在 callback 中重入 close 时保留现有防自等待规则：内部返回不能发布 CLOSED、释放 owner 或打印正常退出；外部调用者仍等待完整关闭。

调用者中断不能提前返回。按既有契约记录中断，继续等待，最后恢复中断标志并保留失败结果。不得把中断、Future 被取消或执行器拒绝任务归类为可豁免 Compaction 失败。执行 Compaction 的线程也须使用稳定线程组，并保留必要的 TCCL/运行上下文，不能依赖被停止任务的短生命周期线程组。

中断记录与最终结果发布使用现有关闭协调锁线性化：完成尚未发布时，中断进入共享 failure；完成已经发布时，仅恢复当前调用者中断标志，不再修改缓存结果。清理结束后一次性发布 outcome、finished 与 completion；重复调用不能因晚到中断返回不同的组件关闭结果。只有该次唯一终态发布者打印最终日志。

### 6.4 业务 drain 与最终 Compaction 分开

保持现有 `commitOrConfirmLocked` 的业务语义，包括精确 pending envelope、identifier 单调推进、状态持久化、微批确认和 offset callback 顺序。普通写入仍使用 `prepareCommit(false, identifier)`，不全局改为强制等待。

在业务 drain 及 callback 完成后，为每个仍存在的 Context 执行一次显式最终 prepare。它不能受 `bufferedRecordCount > 0` 限制：零条新业务记录仍可能有未提交的 Compaction 结果。

最终 prepare 在既有策略模板内增加停止专用入口，继续执行 `beforePrepareCommit` 等模式钩子，底层调用 `delegate.prepareCommit(true, identifier)`。不得绕过 HashDynamic/KeyDynamic 等策略生命周期。

最终 prepare 前执行器保持开放；prepare 返回或失败后才停止提交新 Compaction 任务。没有新业务操作时，这个过程是有限的原生 prepare，不循环强制 compact。

成功返回时，在 1.3.2 原生适配边界逐条验证 `CommitMessage instanceof CommitMessageImpl` 后，再检查 `newFilesIncrement().isEmpty()` 及整体 `isEmpty()`。这两个方法不在 CommitMessage 接口上，不能直接调用接口或静默接受未知实现。检查包含 data、changelog 和 index 的全部字段；未知类型或非空 DataIncrement 是硬失败，不能标记为仅 Compaction。`DynamicBucketIndexMaintainer.prepareCommit()` 仅在 modified 时产生新 index；业务 drain 应已将它清空，此项须用真实动态桶测试证明。

有非空 Compaction 结果时，先通过 §7.1 的控制失败审计并封闭最终 prepare 阶段，再在外部提交前保存精确 `(commitUser, identifier, messages)`，沿既有 commit/filterAndCommit 协议确认结果，再推进并持久化 identifier。普通提交也检查该 Context 已记录的控制失败，防止被原生吞掉的取消在运行态被当作成功。不得重新 prepare 替换待确认 messages。最终提交本身不产生新的源端 offset 进度，也不重复执行业务 callback。

结果全部为空时不为“停止”人为制造空 Snapshot，不推进 identifier，不额外运行一次空提交维护。仍需完整关闭所有资源。

实施时的真实测试补充：一次非空的纯 Compaction envelope 默认会产生空 APPEND 和 COMPACT 两个 Snapshot，使用同一个 identifier。`StreamWriteBuilderImpl.newCommit()` 设置 `ignoreEmptyCommit(false)`，`FileStoreCommitImpl.commit()` 分别提交 append/compact changes；本修复保留原生行为，不能把“一次最终提交”解释为“恰好一个 Snapshot”。上面的空结果规则仅指整个 messages 没有任何增量时不调用 commit。证据见 P15。

## 7. 最终 Compaction 失败的准确识别与收尾

### 7.1 类型化失败来源

连接器拥有的执行器在提交原生 `CompactTask` 时包装其 `Callable.call()`：普通任务执行异常用一个内部类型 `NativeCompactionFailure` 标记，原异常作为 cause 保留。它不是新的重试策略，不改变任务调度、成功结果或封闭前的原生取消语义；封闭后的意外取消按下文作为契约违例处理。

包装边界只接受已核对的 `CompactTask` 任务族，包括 MergeTree 与 bucketed append 的任务。未知任务类型不自动获得豁免资格；若依赖升级改变任务形态，按兼容性门禁失败。`Error`、中断、CancellationException 不转换成可豁免标记，包装异常也不能隐藏致命错误 cause。

仅包装 Callable 异常不够：Paimon 会在 Future.get 处吞掉 CancellationException。执行器适配边界必须额外保留 Context 生命周期内的首个 `controlFailure`，记录取消成功、中断、Error、未知任务或提交拒绝；这些均为不可豁免失败，不能在原生 Future 被消费后清空。普通任务失败仍走 NativeCompactionFailure，不保存历史成功或失败任务列表；持久诊断状态为 O(1)，queued/active 计数仅用于 INFO，不能替代 termination。

返回给 Paimon 的 Future 包装 cancel：调用 delegate.cancel 与取消成功后的 controlFailure 记录置于同一内部短锁；提交前审计也取得该锁，检查无 controlFailure 并封闭最终 prepare 阶段。仅“cancel 返回前记录”不够，因为 delegate.cancel 已可能唤醒 Future.get，审计会抢在记录之前执行。cancel 返回 false 时不记录取消成功；致命/中断 cause 与提交拒绝在各自传播前记录。

封闭后不再调用任何可能产生 Compaction 的 prepare/write/compact；原生 Future 不向外暴露。1.3.2 的正常取消来源是原生 writer.close，必须排在最终提交、shutdown/await/sync 之后，此时 cancel 已完成 Future 应返回 false。封闭后出现对未完成 Future 的意外取消请求，记录契约失败并拒绝取消，最终不能报告成功。锁不能覆盖 await、sync 或外部 commit I/O。

控制失败有两次强制审计：最终 prepare 返回后、创建 pending/任何外部提交之前；以及 shutdown、实际 termination、全部 bucket sync 之后。只有两次审计均无 controlFailure，且捕获的异常全部符合本节任务失败标记条件时，才允许 SUCCESS_COMPACTION_DISCARDED。空结果也必须通过审计，不能由空结果跳过失败检查。

仅在以下条件同时满足时接受放弃：处于最终 STOP prepare；§6.4 业务屏障已成立；没有 pending 外部提交；接收到原生 Future 传播的、cause 为该执行器任务标记的异常。不得依据异常 message、文件后缀、堆栈字符串或任意 `ExecutionException` 判断。

同样的标记若在 RUNNING、业务 drain 或 DDL 阶段被观察到，仍是硬失败。失败可能来自停止前已启动、最终阶段才被观察到的任务；是否豁免以业务屏障及观察阶段为准。

### 7.2 多 bucket 的部分准备失败

一张表某个 bucket 失败时，放弃该表这次最终 prepare 的全部 Compaction 提交，不只跳过失败 bucket 后提交一个无法证明完整的结果集合。其他表可以继续各自关闭；全局只有可豁免错误时才能正常退出。

失败处理固定顺序：

1. 不创建最终 pending，不调用该尝试的 commit/filterAndCommit，不推进 identifier，不修改 offset，不重试最终 prepare。
2. 对该表执行器调用 `shutdown()`，随后等待实际 termination。队列中和正在执行的其他 bucket 任务继续执行到终态，不取消它们。
3. 取得终止证明后，使用 Paimon 的公开接缝读取当前所有 bucket writer 的快照：先验证 `TableWriteImpl.getWrite()` 返回的 `FileStoreWrite` 实际为 `AbstractFileStoreWrite`，再调用 `writers()` 复制全部 `WriterContainer.writer` 引用；未知类型硬失败，不通过反射或空集合回退。
4. 对每个 writer 调用原生 `RecordWriter.sync()`，每个 bucket 都必须被访问，即使前一个抛出已知 Compaction 失败。此时任务已经结束，不再触发 Compaction；原生 `CompactFutureManager` 的 finally 会消费 Future 引用。
5. 保留并记录可豁免任务异常；`sync()` 在应用结果、更新 levels、删除中间文件等位置产生的其他异常为硬失败，不能混同于任务执行失败。
6. 完成控制失败第二次审计后，再执行策略资源、writer、committer 和 IOManager 的最终关闭。已消费的失败 Future 不能在原生 writer.close 再次抛出并截断其他 bucket 的关闭。若原生 close 在其他 I/O 处失败，仍可能短路后续 bucket；这属于硬失败，该表未取得完整资源清理证明，保留 owner，不能宣称全部 bucket 已关闭，也不复制内核 close 实现来掩盖异常。

上述接缝是 `public @VisibleForTesting`，不是 Paimon 承诺的稳定公共生命周期 API。将访问限制在一个小型适配边界，验证实现类型；只能读取 map 并调用原生方法，不能反射修改 `taskFuture`、清空 writers map 或复制 Paimon close 实现。测试必须覆盖所有连接器支持的 bucket 模式；升级时重新核对。

即使前台已有硬失败、不允许最终 prepare，也要对已存在的执行器 graceful shutdown、取得 termination 并消费所有已结束 Future 后再清理。此路径保留原业务错误为主异常，后续错误放入 suppressed；不因清理成功改写为正常退出。

### 7.3 放弃的存储边界

最终阶段输入业务数据已经确认提交。失败时保留此前成功的 Snapshot，不能 abort 或删除已确认业务文件；结果不确定的提交也不能执行 abort。

Paimon 的逐 bucket prepare 可能已经消费部分 increment；失败任务也可能产生未返回的远端文件。因此正常退出保证本地 Spill 和自有资源已正确关闭，不保证失败尝试从未产生远端孤儿文件。可由原生 close 清理的文件交由原生关闭处理；未被引用的远端残留由既有 orphan-file 治理处理。本变更不新增远端递归删除或自动 `remove_orphan_files`，snapshot expiration 也不能被描述为孤儿文件清理器。

## 8. 目录保护、DDL 和构造失败

### 8.1 正常清理与清理失败

成功路径保留现有目录 canonical 化、live 登记和 owner 文件锁，直到 IOManager.close 成功。正常退出必须同时具备：业务阶段成功、最终 Compaction 已提交或按规则放弃、任务实际终止、各资源关闭成功、Spill 删除成功。

若任一资源关闭失败，汇总原异常及 suppressed，并返回失败，不能打印“正常退出”。没有充分资源安全证明时保留物理 owner；不能用状态设为 CLOSED 代替资源证明。此类异常 fence 不使用定时 reaper，不允许同 JVM 自动重建覆盖旧 owner。

IOManager 删除失败时保留 marker；在确认已无访问者后可释放本进程锁和 live 登记，让既有 stale cleaner 后续按锁、年龄等规则处理残留，不能调用会无条件删除 marker 的普通注销分支。若仍无法证明没有访问者，登记和锁全部保留。此修正限定在删除结果与登记收尾的一致性，不引入新的 owner-anchor 框架。

stale cleaner 继续要求：匹配目标目录、无符号链接跟随、不在本 JVM live 集合、有 marker、取得独占锁、目录和子项满足现有 grace、检查无异常。删除失败保留 marker；无 marker 的历史目录不自动删除。等待 Compaction 超过 grace 时，仍必须因 live/锁保护跳过活跃目录。

日志只使用构造时已缓存的目录列表；`IOManagerImpl.getSpillingDirectories()` 会懒建目录，不得为了打印日志在关闭后调用它。

### 8.2 DDL 与临时 committer

DDL 仍在既有准入与表锁下执行。先业务 drain；失败时保留原 Context、pending 和 owner，不执行 DDL action。drain 成功后关闭旧 Context，等待 Compaction 真正结束，消费 Future 并完成资源清理，再执行 DDL action。

DDL 不适用 STOP 的 Compaction 失败豁免。任务或关闭失败时，DDL action 不得执行。物理 owner 的释放必须在 DDL action 结束后按原 token 进行，不能在 action 前创建新代写入窗口。

临时 committer 同样严格校验 SYNC，直接按同步调用生命周期关闭，删去专用 maintenance retained/reaper。原生 `truncateTable()` 本身不调用 maintain，不能为测试方便将其描述成必定触发维护。

### 8.3 构造回滚

Factory 必须在第一次使用前建立执行器与目录所有权。任何构造失败都不得泄漏已创建的 IOManager 引用。`resolveAndCreateIOManager` 中注册失败、部分根路径成功等情形须有局部回滚，不能因尚未返回 build result 而跳过关闭。

回滚只清理本次实际拥有的资源；锁冲突不授权删除其他 owner 的目录。若初始化已启动任何异步任务，仍必须先等待实际终止；只有已证明尚未启动访问者的构造路径才可直接关闭。保留原始构造异常，清理失败作为 suppressed。

Factory 在得到 raw writer、raw committer 后立即保存本地引用，再执行类型校验、注入或包装，防止包装前异常使资源丢失。Service 的构造失败 catch 只有在局部回滚已取得完整清理证明时才注销物理 owner；不得无条件 unregister。同步配置检查提前到 owner 注册、commit-state 绑定和动态桶 preflight 之前，Factory 校验仍作为最终兜底。

共享 Hadoop FileIO/S3A 的稳定线程组、UGI/TCCL 与最终缓存配置语义保持不变；不能反射关闭共享 `fsMap`、引入第二份 RocksDB JNI 或把共享资源改成 Context 独占。

## 9. 简化清单与代码边界

| 当前结构 | 新结构 |
| --- | --- |
| Compaction + maintenance 两类执行器证明 | SYNC 准入校验 + 一个连接器持有的 Compaction 执行器 |
| 30 秒 shared deadline、预算传播和超时异常 | 无总期限等待；5 秒进度间隔 |
| RETAINED_COMPACTION / RETAINED_MAINTENANCE | 正在关闭或已完成；结果单独记录 |
| Context reaper、临时 committer reaper、retained 计数 | 唯一 close operation 等待真实完成，无延迟接管 |
| 独立 retained close monitor | close 调用者读取进度快照并输出 INFO |
| maintenance 结束后可提前释放 owner | 完整关闭后按原 token 释放 |
| 仅有新记录才执行 stop flush | 业务 drain 后显式执行一次最终 Compaction prepare |
| 泛化捕获异常 | 原生任务标记 + O(1) 控制失败记录，最终 STOP 阶段严格审计 |

主要修改位置：

- [PaimonService](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java)：准入、业务屏障、唯一关闭流程、DDL 与 INFO 进度。
- [PaimonTableWriteContext](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContext.java)：最终 prepare/提交、终态结果、原生 Future 收尾与资源关闭。
- [PaimonCompactionLifecycle](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonCompactionLifecycle.java)：压缩为单执行器所有权、任务异常来源和无期限 termination 等待。
- [PaimonTableWriteContextFactory](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContextFactory.java)、[策略模板](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/bucket/AbstractPaimonBucketWriterStrategy.java)：SYNC 校验、原生接缝和停止专用 prepare。
- [PaimonSpillDirCleaner](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/util/PaimonSpillDirCleaner.java)：保留保护协议，补齐注册失败与删除失败的收尾。
- `src/test/java/io/tapdata/connector/paimon/{config,write,service,util,commit,fs}`：单元、真实 Paimon 和 Service 并发回归。

以修改前实际工作区为比较基线，统计生产代码增删行和删除的状态/线程入口。验收要求相关生命周期生产代码净减少，旧 retained/reaper/maintenance 捕获和 deadline 路径全部无引用；不通过压缩格式、删注释、搬到另一类或弱化测试制造“简化”。新引入的异常标记和原生适配必须比被删除的通用保留机制更窄。

## 10. INFO 日志契约

使用 `[paimon-stop]` 固定前缀。进度字段：`table`、`owner`、`phase`、`elapsedMs`、`phaseElapsedMs`；等待 Compaction 时增加已知的 queued/active 数或当前表信息，不能把未知值写成零。目录只在真实资源边界输出缓存值。

阶段固定为 `DRAIN`、`FINAL_PREPARE`、`FINAL_COMMIT`、`WAIT_COMPACTION`、`CLOSE_WRITER`、`CLOSE_COMMITTER`、`CLOSE_SPILL`、`CLOSE_SERVICE`。使用单调时钟计算耗时；INFO 观察不持有阻止 worker 继续工作的锁，不启动新的每表监控线程。

```text
INFO [paimon-stop] event=start owner=... phase=DRAIN
INFO [paimon-stop] event=waiting table=pr_operators phase=FINAL_PREPARE elapsedMs=35000 phaseElapsedMs=30000
INFO [paimon-stop] event=compaction-discarded table=pr_operators identifier=... reason=... cause=...
INFO [paimon-stop] event=compaction-terminated table=pr_operators elapsedMs=...
INFO [paimon-stop] event=spill-closed table=pr_operators dirs=[...]
INFO [paimon-stop] event=finished outcome=SUCCESS_COMPACTION_DISCARDED discardedTables=1 elapsedMs=... message=正常退出
```

失败原因首次输出完整异常链，心跳不重复堆栈。只有最终 Compaction 被放弃时，既要输出原因，也要在全部收尾完成后输出正常退出；不能在 catch 到失败的瞬间就打印正常退出。硬失败输出失败终态与原异常，不出现正常退出日志。日志格式化或日志后端的普通运行时异常不能改变提交、owner 或清理结果。

## 11. 验收矩阵

JUnit Jupiter + Mockito 验证边界，真实 Paimon 本地文件表验证文件、Snapshot 与 Future 行为。现有测试框架和依赖保持不变。所有并发 fixture 在 finally 释放 latch，并验证线程、锁和目录登记回到基线；删除临时目录不能代替关闭资源。

| 编号 | 场景 | 必须观察到的结果 |
| --- | --- | --- |
| S01 | 配置未填、显式 SYNC、有效 options 被覆盖为 ASYNC、已有 ASYNC 表、临时 committer | SYNC 可用；其他值在资源启动前失败；已有表属性未被静默修改 |
| S02 | 原故障确定性复现 | 真实 `MergeTreeCompactTask` 到达 `MergeSorter.spill`；旧顺序删除后放行产生 channel FNF，证明 fixture 确实覆盖事故链路 |
| S03 | 新协议阻塞 Compaction 超过 30 秒 | 至少一个真实集成用例阻塞 ≥35 秒；close 未返回、目录存在、owner 不释放、stale cleaner 跳过、INFO 多次出现；放行后成功退出 |
| S04 | 零条新业务记录但有 Compaction 结果 | 停止执行最终 prepare，Snapshot 的文件集合体现 Compaction，最终读取行值不变；无额外 offset callback |
| S05 | 最终 Compaction 确定失败 | 不提交该表最终尝试；旧业务 Snapshot 与已确认数据仍可读；任务实际退出、资源清理完成后返回成功且日志为放弃后正常退出 |
| S06 | 多 bucket：一个失败，另一个仍阻塞，一个已成功准备 | 失败后仍等待阻塞者；放弃整表最终尝试；遍历全部剩余 Future；无重复失败导致的 bucket 清理短路 |
| S07 | 同一种 Compaction 异常发生在 RUNNING、业务 drain、DDL | 原失败语义保持；DDL action 不执行；不得出现正常退出日志 |
| S08 | 最终 prepare 中普通 IO、index、beforePrepare、结果应用失败及 Future 取消 | 没有任务来源标记的异常为硬失败；Paimon 吞掉取消且返回空结果时也禁止提交/正常退出；不以 ExecutionException/FNF 类型泛化豁免 |
| S09 | 最终 commit 成功但响应失败、filterAndCommit 重试、identifier 保存失败 | 精确 envelope 不变、不再次 prepare、不误推进 offset；恢复成功才确认；无法确认时返回失败，不 abort 不确定文件 |
| S10 | 业务 pending、callback 正在运行、callback 失败、多表部分 drain 失败 | 等待现有 callback；业务失败优先且不进入可豁免阶段；禁止重放/跳过已有业务提交协议 |
| S11 | 并发 close、重入、中断、取消与提交审计竞争 | 一次提交/关闭且无自死锁；取消已唤醒 get 但尚未完成记录时，审计必须等待短锁且禁止提交；晚到中断不篡改已发布终态；完整证明前不释放资源 |
| S12 | 慢 SYNC maintenance，含 partition expiration 提交能力 | close 等待同步调用结束；没有旧 maintenance 与新代重叠；不创建异步维护/回收线程 |
| S13 | 同 JVM 停止后重启、业务失败后完整清理、停止期间抢占、旧 token 延迟释放 | 停止中拒绝新代；完整清理后可重建，即使旧操作返回业务 FAILED；旧 token 不能释放新 owner |
| S14 | DDL、构造失败、注册第二个 root 失败、锁冲突 | 失败不越权删除、不漏局部 IOManager；主异常与 suppressed 正确；没有 retained/reaper |
| S15 | writer/committer/IOManager.close 抛错或长时间阻塞 | INFO 持续可见；原生首个 bucket close 的普通错误可能截断后续关闭，此时不宣称完整清理并保留 owner；删除失败保留 marker；无终止证明不删目录 |
| S16 | HASH_FIXED、HASH_DYNAMIC、KEY_DYNAMIC、POSTPONE、BUCKET_UNAWARE 与 append | 路由语义保持；最终 DataIncrement 为空；强校验 FileStoreWrite/CommitMessageImpl 具体类型与 sync 接缝；未知实现硬失败，无 Compaction 模式正常关闭 |
| S17 | Spill 文件锁、无 marker、符号链接、多 root、超 grace 活跃目录 | 原目录保护用例仍通过；日志不会懒建新目录；清理只作用于本次拥有的目录 |
| S18 | S3A 稳定线程组、UGI/TCCL、cache true/false、RocksDB 装载 | 现有 FileIO 和 JNI 回归通过；停止没有新增共享资源误关或类加载器语义变化 |
| S19 | 静态简化检查及整个模块回归 | 旧超时/retained/reaper 路径全部移除、生产生命周期代码净减少；原写入/提交/offset 用例不弱化 |
| S20 | A/B 独立进程持锁、异常退出与残留回收；核对调度交接 | 真实子 JVM 存活时 grace=0 仍跳过，实际退出后回收，无 marker 保留；同时记录 Engine 单活缺口，不冒充已修复 |

S02–S06 复用并修订 [真实 Spill fixture](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/write/PaimonCompactionSpillLifecycleIntegrationTest.java)。现有参数 `compaction-trigger=100`、3 个 L0 文件不会自动触发：准备文件后必须显式调用 `rawWriter.compact(BinaryRow.EMPTY_ROW, 0, true)`，等待并断言 `MergeSorter.spill` 的 latch 和真实调用栈。原来的“超时 retained 即通过”断言必须替换，不能只改测试名称。

S05 和 S06 的失败注入点必须是原生 Compaction task，分别在 Spill 前后覆盖；还要注入与其相同 message 的普通 prepare 异常，证明失败分类依赖来源而不是字符串。远端孤儿文件可能性按 §7.3 报告，不能删除整个测试 warehouse 后声称原生回收完毕。

## 12. 命令、代码风格与交付门禁

以下是可复核的验证命令。2026-09-05 已执行完整 clean package，原 S01–S19 对应 627 项全部通过；新增 S20 后再次完整构建，629 项全部通过，最终命令和产物以实施验收报告为准。工作目录固定为 `/Users/SL/javaProject/tapdata-connectors`；使用本机已安装的 ARM JDK 17。当前 POM 同时存在 Java 8 properties 与 Java 11 compiler 配置，本变更不趁机调整兼容基线，需记录 effective compiler 配置及 Engine 实际运行版本。

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home mvn -version

env JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home mvn -B -pl connectors/paimon-plus-connector -am -DskipTests compile

env JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home mvn -B -pl connectors/paimon-plus-connector -am -Dtest='PaimonConfigTest,PaimonCompactionLifecycleTest,PaimonCompactionSpillLifecycleIntegrationTest,PaimonTableWriteContextTest,PaimonTableWriteContextIntegrationTest,PaimonTableWriteContextFactoryTest,PaimonServiceCloseTest,PaimonServicePhysicalTableOwnerTest,PaimonSpillDirCleanerTest,PaimonMicroBatchCommitTest' -Dsurefire.failIfNoSpecifiedTests=false test

env JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home mvn -B -pl connectors/paimon-plus-connector -am -DskipTests=false package

git diff --check -- connectors/paimon-plus-connector
git diff --numstat -- connectors/paimon-plus-connector/src/main/java
rg -n 'RETAINED_COMPACTION|RETAINED_MAINTENANCE|executorDeadlineNanos|captureMaintenanceExecutor|PaimonRetainedCloseMonitor|schedule.*Reaper' connectors/paimon-plus-connector/src/main/java
```

最后一个命令预期无匹配，`rg` 退出码 1 表示未发现旧路径。同时人工检查是否换名保留了相同机制。聚焦命令的 failIfNoSpecifiedTests=false 仅允许 reactor 上游没有目标测试，目标模块的 Surefire 报告必须明确包含选中用例；零测试、skipped、编译失败均不能计为通过。新增专门测试类时加入聚焦选择器。

实施前保存工作区文件摘要和基线测试结果；测试异常先区分 baseline failure 与新增回归。package 必须报告测试实际总数、失败/跳过及 JAR 路径；不能沿用旧 Spec 的 540 项通过作为本文验收结论。不要求为文档改动运行 Maven。

沿用现有 Java 风格、命名与窄接口，不做整文件格式化。策略模板中现有实际代码如下；新停止入口保留钩子调用，仅改变底层等待参数，不把停止策略散落到各 bucket 实现：

```java
@Override
public final List<CommitMessage> prepareCommit(long commitIdentifier) throws Exception {
    ensureOpen();
    beforePrepareCommit(commitIdentifier);
    return delegate.prepareCommit(false, commitIdentifier);
}
```

生产注释说明为什么需要屏障和失败边界，并标注 Paimon 版本与方法。面向维护者的 Spec、验收记录使用中文；Java 标识符遵循项目现有英文命名。

## 13. 边界、风险审查与完成条件

**必须执行**：遵守本文准入和删除屏障；保留业务提交恢复与 offset 行为；使用固定版本源码核对；对真实 Spill、部分失败和超过 30 秒等待进行验收；保护当前未提交工作；汇总实际变更和测试证据。

**范围变化时先确认**：修改 Paimon 内核或依赖版本；改变存量表属性/业务路由/offset；新增外部清理作业、宿主强杀策略或跨 JVM 分布式 fencing。这些不是本次修复的必要默认动作。

**禁止执行**：取消 Future 后立即删 Spill；超时释放 owner；泛化吞异常；对未知提交结果 abort；反射篡改 Paimon 内部状态；为了测试通过删减业务回归；覆盖用户已有修改；未经要求提交或推送代码。

已在设计阶段消除的主要漏洞：提前 shutdown 导致最终 prepare 拒绝任务；零输入遗漏 Compaction 提交；部分 bucket 已消费结果后错误地局部提交；Future 失败重复传播截断关闭；SYNC 只校验默认值；日志被阻塞 worker 一起卡住；目录删除失败却删除 marker；同 JVM 新旧代重叠。

仍须通过实现及测试证明的风险：原生任务包装与全部 writer 类型兼容性、最终 DataIncrement 的严格隔离、重入/中断与 callback 顺序、构造回滚所有权、宿主真实停止及 S3A 行为。源码推理不是这些运行时门禁的替代品，本文不作“未经执行即百分之百无漏洞”的保证。

原 S01–S19 已完成实现与本地验收。新增运行事实为故障后可迁移到 Engine B，A/B 都恢复时也可能优先 A；需核对旧实例停止与新实例调度间的交接保证，不能把调度位置或心跳恢复当成 termination proof。跨进程目录回归列为 S20；调度源码与剩余边界见 P17。不会降级到 30 秒返回或 retained 协议。

只有 S01–S19 全部有可回查证据、生命周期生产代码实现净简化、完整模块回归通过，才可声明“本次定义的 Spill 生命周期修复完成”。生产环境中不再复发还须以宿主部署后的实际运行证据确认。

## 14. Paimon 源码证明记录

本节是必须随实现维护的证据记录，不是参考链接清单。每条记录给出设计命题、准确源码入口、关键语句、推导边界以及验证用例。下列片段均为 1.3.2 对应本地源码的局部摘录，省略上下文时明确说明；片段不作为可直接复制的连接器实现。

证据级别分为：**源码事实**（实际代码直接成立）、**设计推导**（在明确前提下由源码事实推出）、**实施验证**（有实际测试运行证据）。原设计阶段的待测试项已逐项执行，以下保留各层证据的区别；源码事实不能替代运行时验收。

### P01：SYNC 的选择依据与执行位置

**对应要求**：§5、I1；S01、S12。

源码入口：[CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE](/Users/SL/javaProject/paimon/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java:435)、[TableCommitImpl 构造器](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:118)、[commitMultiple](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:225)、[maintain](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:348)。

构造器的完整选择表达式：

```java
this.maintainExecutor =
        expireExecutionMode == ExpireExecutionMode.SYNC
                ? MoreExecutors.newDirectExecutorService()
                : Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "expire-main-thread"));
```

**源码事实**：`commitMultiple` 在底层 commit 后调用 `maintain(..., maintainExecutor, ...)`；maintain 通过该 executor.execute 执行维护。SYNC 选择 direct executor；ASYNC 才创建独立维护线程。

**设计推导**：严格锁定真实 table options 为 SYNC，并等所有提交调用完成后，可以删除连接器管理的“独立 maintenance 线程尚未退出”分支。此推导不授权关闭原生共享文件操作线程池，也不等价于所有维护都成功。

**实施验证**：S01/S12 已由 Config、Factory、PhysicalOwner 和真实 NativeMaintenance 用例通过，见实施验收 §4/§6。

### P02：维护失败传播与分区删除的提交能力

**对应要求**：§4、§6.1；S09、S12。

入口：[TableCommitImpl.maintain](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:348)、[PartitionExpire.doBatchExpire](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/PartitionExpire.java:170)、[FileStoreCommitImpl.dropPartitions](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java:601)、[OVERWRITE 提交](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java:926)。

调用链为 `maintain → partitionExpire.expire → doExpire → doBatchExpire → commit.dropPartitions → tryOverwritePartition → tryCommit(CommitKind.OVERWRITE)`；配置 catalog partition handler 时走其对应分区删除实现。

维护 runnable 的局部异常分支：

```java
} catch (Throwable t) {
    LOG.error("Executing maintain encountered an error.", t);
    maintainError.compareAndSet(null, t);
}
```

**源码事实**：维护具有提交能力；失败会被 runnable 捕获保存，当前调用未必向调用者抛出。`PartitionExpire` 的分批过期使用顺序 forEach 调用 doBatchExpire，不另建分区提交 executor。

**设计推导**：不能把 ASYNC 维护与新代并存；仅在 SYNC 配置前提下，用提交调用归零覆盖这条维护入口。保持原生异常传播，不把“调用已结束”写成“维护一定成功”。

### P03：连接器能够拥有 Compaction 执行器关闭权

**对应要求**：I4、§9；S03、S19。

入口：[TableWriteImpl.withCompactExecutor](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java:134)、[AbstractFileStoreWrite.withCompactExecutor](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:146)。后一方法的完整主体：

```java
this.lazyCompactExecutor = compactExecutor;
this.closeCompactExecutorWhenLeaving = false;
```

**源码事实**：外部执行器被原生 writer 使用，并关闭 writer 自行 shutdown 执行器的开关。`AbstractFileStoreWrite.close` 只有该开关为 true 才调用 shutdownNow。

**设计推导**：无需改 Paimon 内核即可先 graceful shutdown、取得真实 termination，再调用原生关闭。执行器拥有权必须在第一次 write 前注入；这不是删除 IOManager 的充分条件，仍须前台访问者归零。

### P04：原生 close 不提供线程实际退出证明

**对应要求**：I3、I4；S02、S03、S11。

入口：[MergeTreeWriter.close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:343)、[AppendOnlyWriter.close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/append/AppendOnlyWriter.java:250)、[CompactFutureManager.cancelCompaction](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java:34)、[AbstractFileStoreWrite.close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:304)。

MergeTreeWriter.close 开头依次调用：

```java
compactManager.cancelCompaction();
sync();
compactManager.close();
```

cancelCompaction 实际调用 `taskFuture.cancel(true)`；原生 FileStoreWrite.close 中没有 `awaitTermination`。

**源码事实**：这里发出取消请求，而不是等待实际线程退出。**设计推导**：需要连接器增加 executor termination 屏障；不能声称“Paimon 内核已经保证 close 会等待完成”。**实施验证**：S02/S03 的真实 Spill fixture 与 35 秒 Service 集成用例证明完成前不会返回或删除。

### P05：最终 prepare 为什么必须发生在 shutdown 之前

**对应要求**：§6.4；S03、S04、S16。

入口：[MergeTreeWriter.flushWriteBuffer](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:247)、[prepareCommit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:252)、[AppendOnlyWriter.flush](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/append/AppendOnlyWriter.java:234)。

MergeTreeWriter.flushWriteBuffer 尾部的连续语句：

```java
trySyncLatestCompaction(waitForLatestCompaction);
compactManager.triggerCompaction(forcedFullCompaction);
```

prepareCommit 先调用 `flushWriteBuffer(waitCompaction, false)`，之后再 `trySyncLatestCompaction(waitCompaction)` 并 drainIncrement；append writer 的 flush 也会 triggerCompaction。

**源码事实**：即使业务缓冲区为空，调用 prepare 仍可能调度原生 Compaction。**设计推导**：先 shutdown 再 prepare 会产生被拒绝任务；正确顺序是 prepare 等待并收集、确认最终提交、再 shutdown/await。这里的等待不承诺强制全量合并所有 sorted runs。

### P06：prepare 不是原子操作，部分结果可能已被消费

**对应要求**：§7.2、§7.3；S06、S08。

入口：[AbstractFileStoreWrite 的 bucket 循环](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:208)、[MergeTreeWriter.drainIncrement](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:280)。

**源码事实**：外层逐 bucket 调用 writer.prepareCommit，再把返回值加入局部 result；只有所有 bucket 完成才返回。内层 drainIncrement 创建增量副本后清空 `newFiles`、`compactBefore`、`compactAfter` 等容器。后续 bucket 抛错不会回滚此前已清空的容器，也不会返回先前局部 result。

**设计推导**：只有业务已经确认提交，才允许最终 Compaction prepare 失败时放弃整表这次尝试；不能猜测或重新 prepare 拼出部分提交。不能承诺原生 close 一定知道所有未提交远端文件。**实施验证**：S05/S06 的真实多 bucket 部分失败回归通过，断言整表不提交、既有 Snapshot/数据保留。

### P07：原生 Future 如何传播并消费失败

**对应要求**：§7.1、§7.2；S05–S08。

入口：[CompactFutureManager.innerGetCompactionResult](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java:47)。该方法内部的连续片段：

```java
try {
    result = obtainCompactResult();
} catch (CancellationException e) {
    return Optional.empty();
} finally {
    taskFuture = null;
}
```

`obtainCompactResult()` 的主体为 `return taskFuture.get();`，声明抛出 InterruptedException 和 ExecutionException。

**源码事实**：普通任务失败传播为 ExecutionException，同时清空该 Future 引用；取消被当成空结果。**设计推导**：不能把 Future.isDone、取消成功或空结果当作实际线程结束；失败后消费所有剩余 Future 有助于避免原生 close 再次因同一失败中断。**边界**：Paimon 没有提供“STOP 失败正常退出”这个业务契约，它是本文明确新增的连接器策略。

### P08：失败标记的原生任务来源

**对应要求**：§7.1；S05、S07、S08、S16。

入口：[CompactTask](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/compact/CompactTask.java:34)、[MergeTreeCompactManager 提交任务](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactManager.java:240)、[bucketed append 提交任务](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/append/BucketedAppendCompactManager.java:115)。

**源码事实**：CompactTask 实现 `Callable<CompactResult>`，call 调用 doCompact；MergeTreeCompactTask 和 FileRewriteCompactTask 继承它，MergeTree manager 将 task 交给 executor.submit。bucketed append 的 FullCompactTask、AutoCompactTask 也继承 CompactTask，交由同一注入执行器提交。

**设计推导**：连接器可以在自己持有的 executor 提交边界标记这类 Callable 抛出的普通异常，从来源上区分任务失败和 prepare 的其他失败。**实施验证**：Executor 15 项与六模式回归通过，覆盖 cause/suppressed Error、取消和自中断；`NativeCompactionFailure` 是连接器新增私有类型，并非内核 API。

### P09：读取 writer 并 sync 的接缝及其限制

**对应要求**：§7.2；S06、S15、S16。

入口：[TableWriteImpl.getWrite](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java:287)、[AbstractFileStoreWrite.writers](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java:567)、[MergeTreeWriter.sync](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java:276)、[AppendOnlyWriter.sync](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/append/AppendOnlyWriter.java:245)、[PostponeBucketWriter.sync](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/postpone/PostponeBucketWriter.java:235)。

**源码事实**：两个 getter 都为 public 且有 VisibleForTesting 标记；WriterContainer.writer 为 public final。MergeTree/append 的 sync 调用 `trySyncLatestCompaction(true)`；Postpone 的 sync 为空。与之相对，前两者的 compactNotCompleted 会调用 triggerCompaction。

**设计推导**：终止后只读快照并逐 writer sync 可以消费剩余结果，不需改私有字段；不能用 compactNotCompleted 代替。**实施验证**：NativeWriteAccess、Factory、六模式和多 bucket 失败用例通过。测试接缝不提供跨版本稳定性承诺。

### P10：Spill 文件打开与递归删除确实共享 IOManager

**对应要求**：§3、I4、§8.1；S02、S03、S17。

入口：[MergeSorter.mergeSort](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeSorter.java:104)、[MergeSorter.spill](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeSorter.java:159)、[IOManagerImpl](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java:74)、[FileChannelManagerImpl.close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java:125)。

**源码事实**：mergeSort 在 `ioManager != null && lazyReaders.size() > spillThreshold` 时进入 Spill；spill 用该 IOManager.createChannel 及 FileChannelUtil.createOutputView 打开 channel。IOManager.close 委托其 channel manager.close，后者对每个目录执行 `FileIOUtils.deleteDirectory(path)`。getSpillingDirectories 也经 lazy channel manager 初始化，具有建目录副作用。

**设计推导**：同一 IOManager 被提前 close 可以导致后续 Compaction channel 打开失败；禁止提前 close 和无所有权删除是可确定验收的修复点。此证明没有确定附件事故的实际删除者，需与 §2.2 的证据边界一起理解。

### P11：最终结果的业务增量检查与提交恢复

**对应要求**：§6.4、§7.3；S04、S08、S09、S16。

入口：[DataIncrement.isEmpty](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/io/DataIncrement.java:83)、[DynamicBucketIndexMaintainer.prepareCommit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/index/DynamicBucketIndexMaintainer.java:81)、[TableCommitImpl.filterAndCommit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:205)、[filterAndCommitMultiple](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:265)。

**源码事实**：DataIncrement.isEmpty 检查五类集合，包括 index；动态桶 maintainer 在 modified 时写 index、清除 modified，否则返回空列表。filterAndCommit 按 identifier 和 messages 创建 committable，排序后先 filterCommitted，再对待重试集合检查文件并提交。

**设计推导**：不能只看新数据文件来证明最终尝试没有业务增量；提交恢复需要原 identifier/messages。最终提交不能以重新 prepare 替代原 envelope。**实施验证**：最终消息隔离、六模式、精确 pending、状态保存故障、callback 屏障用例通过；Paimon 源码本身仍不构成端到端 offset 原子性的证明。

### P12：版本证明的复核命令与结果

核对日期为 2026-09-05；方法是比较本地源码文件字节与 Maven 1.3.2 sources JAR 对应 entry，而不是仅凭本地 POM 版本号或 Git 分支名。

```bash
python3 - <<'PY'
from pathlib import Path
from zipfile import ZipFile
import hashlib

root = Path('/Users/SL/javaProject/paimon')
entries = {
    'paimon-api': ['CoreOptions.java'],
    'paimon-common': ['fs/hadoop/HadoopFileIO.java'],
    'paimon-core': [
        'table/sink/TableWriteImpl.java', 'table/sink/TableCommitImpl.java',
        'table/sink/CommitMessage.java', 'table/sink/CommitMessageImpl.java',
        'operation/AbstractFileStoreWrite.java', 'compact/CompactFutureManager.java',
        'compact/CompactTask.java', 'mergetree/MergeTreeWriter.java',
        'mergetree/MergeSorter.java', 'mergetree/compact/MergeTreeCompactManager.java',
        'mergetree/compact/MergeTreeCompactTask.java', 'append/AppendOnlyWriter.java',
        'append/BucketedAppendCompactManager.java', 'utils/RecordWriter.java',
        'index/DynamicBucketIndexMaintainer.java', 'disk/IOManagerImpl.java',
        'disk/FileChannelManagerImpl.java', 'postpone/PostponeBucketWriter.java',
        'operation/PartitionExpire.java', 'operation/FileStoreCommitImpl.java',
        'io/DataIncrement.java', 'mergetree/compact/FileRewriteCompactTask.java',
    ],
}
matched = 0
for module, files in entries.items():
    jar = (Path.home() / '.m2/repository/org/apache/paimon' / module
           / '1.3.2' / (module + '-1.3.2-sources.jar'))
    print(module, 'sha256=' + hashlib.sha256(jar.read_bytes()).hexdigest())
    with ZipFile(jar) as sources:
        for name in files:
            entry = 'org/apache/paimon/' + name
            local = root / module / 'src/main/java' / entry
            assert local.read_bytes() == sources.read(entry), str(local)
            matched += 1
            print('MATCH', module, name)
print('SOURCE_MATCH_COUNT=' + str(matched))
PY
```

本次结果：24/24 MATCH；未发现上述接缝的源码差异。缺少 sources JAR、entry 不存在、类型或实现变化时，该命令失败，必须重新核对，不能切换到另一版本继续宣称通过。

证据结论：P01–P11、P13–P14 的“源码事实”已完成文本与版本核对；“设计推导”记录了成立前提；S01–S19 已完成本地实施验收；实际结果另见[最终修复对比与生命周期总览](../reviews/SPILL-修复前后对比与生命周期总览.md)。历史逐项过程记录已精简，本文保留 P 编号源码证明。

### P13：原生取消吞掉路径要求独立记录控制失败

**对应要求**：§7.1；S08、S11。

入口仍为 [CompactFutureManager.innerGetCompactionResult](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java:47)：Paimon 捕获 CancellationException，返回空结果并清空 taskFuture。`Future.get()` 被唤醒时，连接器对 cancel 返回后的记录不一定已经发生；因此“Future 包装器在 cancel 返回前记录异常”本身没有跨线程线性化保证。

**已执行的有限验证**：2026-09-05 使用 JDK 17 JShell、真实 FutureTask 和 CountDownLatch，确定地停在 delegate.cancel 已完成但包装层尚未记录的窗口。无锁审计读到无失败；共用短锁后，审计线程停在 BLOCKED，放行记录后必然读到失败。结果为 `UNLOCKED_AUDIT_MISSED_CANCEL=true`、`LOCKED_AUDIT_BLOCKED=true`、`LOCKED_AUDIT_OBSERVED_FAILURE=true`。

**设计推导**：取消状态迁移/记录与提交前审计必须共用短锁，并约束封闭后无合法活跃取消或新任务来源。这个 JDK 级探针验证锁与记录的竞争窗口，不等于新的连接器适配器、Paimon 全链路或 Maven 测试通过；连接器完整用例已在 S08/S11 通过，运行记录见实施验收报告。

### P14：CommitMessage 的接口边界

**对应要求**：§6.4、§7.2；S16。

入口：[CommitMessage 接口](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessage.java:34)、[CommitMessageImpl 方法](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessageImpl.java:79)。

**源码事实**：CommitMessage 仅声明 partition、bucket、totalBuckets；newFilesIncrement、compactIncrement、isEmpty 均属于具体的 CommitMessageImpl。已用下列命令核对本机 1.3.2 二进制 JAR，声明与源码一致：

```bash
/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home/bin/javap -classpath /Users/SL/.m2/repository/org/apache/paimon/paimon-core/1.3.2/paimon-core-1.3.2.jar org.apache.paimon.table.sink.CommitMessage org.apache.paimon.table.sink.CommitMessageImpl
```

**设计推导**：在单一版本适配边界进行显式类型验证后才能检查增量；未知 CommitMessage 类型须硬失败。这是必要的内部类型接缝，不应向 Service 的业务协调逻辑扩散强制转换。


### P15：一次纯 Compaction envelope 的原生 Snapshot 数量

2026-09-05 实施特征测试：旧 Context 停止后仍为第 3 个 Snapshot；新 STOP 确认最终结果后到第 5 个 Snapshot，最终 kind=COMPACT。源码：[StreamWriteBuilderImpl.newCommit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java:76) 明确使用 `ignoreEmptyCommit(false)`；[FileStoreCommitImpl.commit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java:323) 先提交 APPEND，再在 376 行处理 COMPACT。两次 Snapshot 使用同一个 envelope identifier。主测试据此断言 +2；不调整 connector 的 ordinary commit 或 ignoreEmptyCommit 行为。

固定源码链接：[StreamWriteBuilderImpl 1.3.2](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java#L76)、[FileStoreCommitImpl 1.3.2](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L323)。

P15 补充版本校验：`StreamWriteBuilderImpl.java` 与 `paimon-core-1.3.2-sources.jar` 对应 entry 字节一致；连同 P12 的 24 个文件共核对 25 个固定版本文件。异常分类同时检查 cause 和 suppressed 异常链，线程中断标志也作为不可豁免控制失败。

### P16：POSTPONE 可见性与构造回滚的补充证明

2026-09-05 实施审查追加核对 `GlobalIndexAssigner`、`DataTableBatchScan`、`DataTableStreamScan`，均与 1.3.2 sources JAR 字节一致，连同 P12/P15 共 28 个文件。

- [GlobalIndexAssigner.open](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java:114) 建立 RocksDB 与 bootstrap Spill；[close](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java:276) 关闭 stateFactory。连接器必须在 open/bootstrap 的 Exception 或 Error 后关闭已取得的 assigner；关闭失败必须传播不完整清理信号，不能让外层 Factory 错把未知策略资源当作已释放。
- [DataTableBatchScan](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableBatchScan.java:69) 对 POSTPONE 使用 `onlyReadRealBuckets()`，因此默认 BatchScan 不展示已写入 bucket=-2 的文件。S16 使用原生 StreamScan（changelog-producer=none）验证该模式已经确认的数据，并同时断言 BatchScan 仍为空；该原生可见性边界保持，不为测试修改路由或扫描语义。
- 无 Context 的 DDL 同样必须取得物理 owner。原生 [TableCommitImpl.truncateTable](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:174) 进入文件表提交，不能绕过仍活跃的其他 Service writer。

这些检查扩充 S01/S07/S14/S16 的实现证据，不改变用户确认的成功、弃 Compaction 与硬失败边界。实施任务分为 SS12a/SS12b/SS12c，避免把构造回滚问题藏在单个大任务内。


### P17：A/B Engine 调度与跨进程保护的作用域

**新增事实（用户提供，2026-09-05）**：A 上任务失败后通常调度到 B；A/B 在短时间内都恢复时，用户记忆中 A 可能优先。该优先规则尚不能写成已核验的生产调度算法。新方案必须同时容纳回到 A 和迁移到 B，不能依赖调度位置保证安全。

| 情况 | Spill 行为 | 提交交接条件 |
| --- | --- | --- |
| A JVM 已退出，调度 B 或重启后的 A | 旧进程无法再访问 channel；新的 IOManager 创建独立 UUID 目录；旧目录由能访问原磁盘的 cleaner 按 marker/锁/grace 清理 | 按宿主交付的任务状态恢复；不能复用旧内存 writer/Future |
| A JVM 仍活跃，旧任务已完整关闭后调度 B | A 的目录在完整关闭后删除，B 创建自己的目录 | 不存在旧实例继续提交，符合顺序交接前提 |
| A 仍在 STOP/Compaction/commit，B 已开始运行 | 共享目录上的 advisory FileLock 保护 A 的 Spill；不同本地磁盘互不清理 | JVM 内 owner 不能跨 Engine 排他，须由宿主证明旧写入者已停止或由真正 fencing 拒绝旧提交 |
| A/B 位于不同主机且临时磁盘不共享 | B 不能清理 A 的本地残留；A 恢复后才有本地清理机会 | “路径字符串相同”不代表同一个磁盘，也不提供远端表租约 |

**源码事实**：Paimon 1.3.2 [FileChannelManagerImpl.createFiles](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java:74) 每次用 UUID 创建目录，`close()` 只删除该实例保存的 paths。连接器 `PaimonSpillDirCleaner` 的 marker 文件锁限制本地删除；`PaimonService.ACTIVE_PHYSICAL_TABLE_OWNERS` 为静态 Map，作用域不覆盖其他 JVM。

**源码事实**：固定版本 [BucketMode](/Users/SL/javaProject/paimon/paimon-common/src/main/java/org/apache/paimon/table/BucketMode.java:40) 明确 HASH_DYNAMIC 不支持并发写入、KEY_DYNAMIC 使用本地完整主键索引。该文件已与 Maven 1.3.2 common sources JAR 字节一致，引用源码总数由 28 增至 29。

**源码事实**：[FileStoreCommitImpl.filterCommitted](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java:260) 按同 commitUser 的最新 identifier 过滤；[tryCommitOnce](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java:954) 的重试成功判断使用 user/identifier/kind。它们不是带 epoch 的分布式租约，不能据此宣称旧 Engine 被拒绝。连接器 stateMap 的 put/get 校验同样不能代替提交端 fencing。

**补充验收 S20 / SS20**：使用真正独立 JVM 验证共享本地目录中的 live lock、进程退出后的 lock 释放与残留清理、无 marker 目录跳过；测试不得依赖父 JVM 的 LIVE_DIRS，也不把本机文件锁测试延伸为 NFS 或跨 Engine writer fencing 的证明。真实子 JVM 两项与完整 629 项构建已通过；这只证明目录互斥与回收。下节本地 Engine 核对已发现缺少旧实例终止保证，故障切换整体不能写成完全安全。


#### P17.1 本地 Engine 版本与分配规则

核对仓库 `/Users/SL/javaProject/tapdata`，HEAD `ba3533f47fc3c43e9f0dc408f5e871986f1e8626`，分支 `develop`；本地 upstream 比较显示落后 11 个提交，未 fetch 或修改仓库。所审阅生产文件相对该 HEAD 无修改。尚无生产镜像/JAR/提交指纹，因此下面是**该本地版本的源码结论**，不直接断言事故环境与其相同。

- [TaskScheduleServiceImpl.cloudTaskLimitNum](/Users/SL/javaProject/tapdata/manager/tm/src/main/java/com/tapdata/tm/task/service/impl/TaskScheduleServiceImpl.java:174)：若任务仍记录 `agentId=A` 且 A 的 worker 心跳可用，保留 A。手工组配置还受 `priorityProcessId` 影响。
- [WorkerServiceImpl.calculationEngine](/Users/SL/javaProject/tapdata/manager/tm/src/main/java/com/tapdata/tm/worker/service/WorkerServiceImpl.java:405)：旧 A 通过较严格的心跳过滤时直接返回；不满足时按可用节点的权重、剩余容量等选择，完全相同受返回顺序影响（450 行起）。因此不是无条件随机，也不是无条件切 B；“A 恢复及时则可能优先”有源码支撑。
- 普通任务错误与 Engine 失联分开：[TapdataTaskScheduler](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/TapdataTaskScheduler.java:674) 的可重试错误先等待本机 `TaskClient.stop()` 成功，再在本机重新入队；不可重试错误先停止再上报 ERROR。这条路径本身不选择 B，B 接管还需 HA/新一次 START 等调度事件。

#### P17.2 Required E01：新 Engine 启动缺少旧代次终止屏障

| 源码入口 | 实际条件与行为 | 推论 |
| --- | --- | --- |
| [TaskRestartSchedule.engineRestartNeedStartTask](/Users/SL/javaProject/tapdata/manager/tm/src/main/java/com/tapdata/tm/schedule/TaskRestartSchedule.java:91) | 每 10 秒扫描 RUNNING 且任务 pingTime 过期，重新确认仍过期后发 OVERTIME、调用 scheduling | 心跳超时是失联检测，无法证明进程死亡 |
| [TaskStateMachineConfig](/Users/SL/javaProject/tapdata/manager/tm/src/main/java/com/tapdata/tm/statemachine/config/TaskStateMachineConfig.java:70) | RUNNING + OVERTIME 直接到 SCHEDULING | 没有旧 A terminal ACK 屏障 |
| [TaskScheduleServiceImpl.scheduling](/Users/SL/javaProject/tapdata/manager/tm/src/main/java/com/tapdata/tm/task/service/impl/TaskScheduleServiceImpl.java:99) | 改 agentId，转 WAIT_RUN，立即 sendStartMsg | B 可在旧 A 结束之前启动 |
| [TaskScheduleServiceImplTest](/Users/SL/javaProject/tapdata/manager/tm/src/test/java/com/tapdata/tm/schedule/service/impl/TaskScheduleServiceImplTest.java:78) | 明确验证不先向旧节点发送停止消息、直接启动新节点 | 现有测试固定了立即重调度行为，没有交接证明 |
| [TaskPingTimeMonitor](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/monitor/impl/TaskPingTimeMonitor.java:93) | A 后续心跳更新 0 行或访问失败达到本地阈值，才 INTERNAL_STOP | 是新分配之后的异步自停；不是 B 准入前置条件 |
| [TapdataTaskScheduler.scanAndInternalStopRescheduledTasks](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/TapdataTaskScheduler.java:742) | 检出 agentId 改变后入停止队列；10 秒循环调用 stop | 旧 Compaction/同步提交可能还在执行 |
| [HazelcastTaskClient.stop](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/task/impl/HazelcastTaskClient.java:171) | 发 cancel 后检查当前 terminal；不是阻塞等待全部完成 | 取消请求本身不是 termination proof |
| [HazelcastPdkBaseNode](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastPdkBaseNode.java:451) | connectorStop 异常记录后继续收尾 | 不能用 Engine 状态替代 connector cleanupComplete |

附加边界：[TaskRestartSchedule](/Users/SL/javaProject/tapdata/manager/tm/src/main/java/com/tapdata/tm/schedule/TaskRestartSchedule.java:226) 可在停止重试达到阈值后由 OVERTIME 合成 STOPPED；[ClusterComponentStopService](/Users/SL/javaProject/tapdata/manager/tm/src/main/java/com/tapdata/tm/cluster/service/ClusterComponentStopService.java:64) 的组件下线也会直接重调度。因此“数据库状态为 STOPPED”与“旧进程真实死亡”均需分辨其来源。

**可执行反例**：A 与 TM 的心跳网络中断，但仍能访问 Paimon warehouse；TM 超时改派 B，B 开始写入；A 在后续发现失联/归属改变后进入无限等待 Compaction 的 STOP。此时本连接器正确保护自己的 Spill，却不能禁止另一个 JVM 提交。这是远端表单活缺口，不是本地文件删除互斥失败。

```mermaid
sequenceDiagram
    participant TM as TM 调度器
    participant A as Engine A 旧实例
    participant B as Engine B 新实例
    participant P as Paimon warehouse
    A->>P: 旧写入或同步提交仍在执行
    TM->>TM: 任务心跳过期，OVERTIME 到 SCHEDULING
    TM->>B: 改 agentId 后立即 START
    B->>P: 新实例开始写入与提交
    Note over A,B: 当前缺少旧代次终止确认，可能重叠
    A->>TM: 后续心跳被拒绝或归属检查失败
    A->>A: INTERNAL_STOP，等待 Compaction 与资源关闭
```

#### P17.3 Required E02：旧领取操作可覆盖新调度

[TapdataTaskScheduler.scheduledTask](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/TapdataTaskScheduler.java:343) 初查有 `agentId=instanceNo,status=WAIT_RUN`，但 352 行逐条认领重新构造的 query 只剩 `_id`，`addAgentIdUpdate` 无条件把 owner 改成本机。底层 [HttpClientMongoOperator.findAndModify](/Users/SL/javaProject/tapdata/iengine/iengine-common/src/main/java/com/tapdata/mongo/HttpClientMongoOperator.java:713) 还是“读取 → update → 再读取”，不能凭方法名声称单次原子认领。

**可执行反例**：B 初查到 assigned-B/WAIT_RUN 后暂停；TM 重派 A，A 已 RUNNING；B 恢复后仅按 `_id` 将 owner 覆写 B；[TaskServiceImpl.runningInternal](/Users/SL/javaProject/tapdata/manager/tm/src/main/java/com/tapdata/tm/task/service/TaskServiceImpl.java:5013) 对 owner 检查已通过的 RUNNING 记录返回成功，B 启动。A 直到后续检查才自停。即使收紧 E01 的等待条件，E02 仍需独立修复。

#### P17.4 后续 Engine 修正规约与未实现状态

以下为明确的修正方向，**尚未修改 Engine、尚未执行 Engine 测试**。它扩展原 §13“仅连接器”的范围，不混入已通过的 629 项成绩：

1. 每次分配拥有不可变的 assignment generation。领取必须在服务端单次原子操作中匹配 `_id + WAIT_RUN + expectedAgentId + expectedGeneration`；未匹配不得改 owner、不得返回成功授权启动。不能仅给现有客户端先查再改方法添加“原子”注释。
2. 新代次的写入准入须依赖旧代次 terminal ACK；ACK 至少证明旧业务/提交调用已经结束、connector close 已完成且没有仍可访问/提交的资源。Jet 取消已请求、手工改状态、心跳过期都不合格。ACK 必须绑定旧代次，延迟报告不能释放新代次。
3. 若 A 不可达，只能由受信任宿主证明旧进程/Pod 确实终止，或在目标端实现能拒绝旧代次提交的真实 fencing。没有此证明时保持 WAIT_HANDOVER（概念状态名，尚未指定现有枚举实现），持续 INFO，不把超时当成功。
4. 单独增加 Engine 故障注入门禁：A 心跳丢失但 warehouse 连通；旧 STOP 超过 35 秒；B 领取暂停后重派 A；A→B→A 的 ABA；旧 ACK 晚到；connector close 错误被 Engine 捕获；进程真实死亡。断言新写入准入与旧代次终止之间具有先后屏障。

这不是把同一个 Paimon snapshot 提交锁当成 writer 租约。只序列化某一次 commit 仍不能保护 HASH_DYNAMIC/KEY_DYNAMIC 的整个路由与索引生命周期；也不能通过更换 commitUser 或增大心跳超时关闭这些窗口。


#### P17.5 用户选择：不改 Engine，评估 connector 写入准入

用户已明确本轮不修改 Engine。E01/E02 的源码发现作为运行边界保留，P17.4 不再作为默认后续实施任务。只改 connector 可以阻止两个任务实例**同时获得 Paimon 写入资格**，但不能阻止 Engine 调度两个任务，也不能保证锁竞争获胜者就是调度器的最新 assignment。写入互斥与“最新代次拥有写权”是不同保证。

候选方案为**物理表级、覆盖整个 writer 生命周期的跨进程排他锁**。该方案尚未实现，不属于 629 项已通过测试的行为：

1. 所有实例使用同一个规范化物理表身份、同一锁域。在 state bind、动态桶 preflight、writer/index 初始化及任何有提交能力的临时 DDL 操作之前取得锁；未取得时不创建写入资源、不确认业务成功。
2. 持锁覆盖普通写入、业务 drain、最终 prepare/提交、原生 SYNC maintenance、实际 Compaction termination、writer/committer/IO 清理。只在取得完整关闭证明后释放；构造或关闭不完整时保留。等待日志使用 INFO，超时不产生抢锁许可。
3. **同机且共享同一本地文件系统**：可用物理表摘要对应的稳定 FileLock 文件。它必须位于不会被 Spill cleaner 或临时目录清理者删除的专用目录，不能复用每个 IOManager 各自的 UUID marker；锁文件不在解锁后删除重建，避免 inode 分裂。保证整个 JVM 对该锁文件的 channel 所有权统一，并要求所有参与写入的 connector 版本遵守相同协议。JVM 真实终止会释放 OS 锁；任务卡住但进程仍存活则持续持锁，另一 Engine 等待。
4. **跨机器**：本地文件锁无效。需要所有实例可见、原子语义已验证的协调存储。无自动过期的持久排他锁可阻止抢占，但持锁进程崩溃后不能仅靠时间自动清除，须有可靠旧进程终止证明或人工安全释放。可过期租约要支持真正的 stale-writer fencing，不能只在 commit 前做一次“租约还在”检查；该检查与实际 Snapshot 发布之间仍有 TOCTOU。
5. 本轮尚未确定 A/B 的同机/跨机与共享锁目录条件，不选定存储实现，不增加依赖，不改变已验收 STOP 协议。若没有可靠协调与 fencing，又要求失联后立即自动接管，则不能承诺同时满足无双写和可用性。

**Paimon 1.3.2 证据**：新增核对 `RenamingSnapshotCommit`、`CatalogEnvironment`、`JdbcCatalogLock`、`JdbcUtils`，4/4 与 Maven core 1.3.2 sources JAR 字节一致。前述 29 文件加上这 4 个，共核对 33 个引用文件；这些是可行性源码核对，不是新增协议运行验收。

- [RenamingSnapshotCommit.commit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/catalog/RenamingSnapshotCommit.java:50)：`lock.runWithLock` 包住 Snapshot 是否存在检查和原子写入，不覆盖 writer/index 生命周期。
- [CatalogEnvironment.snapshotCommit](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/table/CatalogEnvironment.java:96)：没有 CatalogLockFactory 时用 `Lock.empty()`，不能把默认 FileSystemCatalog 当成已配置分布式写入锁。
- [JdbcCatalogLock.runWithLock](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/jdbc/JdbcCatalogLock.java:56) 仅持锁执行 callable；[JdbcUtils.acquire](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/jdbc/JdbcUtils.java:493) 会处理过期记录。不能把这个有过期回收的提交锁直接延长成无限期 writer 租约并宣称安全。
- [KVMap](/Users/SL/javaProject/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/entity/utils/cache/KVMap.java:4) 只有普通 put/get/putIfAbsent/remove 等接口，没有显式租约、按 owner 条件释放或发布时 fencing 契约。`PdkStateMap` 当前适配也不能由这个接口推导出跨分区线性一致、存储持久化与快照提交绑定。
- [JDK 17 FileLock 官方契约](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/channels/FileLock.html)：锁随 release、channel close 或 JVM 终止而释放；锁按 advisory 协议使用。某些平台关闭同文件的其他 channel 也会释放 JVM 持锁，因此实现须统一 channel 所有权；网络文件系统须单独验证，不能外推本机结果。


## P18：2026-09-06 回收与查询准入缺口修复

本节补充目录范围与准入契约，继续遵守 SYNC、无超时 Compaction 终止屏障；不实现跨机器持久所有权锁。

- KEY_DYNAMIC writer 与 HASH_DYNAMIC preflight 的 GlobalIndexAssigner 必须通过约束 IOManager 视图取得临时根：`tempDirs()` 返回已登记的 `paimon-io-*`，channel、reader、writer 操作仍委托原 IOManager。原始 IOManager 的关闭权和终止屏障保持不变。
- 启动扫描根为全局 diskTmpDir 与当前 tableConfig 中有效字符串覆盖值的并集；仍执行 live 登记、owner 文件锁、宽限期和符号链接保护。配置中已移除的旧根、无 marker 的历史 rocksdb-* 不自动删除，需要停写后的人工处理。跨机器只清理实际可访问的本地磁盘。
- queryByAdvanceFilter 从 Catalog 访问到 reader 清理和最终 consumer 回调，全程持有生命周期准入；STOP 拒绝新查询并等待在途查询完成。
- 删除仅测试使用的 unregisterLiveDirs 快捷入口；测试显式调用 releaseAfterClose。合并重复 DRAIN 设置，缓存最终 Compaction 校验结果，但保留校验发生在 sealFinalPrepare 之前的顺序。

### P18.1 固定内核来源

模块固定 Paimon 1.3.2、Hadoop 3.3.6。本轮已逐字节比较下列前三个文件与本地 paimon-core-1.3.2-sources.jar，均一致。

| 来源 | 源码事实与修复约束 |
| --- | --- |
| [GlobalIndexAssigner.java:136](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java:136) | open 在 ioManager.tempDirs() 下建立 rocksdb-UUID；close 在 276 行关闭 stateFactory 并删除目录，进程崩溃不会执行该路径。 |
| [IOManagerImpl.java:91](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java:91) | tempDirs 返回原始根，不能当成已登记的 Spill 子目录；因此使用委托视图。 |
| [FileChannelManagerImpl.java:125](/Users/SL/javaProject/paimon/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java:125) | close 递归删除其管理的 paimon-io-*；索引必须位于该树内才能被相同目录屏障与崩溃清理覆盖。 |

官方固定版本源码：[GlobalIndexAssigner](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L136)。两个历史修复 2a4c4db0、5656a63d 不在 bdd9d537 的祖先链，本轮适配其目录修复思路，不恢复历史 maintenance/retained 协议。

### P18.2 验证门禁

修复前真实 KEY_DYNAMIC 目录布局、表级根回收、查询 activeIngress 三项回归均失败（/tmp/paimon-gap-red.log）。修复后需验证实际 RocksDB 位于受保护目录、preflight 视图、跨 JVM 活跃保护与崩溃嵌套回收、查询等待及停止后拒绝，并执行完整模块测试。成绩记录在本轮实施验收中，不覆盖或改写历史 629 项与补充 3 项的原记录。


</details>
