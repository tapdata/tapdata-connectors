# 评审与证据：Paimon Spill 所有权隔离（2026-11-12）

> 关联规范：[SPEC-paimon-spill-owner-isolation.md V0.3](../specs/SPEC-paimon-spill-owner-isolation.md)\
> 关联计划：[plan.md](../../../tasks/spill-owner-isolation/plan.md)、[todo.md](../../../tasks/spill-owner-isolation/todo.md)\
> 生命周期图：[SPILL-所有权隔离-修改前后对比与生命周期.md](SPILL-所有权隔离-修改前后对比与生命周期.md)\
> 证据基线：tapdata-connectors develop `e3f68298`；Paimon `release-1.3.2`（`c05f7d1f`）；Flink Runtime 1.20.1 sources JAR；Oracle JDK 17.0.20 `lib/src.zip`。\
> 可执行证据：本地 PR #2（`213f9c49`）新增的特征化测试。本文只陈述已核实的事实；未核实项单独标注。

## 1. 结论摘要

1. 现场异常是 spill 目录在 IOManager 生命周期内被删除后，Paimon 1.3.2 不重建目录，下一次 `MergeSorter.spill` 以 ENOENT 失败。
2. develop 中唯一能删除他人 `paimon-io-*` 目录的路径是 `PaimonSpillDirCleaner.cleanupStaleSpillDirs`。它对同进程已持有的锁文件做探测后关闭 channel，按 POSIX 记录锁语义释放了 owner 进程的锁；随后另一进程的同一清理器取得锁，并以目录空闲时长判定删除。
3. 该因果链已用 develop 现有代码端到端复现，并通过变异检验确认“探测后关闭”是必要条件（§4）。
4. V0.2 用 `RuntimeMXBean.getName()` 派生 jvmId 存在缺陷：该值随主机名解析结果变化，会让同一 JVM 的两个 ClassLoader 得到不同 jvmId，从而重新触发同一缺陷（§5）。V0.3 改为 pid 与 JVM 启动时间。
5. 修复能否根治现场故障取决于部署事实：删除动作必须由另一个能访问同一目录的进程完成（§8）。

## 2. 现场栈定位

### 2.1 Paimon 帧与 release-1.3.2 逐行一致

现场 `Caused by: java.io.IOException` 之后的 27 个 Paimon 帧，在 `release-1.3.2` 中逐行对应如下（行号：源码语句）：

| 帧 | 1.3.2 源码 |
| --- | --- |
| AbstractFileIOChannel:62 | `"Channel to path '" + channelID.getPath() + "' could not be opened.", e` |
| BufferFileWriterImpl:31 | `super(channelID, true)` |
| IOManagerImpl:145 | `return new BufferFileWriterImpl(channelID)` |
| FileChannelUtil:53 | `ioManager.createBufferFileWriter(channel)` |
| MergeSorter:171 / 153 / 111 | `FileChannelUtil.createOutputView(` / `readers.add(spill(supplier))` / `return spillMergeSort(` |
| MergeTreeReaders:90 / 56 / 64 | `mergeSorter.mergeSort(` / `readerForSection(` / `ConcatRecordReader.create(readers)` |
| ConcatRecordReader:53 | `readers.size() == 1 ? ... : new ConcatRecordReader<>(readers)` |
| MergeTreeCompactRewriter:113 / 84 / 73 | `readerForMergeTree(` / `readerForMergeTree(` / `rewriteCompaction(...)` |
| MergeTreeCompactTask:161 / 156 / 110 | `rewriter.rewrite(...)` / `rewriteImpl(...)` / `rewrite(candidate, result)` |
| CompactTask:49 / 34 | `doCompact()` / 类声明（桥接方法帧） |
| CompactFutureManager:67 / 53 | `taskFuture.get()` / `obtainCompactResult()` |
| MergeTreeCompactManager:251 | `innerGetCompactionResult(blocking)` |
| MergeTreeWriter:305 / 247 / 253 | `compactManager.getCompactionResult(blocking)` / `trySyncLatestCompaction(...)` / `flushWriteBuffer(...)` |
| AbstractFileStoreWrite:215 | `writerContainer.writer.prepareCommit(waitCompaction)` |
| MemoryFileStoreWrite:152 | `super.prepareCommit(waitCompaction, commitIdentifier)` |
| TableWriteImpl:262 | `write.prepareCommit(waitCompaction, commitIdentifier)` |

spill 文件路径的构成：`MergeSorter.spill` 调用 `ioManager.createChannel()`，`FileChannelManagerImpl.createChannel` 在构造时确定的 `paths[num]` 下拼出 `<32 hex>.channel`；`AbstractFileIOChannel` 以 `new RandomAccessFile(path, "rw")` 打开。父目录不存在时，`open(2)` 返回 ENOENT，Java 报 `FileNotFoundException: ... (No such file or directory)`。

连接器依赖为 paimon-core 1.3.2（`mvn dependency:tree`）。Maven `paimon-core-1.3.2-sources.jar` 与 git 标签中 16 个相关文件逐字节一致：IOManagerImpl、FileChannelManagerImpl、AbstractFileIOChannel、FileIOChannel、FileChannelUtil、BufferFileWriterImpl、MergeSorter、MergeTreeWriter、MergeTreeCompactManager、MergeTreeCompactTask、CompactFutureManager、CompactTask、AbstractFileStoreWrite、MemoryFileStoreWrite、TableWriteImpl、GlobalIndexAssigner。

### 2.2 Connector 帧

- 现场栈中出现 `PaimonCompactionExecutor$NativeCompactionFailure` 与 `$GuardedFuture`，这两个类在 develop 中由 `962083ff`（2026-09-06）引入。因此现场包不早于该提交，已包含连接器自有 compaction executor 与 IOManager 关闭前的 termination 屏障。
- `PaimonService` 帧行号（1414、1451、1580、2343）与 develop 上 `7c40958f`（2026-09-08）至 `2841ae39`（2026-10-27）之间的修订一致。
- 错误出现在 `PaimonAsyncCommitScheduler → flushScheduledTable → commit → prepareCommit` 的正常提交路径中，不在 STOP 路径上，因此排除 owner 自身关闭与 compaction 并发的竞态。
- 2026-09 现场包在 9 月 hotfix Spec 中记为**推定** `496a4d7a`（07-30 构建）。该提交的 `OwnerLock.tryAcquire` 包含相同的“冲突后关闭”代码。

## 3. 根因因果链

| 编号 | 事实 | 来源 |
| --- | --- | --- |
| F1 | JDK 在 JVM 级维护 `FileLockTable`，以 `FileKey`（设备号 + inode）为键，由 bootstrap 加载，所有 ClassLoader 共享；对已持锁区间再次 `tryLock` 抛 `OverlappingFileLockException` | JDK 17 `sun/nio/ch/FileLockTable.java:44,69,94,222`；`FileChannelImpl.java:1311-1321` |
| F2 | Linux/macOS 上的 `FileChannel.lock/tryLock` 是 POSIX `fcntl` 记录锁，锁的所有者是进程；进程关闭同一文件的任一 fd，都会释放该进程在该文件上的全部记录锁。JDK `FileLock` 文档明确提示“同一程序内应使用唯一 channel 加锁” | POSIX `close(2)`/`fcntl(2)`；JDK `FileLock` javadoc；PR #2 C1 |
| F3 | 其他进程关闭自己的 fd 只释放它自己的锁 | PR #2 C2 |
| F4 | 存活登记 `LIVE_DIRS`/`OWNER_LOCKS` 是 static 字段，作用域是定义它的 ClassLoader；PDK 每次（重新）加载 jar 都创建新的 `DependencyURLClassLoader` | `PaimonSpillDirCleaner.java`；`ExternalJarManager.java:231`；Engine `PdkUtil.java:99/101` |
| F5 | 注册顺序为先取锁（`:269`）、后登记 `LIVE_DIRS`（`:284`）；目录在此之前已物化（`:350`）。同 ClassLoader 并发扫描若在 `:402` 读到“未登记”，就会在 `:413` 探测、在 `:584` 关闭 | `PaimonSpillDirCleaner.java` |
| F6 | 每次 `onStart`（包括 connectionTest）都会执行扫描 | `PaimonConnector.java:128`；`PaimonService.java:307` |
| F7 | 锁丢失后，同 JVM 的后续探测仍因 F1 抛 Overlapping 而跳过，同进程删不掉；另一进程可以取得锁，再以“目录及直接子项的 mtime 早于 10 分钟”判定删除 | `PaimonSpillDirCleaner.java:413-442` |
| F8 | Paimon 1.3.2 `FileChannelManagerImpl` 只在构造时 `mkdirs`，`close()` 只删除自己的 `paths`；目录被删后不会重建 | `FileChannelManagerImpl.java`（release-1.3.2）；PR #2 C10 |

合取条件：现场故障 ⇔（F4 或 F5 触发同进程探测）∧（另一进程在同一文件系统上运行旧清理器）∧（目录空闲 ≥ grace）。

## 4. 可执行证据（本地 PR #2）

执行环境：macOS（Darwin 27.2）、Oracle JDK 17.0.20、apfs、Maven 3.9.16 离线。共 13 项，全部通过，无跳过。

| ID | 用例 | 证明的事实 |
| --- | --- | --- |
| C1 | `sameJvmProbeCloseReleasesOwnerProcessLock` | F1+F2：探测后 owner `isValid()==true`，外部进程却能取得锁 |
| C2 | `otherProcessProbeDoesNotReleaseOwnerLock` | F3 |
| C3 | `runtimeNameDependsOnHostResolutionButPidAndStartTimeDoNot` | §5：同一 JVM 内 `getName()` 先后为 `pid@<host>` 与 `pid@localhost`，pid 与启动时间不变 |
| C4 | `exclusiveCreateFailsWithNoSuchFileWhenParentIsRemoved` | 父目录被 rmdir 后，`O_CREAT\|O_EXCL` 与 `mkdir` 均返回 ENOENT |
| C5 | `rmdirRemovesOnlyEmptyDirectoriesAndReportsMissingOnes` | rmdir 对非空目录返回 ENOTEMPTY，对已删除目录返回 ENOENT |
| C6 | `openWithoutCreateNeverRecreatesLockFile` | 不带 `O_CREAT` 的 open 不会产生新 inode |
| C7 | `unlinkBeforeReleaseLeavesLateOpenerWithUnlinkedInodeOnly` | 先 unlink 再释放时，迟到者 `acquired=true exists=false`；`fileKey` 非 null |
| C8 | `fileStoreTypeIsObservableForTemporaryRoot` | 本机 `FileStore.type()=apfs` |
| C9 | `paimonIoManagerAcceptsDuplicateRoots` | Paimon 1.3.2 对重复根目录各建一个 `paimon-io-*` |
| C10 | `paimonIoManagerNeverRecreatesDeletedSpillDirectory` | F8，且异常形态与现场一致 |
| R1 | `foreignClassLoaderProbeThenOtherJvmCleanupReproducesFieldEnoent` | 跨 ClassLoader 路径端到端复现现场异常 |
| R2 | `sameClassLoaderRegistrationWindowAlsoLosesOwnerLock` | 同 ClassLoader 注册窗口路径端到端复现 |
| R3 | `withoutSameJvmProbeOtherJvmCleanupKeepsLiveSpillDirectory` | 对照组：没有同进程探测时，外部进程即使 grace=0 也删除 0 个 |

**变异检验**：把 `OwnerLock.tryAcquire` 在 `OverlappingFileLockException` 后改为不关闭探测 channel，R1、R2 失败（“探测关闭后 owner 的 OS 锁已丢失 ==> expected <true> but was <false>”），R3 仍通过；源码已还原。这说明“同进程探测后关闭”是缺陷的必要条件。

**未覆盖**：Linux 未实测（本机 Docker 未运行），留待 Plan AC-24。

## 5. jvmId 派生缺陷（评审 Critical）

- JDK 17 `sun/management/VMManagementImpl.java:140-150`：`getVmId()` 每次调用都执行 `InetAddress.getLocalHost().getHostName()`，遇到 `UnknownHostException` 时退回 `"localhost"`；`RuntimeImpl.getName()`（`RuntimeImpl.java:58-60`）直接返回该值，不缓存。
- JDK 17 `java/net/InetAddress.java:1604-1607,1666-1675`：本机地址只缓存 5 秒（`expiryTime = now + 5s`），过期后以“不使用缓存数据”的方式重新解析，失败即抛 `UnknownHostException`。
- 后果：同一 JVM 的两个 ClassLoader 若在不同时间首次计算 jvmId，而主机名解析在两次之间发生变化（容器 `/etc/hosts` 缺项、DNS 抖动），两者会得到不同的 jvmId。其中一个 ClassLoader 会把另一个的命名空间当作外部命名空间去探测，重新触发 F1+F2，恢复原缺陷。
- 实测：C3 在 JDK 17 与 JDK 11 下得到同样结果（原型输出 `3404@ConradJamdeMac-Studio.local` → `3404@localhost`，pid 3404 不变）。
- 修复：`RuntimeMXBean.getPid()` 自 JDK 10 起为 default 方法，实现为 `ProcessHandle.current().pid()`，不做名称解析（`RuntimeMXBean.java:78-81`）。生产代码以 `<release>8` 编译，需通过反射调用；Java 8 运行时退回 `getName()` 中 `@` 之前的部分（该部分来自本地 `getProcessId()`，与主机名无关）。`getStartTime()` 是 JVM 启动时刻常量。

## 6. Paimon / Flink 源码核实

| 事实 | 位置（Paimon 1.3.2 除另注） | 对本设计的约束 |
| --- | --- | --- |
| IOManager 懒创建 FileChannelManager；`close()` 只关闭已创建的 manager | `IOManagerImpl.java` | 必须先物化目录再登记 owner |
| 每个根各建一个 `paimon-<prefix>-<UUID>`，mkdir 失败时跳过该根；构造后不再 mkdirs | `FileChannelManagerImpl.java` `createFiles` | 目录被删后不可恢复；必须校验每个根都已物化；重复根各建一个目录（C9） |
| 注释写“先注册 shutdown hook 再建目录”，但 Paimon 版本没有 hook | `FileChannelManagerImpl.java:62` | 进程正常退出时 Paimon 不清理；由 owner 退役或后继回收负责 |
| `GlobalIndexAssigner.open` 在 `ioManager.tempDirs()` 下建 `rocksdb-<UUID>`，`close()` 只删除该子目录 | `GlobalIndexAssigner.java:136-141,276-284` | 保留 `withConfinedTempDirs`，把 RocksDB 限制在 lease 内 |
| `AbstractFileStoreWrite.close` 只在自建 executor 时 `shutdownNow`，不 `awaitTermination` | `AbstractFileStoreWrite.java:86,149,304-312` | 连接器注入 executor 并自行证明终止（V1.2 不变） |
| Flink sink 把 Paimon IOManager 嵌套在 Flink 进程的 spill 目录内，关闭顺序为 `write.close()` → `paimonIOManager.close()` | paimon-flink-common `StoreSinkWriteImpl.java:75,174,177` | 采用“嵌套归属”：`paimon-io-*` 位于 lease 的 `io/` 下 |
| 动态 bucket 索引同样在 Flink spill 目录内创建 IOManager，`close()` 先关 assigner 再关 IOManager | `GlobalIndexAssignerOperator.java:59-60,104-107` | 预检与写入使用同一 lease 约束 |
| Lookup 函数在 tmp 目录下建 `lookup-<UUID>`，只删除自己的目录 | `FileStoreLookupFunction.java:164,377`；`FullCacheLookupTable.java:340` | 只有 owner 删除自己的目录 |
| paimon-flink 中不存在扫描并删除他人临时目录的清理器 | `git grep`（release-1.3.2，paimon-flink/*/src/main） | 不保留基于时间的跨进程扫描 |
| 专用 append compaction worker 关闭时 `shutdownNow` 后等待 120 s，超时 WARN | `AppendCompactWorkerOperator.java:105-110` | 超时后不能直接执行破坏性关闭；保留 FAILED_RETAINED |
| CDC 多表 sink 逐表 `write.close()` 之后才 `shutdownNow` 共享 executor | `CdcRecordStoreMultiWriteOperator.java:227-236` | 不把该顺序当作删除许可 |
| Flink：每个 TaskManager 一个 `IOManagerAsync`；`FileChannelManagerImpl` 先注册 shutdown hook 再建目录；工作目录 `tm_<resourceId>` 启动时清空 `tmp`；flink-runtime 中没有扫描他人目录的清理器 | Flink 1.20.1 `TaskManagerServices.java:342`、`FileChannelManagerImpl.java:76-81`、`ClusterEntrypointUtils.java:171`、`WorkingDirectory.java:43` | 采用进程命名空间；崩溃残留由“后继进程”回收；不加 shutdown hook（理由见 Spec §4.4） |

YARN/K8s 部署下临时目录为容器私有，属于 Flink 部署行为；本地没有 flink-yarn 源码，未核对。

## 7. 评审问题处置（V0.2 → V0.3）

| # | 级别 | 问题 | V0.3 处置 |
| --- | --- | --- | --- |
| 1 | Critical | jvmId 由 `getName()` 派生，不稳定且可能阻塞在 DNS 上 | jvmId = H(pid, startTime)；pid 经反射 `getPid()` 取得；加入固定期望值测试与主机名无关性测试（Spec §4.2、AC-01） |
| 2 | Required | Plan T04/T05 顺序不可行：测试断言旧锁文件位置并反射 `OWNER_LOCKS` | 先引入测试抽象层并在旧布局上全绿，再切换生产实现（Plan T08→T09） |
| 3 | Required | owner 与 GC 的空目录 rmdir 存在两处竞态 | 创建阶段遇 ENOENT 换新 leaseId 有界重试；退役阶段最后 rmdir 遇 ENOENT 视为完成；锁释放后的 rmdir 只尽力而为（Spec §4.4、AC-17） |
| 4 | Required | INV-01 与 REQ-03 字面矛盾 | 改为“除 lease 创建者打开并持有自己的锁文件外”（Spec INV-01、§13） |
| 5 | Required | jvmId 推导与命名属于协议，但没有版本规则 | 任何变更必须升级子根版本；固定期望值测试守护（Spec §4.2） |
| 6 | Required | 根目录重复或互为别名时 leaseId 冲突，比现在多出一种失败 | 规范化后按真实路径去重，保持原顺序（Spec REQ-03、AC-20） |
| 7 | Required | AC-15 定义含糊 | 保留 loader A 中的活跃使用者线程，只丢弃 PDK 侧引用（Spec AC-15） |
| C1 | Consider | 未登记路径的释放静默成功 | 非空且未登记即判为释放不完整（Spec REQ-08、AC-21） |
| C2 | Consider | IOManager 已建成后某根未物化时的处理未定义 | 按 V1.2 语义保留资源（Spec REQ-03） |
| C3 | Consider | 审计取 hostname 需要名称解析 | 只读环境变量 `HOSTNAME`，不做解析（Spec REQ-10） |
| C4 | FYI | 每次 onStart 同步回收，没有上限 | 每次调用的回收数量设上限（Spec REQ-06） |
| N1 | Nit | 9 月现场包版本写成确定事实 | 改为“推定”，并补充 `962083ff` 的类存在性证据（本文 §2.2） |
| N2 | Nit | AC-07 提到“注入时钟” | 回收器没有时间输入，改为 mtime 篡改用例 |
| O1 | Optional | Spec 中混有故障叙述与取证内容 | 迁入本文，Spec 只保留规范性内容 |

## 8. 现场取证程序（OQ-04）与结论依赖

删除者必须是另一个能访问同一目录的进程（F7）。现场应执行：

~~~sh
# 1. 删除者：检索同机/共享卷上所有引擎的全部任务日志与引擎日志
grep -rn "<paimon-io UUID>" <所有引擎日志目录>
#   "Removed stale Paimon spill dir"                   → 旧清理器删除（写在发起扫描的那个任务/连接测试日志中）
#   "FileChannelManager removed spill file directory"  → Paimon IOManager.close 删除

# 2. 文件系统类型，以及有多少个 JVM 能访问该目录
findmnt -T /tapdata_cache; df -T /tapdata_cache
ps -ef | grep -i [t]apdata    # 容器部署时同时核对各容器的 volume 挂载

# 3. 生产锁状态（在正常运行的引擎上执行，不改变任何状态）
ENGINE_PID=<引擎 pid>
for f in /tapdata_cache/.paimon-io-*.tapdata-owner.lock; do
  ino=$(stat -c %i "$f")
  holder=$(awk -v ino="$ino" '$2 != "->" {n=split($6,a,":"); if (a[n]==ino) print $5}' /proc/locks)
  fdopen=$(ls -l /proc/$ENGINE_PID/fd 2>/dev/null | grep -cF "$f")
  echo "$f fd_open=$fdopen lock_pid=${holder:-NONE}"
done
# fd_open=1 且 lock_pid=NONE：引擎仍打开锁文件但 OS 锁已丢失，即 F2 已在生产发生
~~~

| 部署事实 | 结论 |
| --- | --- |
| 同机多个引擎 JVM 共享本地文件系统上的 diskTmpDir | V0.3 协议切断删除链路，可根治；以 AC-03/AC-24 为准 |
| diskTmpDir 为跨主机网络/FUSE 共享卷 | 锁语义不可依赖；V0.3 通过文件系统准入控制关闭回收，保证不误删，残留由运维处理 |
| 只有一个 JVM 能访问 diskTmpDir | 上述机制不能解释该故障，删除者在连接器之外，需另行取证 |
