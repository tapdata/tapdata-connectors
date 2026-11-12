# Spec：Paimon 本地 Spill 进程命名空间与死亡 owner 回收

> Spec ID：paimon-spill-owner-isolation  
> 版本：V0.2，2026-11-12；状态：用户已批准（2026-11-12），进入 Plan；尚未实现。V0.1 的变更见 §15。  
> 目标模块：tapdata-connectors / connectors/paimon-plus-connector。  
> 实施计划：[plan.md](../../../tasks/spill-owner-isolation/plan.md)；任务清单：[todo.md](../../../tasks/spill-owner-isolation/todo.md)。  
> 本文补充现有 SPEC-paimon-spill-sync-graceful-stop.md V1.2，不替代其业务提交、取消来源、超时预算与不可逆保留契约。

## 1. 已确认方向与待定项

### 1.1 用户已确认

| 决策 | 本次范围 |
| --- | --- |
| 不修改 Engine | 修改限于 Connector；不要求宿主注入共享服务，不修改 Paimon Core |
| 不做 JMX 协调 | 不注册 MBean、不调用 MBeanServer、不建 broker；只读 RuntimeMXBean 仅用于计算 jvmId（2026-11-12 按 Flink 对照方案确认） |
| 对齐 Flink 的所有权模型 | 目录按进程划分命名空间；删除权只属于 owner，或属于已证明 owner 进程死亡的回收者；不探测本进程的锁（2026-11-12 确认） |
| 表级隔离 | 每个写上下文/预检一个 lease 目录，带表标签；表级隔离用于归因与资源边界，不作为删除安全依据（2026-11-12 确认） |
| 部署范围 | 同机本地磁盘，覆盖多个 JVM、多个隔离插件 ClassLoader；网络/FUSE 文件系统上关闭自动回收 |
| 去掉跨 ClassLoader GC 串行 | V0.1 的 REQ-05 intern monitor 删除，改为并发回收容忍（2026-11-12 确认） |
| 回收取舍 | 本 JVM 失败残留保留到进程退出，由其他进程或同一引擎重启后的新 JVM 回收 |
| 超时策略 | 沿用现有 FAILED_RETAINED 契约；不自动终止 Engine，不增加本 JVM 自动恢复器 |
| 时间预算 | 保持 stopTimeoutSeconds=180、finalCompactionTimeoutSeconds=120、compactionCancelGraceSeconds=30 的可配置默认值 |

### 1.2 V0.2 补全的细节（2026-11-12 随 Spec 一并批准）

1. jvmId 获取失败时，写上下文初始化失败并报告，不回退旧平铺布局（REQ-02）。
2. 删除判定不再读取 mtime，也不保留 grace 延迟；存活只由操作系统文件锁证明（REQ-06）。
3. 文件系统门禁采用白名单，白名单外的类型关闭自动回收（REQ-07）；初始候选清单见该节。
4. 不照搬 Flink 的 JVM shutdown hook 删除目录（REQ-08）。

## 2. Objective

修复“本 JVM 内的清理扫描打开了本 JVM 仍持有的 owner 锁文件，关闭时让操作系统释放了 owner 的锁，随后另一个 JVM 拿到锁并删除活跃 spill 目录”的生命周期缺口。

成功标准：

- 本 JVM 的代码永远不打开本 JVM 命名空间下的锁文件，从而不可能弄丢自己的锁；
- 任何仍在使用的 lease 不会被任何遵守本协议的进程删除；
- 进程退出后的残留能被其他进程或同一引擎重启后的新 JVM 回收；
- 正常业务提交与现有停止语义不变。

本次视为一个完整能力：命名空间、lease 创建、owner 关闭与 GC 必须遵守同一协议才能证明安全，不能分别发布部分机制。表写入排他与跨机器故障转移是独立能力，本 Spec 不承诺补足。

### 2.1 范围

- Connector 管理的 Paimon IOManager 目录，以及经受限 tempDirs 视图放入其中的 RocksDB 临时资源。
- 写上下文 Factory、HASH_DYNAMIC 预检、正常 STOP、异常 STOP、Factory 回滚、DDL/rebuild、有限读退出相关的既有资源接缝。
- 多临时根目录及表级 diskTmpDir 覆盖；同机多个 JVM 和同 JVM 多个 PDK ClassLoader。
- 新协议目录的创建、关闭、失败保留、跨 JVM 残留回收、文件系统门禁、版本隔离与审计。
- 部署规范：建议每个引擎使用独占的 diskTmpDir（REQ-11）。
- 能证明安全边界的测试与删除审计。

### 2.2 不在本次范围

- 修改 Engine 调度、强制退出/重启 Engine、远程协调服务。
- 网络/FUSE 共享文件系统上的自动回收；这类文件系统只保证不误删，残留由运维处理。
- 同表跨 JVM / 跨 ClassLoader 的全局写入 fencing。现有 static ACTIVE_PHYSICAL_TABLE_OWNERS 的覆盖边界必须继续显式标注。
- 改变 at-least-once、commitUser、identifier、pending commit、offset callback、SYNC maintenance 等已有契约。
- 自动删除旧平铺布局目录、远端 Paimon orphan data files、Hadoop/S3A 自有缓存、任意外部临时目录。
- 引入 reaper、按 TTL 释放活跃 owner、JVM shutdown hook 删除，或 FAILED_RETAINED 后在本 JVM 自动恢复表写入资格。
- 把主键 MergeTree 语义推广到普通 append 或 Data Evolution append。

原始错误位于主键表 MergeTree compaction spill。普通 append 不按主键合并；Data Evolution 使用 row-ID/列文件版本重建值。共享资源所有权规则需要适配已支持模式，但不据此宣称三类表的数据组织相同，或新增 Data Evolution 写入支持。

## 3. 参考实现与问题证据

### 3.1 固定基线

| 对象 | 基线 |
| --- | --- |
| Paimon | release-1.3.2，c05f7d1f1b1e5d37e64edab0f2978124d90b64f7 |
| 本地 TapData Connectors | develop e3f6829863dab17a40904f98639e55224f625f53；已有用户未提交改动不属于本次 Spec |
| 本地 PDK 类加载源码 | tapdata-common-lib，b09e683c27808598c01d85ab53cb29d500956d8f |
| 本地 TapData Engine 源码 | tapdata，44769ce000 |
| Flink Runtime | 1.20.1 本地 sources JAR |
| 当前 Connector 依赖 | Paimon 1.3.2、Hadoop 3.3.6、RocksDB JNI 7.3.1 |
| 生产 JDK | 现场栈 `Thread.java:840` 对应 JDK 17 |
| 源码兼容 | Java 8 语法/API 基线；构建/测试 JVM 另见 §11 |

### 3.2 Flink 的做法与本方案的取舍

| Flink / Paimon Flink 源码事实 | 本方案的取舍 |
| --- | --- |
| 每个 TaskManager 进程一个 IOManagerAsync，目录为 `flink-io-<uuid>`（TaskManagerServices:342） | 采用“进程命名空间”：同一 JVM 所有 ClassLoader 的 lease 都放在 `<jvmId>/` 下 |
| Paimon Flink sink 在 Flink spill 目录内创建自己的 Paimon IOManager，`write.close` 后再关闭它（StoreSinkWriteImpl:75/177） | 采用“嵌套归属”：Paimon 的 `paimon-io-*` 建在 lease 的 `io/` 下，关闭顺序沿用 V1.2 屏障 |
| FileChannelManagerImpl 先注册 shutdown hook 再建目录（Flink:76-81）；Paimon 1.3.2 保留了这段注释但删掉了 hook（Paimon:62） | 不加 hook：hook 可能在 compaction 仍运行时删除目录，违反关闭证明；残留由下一个 JVM 按锁回收 |
| TaskManager 工作目录为 `tm_<resourceId>`，启动时清空自己的 `tmp`（ClusterEntrypointUtils:171、WorkingDirectory:43） | 采用“同身份/后继回收”：进程死亡后由其他 JVM 或重启后的新 JVM 回收；用 OS 锁代替稳定 resourceId，因为 Connector 拿不到可靠的引擎身份 |
| flink-runtime 中没有扫描其他进程目录的清理器（全 sources JAR 检索，只有 FileChannelManagerImpl 使用该命名） | 本进程目录绝不被本进程探测；跨进程回收只针对 OS 锁已释放的 lease |
| YARN/K8s 部署下临时目录为容器私有，由资源管理器回收（Flink 部署行为，本地未核对 flink-yarn 源码） | REQ-11 建议每个引擎独占 diskTmpDir |
| Paimon 主键 writer：cancelCompaction → sync → compactManager.close；Core 自有池 shutdownNow 不 await | 不把 cancel / Future.isDone 当线程退出证明，保留 Connector 实际 termination 屏障 |
| Flink Task cancellation watchdog 超时上报 fatal TaskManager error | 属于 Runtime 的进程级处理，Connector 不擅自杀 Engine |

Flink 的 task cancellation timeout 针对任务取消，不能无条件覆盖普通失败或正常结束的清理。即使任务线程退出，遗留库线程也不一定被该 watchdog 发现，因此不能写成“所有 Paimon compaction 都有退出保证”。[P1–P6][F1–F5]

### 3.3 缺陷机制（develop 源码 + 实测）

**锁丢失。** JDK 在同一 JVM 内共享 FileLockTable（按文件 dev+inode）。本 JVM 已持锁时，再打开同一文件并 tryLock 会抛 OverlappingFileLockException；此后关闭这个探测 channel，POSIX fcntl 语义会释放本进程在该文件上的全部锁。owner 的 `FileLock.isValid()` 仍返回 true，但其他进程已经可以加锁。[J1]

2026-11-12 本地最小探针实测（macOS，Temurin 11.0.32.1 与 Oracle JDK 17.0.20 结果相同）：

~~~text
[before probe] external tryLock acquired=false
probe: OverlappingFileLockException -> close
owner FileLock.isValid()=true
[after probe]  external tryLock acquired=true
~~~

Linux 上未实测，但使用同一 POSIX fcntl 语义；AC-21 要求在 Linux + JDK 17 上复验。

**两条触发路径（develop）：**

| 触发 | 位置 | 条件 |
| --- | --- | --- |
| 跨 ClassLoader | 每个 ClassLoader 各有一份 static LIVE_DIRS；新 ClassLoader 扫描到旧 ClassLoader 的活跃目录时，在 PaimonSpillDirCleaner:413 打开其锁文件，在 :584 关闭 | 不重启引擎就重新加载 paimon-plus jar（PdkUtil 下载新 jar 后 refreshJars 会新建 ClassLoader） |
| 同 ClassLoader 注册竞态 | 注册时先在 :269 拿锁，后在 :284 登记 LIVE_DIRS；目录在 :350 已物化。并发扫描在 :402 读到“未登记”，就会打开同一锁文件 | 任务启动与扫描并发；每次 onStart（含 connectionTest，PaimonConnector:128）都会扫描（PaimonService:307） |

**删除必须由另一个进程完成。** 锁丢失后，同 JVM 再次探测仍会因 FileLockTable 中的 owner 记录抛 OverlappingFileLockException 而跳过。Paimon 1.3.2 内部只删除自身子路径（FileChannelManagerImpl.close、GlobalIndexAssigner.close），目录一经删除不会重建。develop 的 owner 关闭路径受 compaction termination 屏障保护。因此，在 develop 上，活跃目录被删的必要条件是：另一个能看到同一根目录的 JVM 拿到锁，且目录最近 10 分钟内没有 spill 活动。

**2026-11 现场故障。** 栈行号与 develop 上 7c40958f（2026-09-08）至 2841ae39（2026-10-27）之间的 PaimonService 修订一致，已包含 7e50022d/7c40958f 的停止屏障，错误发生在正常异步提交中，可排除 owner 关闭竞态。2026-09 现场包（496a4d7a，07-30 构建）的 OwnerLock.tryAcquire 有相同的“冲突后关闭”代码，两次故障可用同一机制解释。

**本 Spec 对该故障的结论取决于部署事实（OQ-04）：**

| 现场情况 | 结论 |
| --- | --- |
| 同机多个引擎 JVM 共享本地盘 diskTmpDir | 本 Spec 切断删除链路，可根治；需 AC-03/AC-04 在 Linux + JDK 17 通过 |
| diskTmpDir 为跨主机网络/FUSE 共享卷 | 锁不保证跨主机生效；本 Spec 通过 REQ-07 关闭自动回收来保证不误删，残留由运维处理 |
| 只有一个 JVM 能看到 diskTmpDir | 上述机制不能解释该故障，删除者在 Connector 之外，需另行取证 |

另一个已知缺口：现有删除日志（PaimonService:352）写入**发起扫描的任务或连接测试**的 TapLog，不在出错任务日志里，连接测试日志可能不保留。这很可能是 2026-09 没能确认删除者的原因，REQ-10 对此作出要求。

### 3.4 现场取证（不阻塞 Plan；阻塞对外宣称“已根治”）

~~~sh
# 1. 删除者：检索同机/共享卷上所有引擎的全部任务日志与引擎日志
grep -rn "<paimon-io UUID>" <所有引擎日志目录>
#   "Removed stale Paimon spill dir"               → 清理器删除（位于发起扫描的任务日志）
#   "FileChannelManager removed spill file directory" → IOManager.close 删除

# 2. 文件系统类型与可访问该目录的 JVM 数量
findmnt -T /tapdata_cache; df -T /tapdata_cache
ps -ef | grep -i [t]apdata    # 容器部署时同时检查各容器的 volume 挂载

# 3. 生产锁状态（在正常运行的引擎上执行，非侵入）
ENGINE_PID=<引擎 pid>
for f in /tapdata_cache/.paimon-io-*.tapdata-owner.lock; do
  ino=$(stat -c %i "$f")
  holder=$(awk -v ino="$ino" '{n=split($6,a,":"); if (a[n]==ino) print $5}' /proc/locks)
  fdopen=$(ls -l /proc/$ENGINE_PID/fd 2>/dev/null | grep -cF "$f")
  echo "$f fd_open=$fdopen lock_pid=${holder:-NONE}"
done
# fd_open=1 且 lock_pid=NONE：锁已在 OS 层丢失，直接证明 §3.3 机制存在于生产
~~~

同时确认故障前引擎是否在未重启的情况下重新加载过 paimon-plus jar。

## 4. 术语与信任边界

| 术语 | 含义 |
| --- | --- |
| jvmId | 由只读 RuntimeMXBean 的 name 与 startTime 派生的 JVM 实例身份；同 JVM 所有 ClassLoader 相同，重启后不同 |
| 进程命名空间 | `<subroot>/<jvmId>/`，只存放该 JVM 实例创建的 lease |
| lease | 一个写上下文或预检在一个根目录下的私有目录，含 owner.lock 与 io/ |
| leaseId | `<tableTag>-<UUID>`，一次性，不复用、不重新激活 |
| tableTag | 由表标识派生的短哈希，只用于归因，不包含原始表名 |
| owner lock | lease 创建者持有的 owner.lock 文件锁；是唯一的存活证明 |
| GC | 回收其他 JVM 命名空间中 owner 已死亡的 lease 的过程 |
| 关闭证明 | 既有协议确认 API/reader 使用者退出、prepare 返回、后台任务实际终止，且相关关闭成功 |
| FAILED_RETAINED | 调用者已经失败返回，但资源仍可能在使用，必须保留的既有终态 |

所有参与进程必须遵守同一新协议，目录由受信任运行用户管理。恶意程序、运维 rm、系统临时目录清理、失效挂载或不遵守锁协议的工具不在互斥保证内；出现异常仍须失败并报告，不能补 mkdir 后继续旧 writer。

jvmId 不同只允许进入锁检查，**不证明对方进程死亡**；jvmId 碰撞只会导致保守跳过（残留），不能导致删除。

## 5. 核心不变量

| 编号 | 必须始终成立 |
| --- | --- |
| INV-01 | 任何代码都不打开本 JVM jvmId 命名空间下的 owner.lock；排除按目录名在任何文件打开之前完成，不依赖 LIVE_DIRS、ClassLoader 数量或注册顺序 |
| INV-02 | 活跃 lease 的 owner lock 由创建它的 lease 对象独占持有；锁对象的可达性不短于 lease 内任何资源使用者 |
| INV-03 | 唯一的存活证明是 OS 文件锁；mtime、目录年龄、PID、心跳、取消结果、PDK unregister、ClassLoader.close、写入 API 返回都不参与删除判定 |
| INV-04 | GC 不创建、不重建 owner.lock；leaseId 一次性；持锁后复核锁文件仍是同一文件（fileKey 相同） |
| INV-05 | 并发回收同一死亡 lease 最多造成重复删除或失败记录，不能影响任何活跃 lease；部分删除不计为成功 |
| INV-06 | 实际使用者退出证明先于 writer.close；writer/committer/IOManager 关闭成功先于 owner 释放（沿用 V1.2） |
| INV-07 | FAILED_RETAINED 不释放 owner、不进入新清理动作，迟到结束不改成 SUCCESS |
| INV-08 | 文件系统类型不在白名单、路径或身份无法确定时，关闭 GC 并保留，不回退到时间判断 |
| INV-09 | 新代码不打开旧平铺布局的任何锁文件，不删除旧目录 |
| INV-10 | 本地 spill lease 不被表述为全局表写入锁或 exactly-once 保证 |

## 6. 功能要求

### 6.1 REQ-01：目录布局

每个解析后的 diskTmpDir 根目录下使用独立版本子根：

~~~text
<root>/tapdata-paimon-v2/
  <jvmId>/                          # 进程命名空间
    <tableTag>-<UUID>/              # lease：一个写上下文或预检
      owner.lock                    # owner lock
      io/                           # 传给 IOManager.create 的根目录
        paimon-io-<uuid>/...        # Paimon 自有目录；受限的 rocksdb-* 也在其中
~~~

- 具体编码（jvmId 长度、tableTag 长度、子根名）在 Plan 中固定；协议版本只由子根名表达。
- 不要求 lease.json。路径已经包含 jvmId 与 leaseId；如果 Plan 为诊断加入元数据文件，它不得参与任何删除判定。
- 多根目录时，同一 leaseId 在每个根下各建一个 lease，每个 lease 各持一把锁；任一根失败，不发布可写 Context，已取得的部分按既有 scope 规则处理。
- 不使用配置中的 task/table 名直接拼接文件路径；tableTag 与表标识的对应关系只写入审计日志，不输出连接凭据。
- Paimon IOManager 与 GlobalIndexAssigner 的 RocksDB 路径都必须位于 lease 的 io/ 内；沿用 withConfinedTempDirs。
- HASH_DYNAMIC 预检（PaimonDynamicBucketPreflight）创建的 IOManager 同样使用独立 lease。

### 6.2 REQ-02：jvmId

- 来源：`ManagementFactory.getRuntimeMXBean()` 的 `getName()` 与 `getStartTime()`，经 SHA-256 等不可逆哈希截取为目录名。只读调用，不调用 getPlatformMBeanServer，不注册 MBean。
- 同 JVM 所有插件 ClassLoader、重复加载和并发初始化必须得到相同值；JVM 重启后必须不同。
- 不从插件自己的随机 UUID 或 static 变量推导，不使用可随意改写的系统属性。
- 身份相同一律跳过；身份不同仍须通过锁检查。
- 获取失败时：写上下文/预检初始化失败并报告，不创建 lease，不回退旧平铺布局，不进行 GC。

### 6.3 REQ-03：lease 创建

顺序必须满足：

1. 解析并校验所有本地根路径；计算 jvmId 与新 leaseId。
2. 预登记构造占位并保存后续资源强引用，受现有 StopController 动作门禁保护。
3. 创建 `<jvmId>/` 后，排他创建 lease 目录；若父目录在此期间被外部 GC 删除（只可能在其为空时发生），换新 leaseId 有界重试。
4. 以 CREATE_NEW 创建 owner.lock 并 tryLock；任何失败都使 lease 创建失败，不得改用已有文件。
5. 在 io/ 下创建 IOManager、物化目录、构造受限 tempDirs 视图。
6. 全部根就绪后才允许 writer 使用或发布 Context。

构造失败沿用已有 scope 资源记账与关闭证明，不因“尚未完整返回 Context”就假定没有异步使用者。

### 6.4 REQ-04：同 JVM 隔离

- GC 列出 `tapdata-paimon-v2/` 下的子目录后，先用目录名与本 jvmId 比较；相同即跳过整个命名空间，不列出其内容、不打开其中任何文件。
- 本 JVM 的 lease 无论是否在 LIVE_DIRS 中、存在多久、STOP 是否失败、ClassLoader 是否被替换，均不被本 JVM 的任何 GC 触碰。
- owner close 与 GC 是两个独立入口，不得复用。
- LIVE_DIRS / OWNER_LOCKS 等 static 集合不再作为删除判定依据；如保留，只作诊断。
- 正常回收不得调用 IOManager.getSpillingDirectories 等带懒创建副作用的方法；使用已缓存路径。

### 6.5 REQ-05：并发回收容忍与复核

同 JVM 两个 ClassLoader、或两个 JVM 可能同时回收同一个死亡 lease。由于 owner 已死，这只影响回收记账，不影响活跃数据，因此**不建立跨 ClassLoader 的 GC 临界区**。要求：

- 删除时容忍 NoSuchFile / 目录已不存在，部分删除计为失败而不是成功。
- 持锁后复核：锁文件仍存在，且其 fileKey 与打开前读取的一致；否则释放并跳过。
- OverlappingFileLockException 视为“另一个回收者正在处理”，关闭探测 channel 后跳过。该 lease 已无 owner，关闭不会伤害活跃数据。
- 用户回调、日志后端调用放在锁与 channel 关闭之后。
- STOP 监督、正常写入与 owner close 不等待 GC。

### 6.6 REQ-06：跨 JVM 回收

只有同时满足以下条件才回收一个 lease：

1. 所在命名空间的 jvmId 不等于本 jvmId，且文件系统通过 REQ-07 门禁。
2. lease 与 owner.lock 都是普通目录/文件，不是符号链接。
3. 以 READ+WRITE、**不带 CREATE**、NOFOLLOW_LINKS 打开 owner.lock，tryLock 成功；返回 null 或任何异常都跳过。
4. 通过 REQ-05 的持锁复核。

回收顺序：删除 io/ 整棵树（不跟随符号链接）→ 持锁状态下 unlink owner.lock → 释放锁并关闭 channel → 删除 lease 目录。io/ 删除失败时保留 owner.lock，释放锁，记录失败，留待后续重试。

附加规则：

- 外部命名空间中没有 owner.lock 的 lease 目录，只允许以 rmdir 删除（非空即失败）；正在创建的 owner 会按 REQ-03 第 3 步重试。
- 外部命名空间目录为空时，允许 rmdir。
- 不读取 mtime，不设 grace；不以目录年龄、PID 不存在或心跳失联作为任何判定输入。
- GC 触发仍沿用初始化清理入口；不新增常驻线程。

### 6.7 REQ-07：文件系统门禁

- 回收前用 `Files.getFileStore(subroot).type()` 判断文件系统类型。[J4]
- 只有白名单内的本地文件系统允许回收。初始候选：ext2/ext3/ext4、xfs、btrfs、zfs、tmpfs、overlay；apfs 仅用于开发测试。最终清单在 Plan 中按生产 OS 固定。
- nfs*、cifs/smb*、fuse.*、ceph、glusterfs、9p 及任何未知类型：GC 返回 0、不打开任何锁文件、记录限流 WARN；spill 写入不受影响。
- 类型获取失败视为未知。

### 6.8 REQ-08：owner 关闭与超时契约保持

沿用现有 V1.2，保护顺序为：

~~~mermaid
flowchart TD
    A["关闭入口；完成既有业务屏障"] --> B["最终 prepare 或受控取消"]
    B --> C{"prepare 已返回且实际使用者已退出？"}
    C -- "预算内已证明" --> D["关闭 writer、committer、IOManager"]
    D --> E["删除 io/ 残留 → unlink owner.lock → 释放锁 → 删除 lease 目录"]
    C -- "无法证明或超时" --> F["FAILED_RETAINED：保留资源与 owner lock"]
~~~

具体要求：

- 第一次使用前注入的 PaimonCompactionExecutor 继续负责实际执行追踪。
- 保留 GuardedFuture 的取消来源桥接，避免 CancellationException 被内核吞掉后进入 prepare 内联 writer.close。
- writer.close 会关闭 rewriter，必须在实际 compaction 退出后执行；不接受仅把 IOManager.close 延后。
- owner 先 unlink owner.lock 再释放锁：已打开旧 inode 的外部 GC 即使随后拿到锁，也会在复核时退出。
- io/ 删除、unlink 或锁释放任一失败：按既有 release 失败处理，保留锁直至进程退出，由下一个 JVM 回收。
- 超时之后不开始新的提交、callback、writer/IO close 或 owner 释放；已获准的在途动作可能迟到发生，仍按现有未知结果规则处理。
- 不注册 JVM shutdown hook 删除 lease。
- 残留被回收只表明本地资源失去 owner，不证明可以接管远端表的写入资格。
- ClassLoader 关闭和卸载测试必须验证：仍有资源使用者时 lease 锁不会因引用丢失而在 OS 层释放。

### 6.9 REQ-09：滚动升级与回退

- 新版本只在新子根创建和回收 lease；不打开旧平铺布局的 `.paimon-io-*.tapdata-owner.lock`，不删除旧 `paimon-io-*` 目录。
- 旧平铺目录仅输出限流的清单 WARN；所有引擎升级并重启后，由运维一次性清理。
- 旧版本清理器只列出根目录下的 `paimon-io-*`，不会进入新子根。新旧版本并存期间，旧版本自身的旧缺陷仍可能作用于旧目录，不能宣称全机已消除旧风险。
- 发布说明建议：升级 paimon-plus 后重启引擎，避免同一 JVM 同时存在新旧两个版本的 ClassLoader。
- 回退到旧版本不会自动迁移或删除新目录；回退后的旧任务使用自己的旧布局。
- 不修改 warehouse 表 schema/options，不移动远端数据，不改已持久化业务状态格式。
- 子根版本未知时拒绝读取与回收。

### 6.10 REQ-10：审计与可观测性

- lease 创建、保留、回收开始/完成/失败、跳过（限流）事件必须写入引擎级 SLF4J 日志；可同时写入任务 TapLog，但不能只写 TapLog。
- 字段：协议版本、pid、hostname、jvmId 摘要、leaseId、tableTag（创建时附表标识映射）、ClassLoader 标识、根目录、文件系统类型、动作来源、锁检查结果、关闭阶段、真实 executor termination 状态、已保留时长、删除失败路径与异常。
- 日志仅用于诊断，不参与关闭证明；日志后端阻塞不能拖住 StopController 监督者；不输出连接串或凭据。

### 6.11 REQ-11：部署规范（与 Flink 对齐）

- 推荐每个引擎进程使用独占的 diskTmpDir：K8s 用 per-pod emptyDir 或独立 subPath，Docker 用独立 volume。这样跨进程删除路径在部署层面就不存在，与 Flink 在 YARN/K8s 上的做法一致。
- 共享根目录部署由本 Spec 的协议兜底；网络/FUSE 共享卷上不自动回收。
- 以上写入发布说明与连接器配置说明；不在代码中强制。

## 7. 与现有代码的接缝

以下是职责边界，不是实施任务清单。

| 位置（相对目标仓库根） | 本次要求 |
| --- | --- |
| connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/util/PaimonSpillDirCleaner.java | 拆出或替换为命名空间/lease/GC 实现；删除旧平铺扫描与 mtime 判定；static 集合不再作为删除依据 |
| .../write/PaimonTableWriteContextFactory.java | 创建前登记、lease 创建、受限 tempDirs、多根与失败回滚 |
| .../write/PaimonTableWriteContext.java | closeForStop 末尾接入 lease 退役顺序，沿用屏障与不可逆保留 |
| .../service/PaimonDynamicBucketPreflight.java | 预检 IOManager 使用独立 lease，关闭顺序与 Context 一致 |
| .../service/PaimonService.java | 根目录解析、初始化 GC 入口、旧布局清单、引擎级审计 |
| .../service/PaimonStopController.java、PaimonStopResources.java | 保持已确认 STOP 行为；验证保留资源在 ClassLoader 生命周期中的可达性 |
| .../write/PaimonCompactionExecutor.java / PaimonCompactionLifecycle.java | 原有实际退出及取消来源机制不能被本次简化掉 |
| .../src/test/java/io/tapdata/connector/paimon/util | 多加载器、多 JVM、文件系统门禁与锁协议验收 |
| .../src/test/java/io/tapdata/connector/paimon/write、service | Factory、STOP、有限读、DDL、预检及真实 Paimon spill 回归 |
| .../src/doc/specs | 本 Spec 与现有停止主 Spec 的正式落点；连接器配置说明补充部署规范 |

若设计需要改 Engine、改 PDK 父加载器、注入全局对象或新增服务，属于方向变更，必须先更新并重新确认 Spec。

## 8. Testing Strategy

采用现有 JUnit Jupiter 5.8.1、Mockito 和真实 Paimon 1.3.2 测试接缝。目录使用 @TempDir；进程用 ProcessBuilder 启动真实 Java 子进程；ClassLoader 测试必须实际隔离定义 Connector 类。

不设没有依据的行覆盖率目标；要求 INV-01–10 每项有可追踪测试，所有生产资源创建/释放入口有路径覆盖。并发顺序使用 latch/受控屏障，不依赖固定 sleep。忽略中断的 fixture 必须在 finally 释放、等待真正退出并清理。文件系统类型通过可注入的类型提供器模拟，真实锁行为不得用 mock 替代。

| 验收 ID | 场景与断言 |
| --- | --- |
| AC-01 | 两个隔离 ClassLoader 得到相同 jvmId；子进程得到不同 jvmId；没有共享 Connector static |
| AC-02 | loader A 有活跃 lease，loader B 扫描：对 A 命名空间内任何文件的打开次数为 0 |
| AC-03 | 重放反例：A 持锁、B 同 JVM 扫描、外部 JVM C 扫描；C 删除数为 0，A 用真实 IOManager 创建 channel 成功，外部进程 tryLock 仍失败 |
| AC-04 | 同 ClassLoader 下 lease 创建与扫描用屏障交错（覆盖旧实现 :269→:284 窗口的等价点）：扫描器对 owner.lock 的打开次数为 0 |
| AC-05 | 两个 loader 同时回收同一外部死亡 lease：无异常逃逸，至多一次成功，其余跳过或记失败；同机另一活跃 lease 不受影响 |
| AC-06 | 两个独立 GC JVM 同时回收同一死亡 lease：至多一个完成删除；全程不创建任何 owner.lock |
| AC-07 | 活跃 writer 空闲任意时长（注入时钟模拟数小时）、墙钟回拨/跳跃、jvmId 碰撞：均不删除 |
| AC-08 | 缺 owner.lock、符号链接 lease/命名空间、路径别名、未知子根版本：均保守处理；jvmId 获取失败时 Context 初始化失败且不产生 lease |
| AC-09 | owner 子进程真实退出（含 SIGKILL）后，另一 JVM 回收其 lease；模拟同一引擎重启（新 JVM）回收上一实例 lease；不依赖父 JVM 的 static 集合 |
| AC-10 | 本 JVM 的 FAILED_RETAINED lease 被重复扫描仍保留，没有后台解锁或恢复 |
| AC-11 | 取消后 worker 忽略中断：Future 已取消但 executor 未终止时，不关闭 rewriter、不删目录、不释放锁 |
| AC-12 | 既有 prepare 内联 close 防护、受控取消来源、pending 未知结果、offset callback 顺序全部回归通过 |
| AC-13 | Factory 多根部分失败、DDL/rebuild、有限读 close 失败与超时迟到构造：未证明关闭的 lease 保持持锁 |
| AC-14 | 实际 RocksDB 临时路径位于 lease 的 io/ 内；所有 bucket strategy 与 HASH_DYNAMIC 预检执行相同 lease 约束 |
| AC-15 | 插件 close/替换及 ClassLoader GC 压力下，活跃 lease 的锁在 OS 层持续有效（外部进程 tryLock 失败），不能只检查 FileLock.isValid |
| AC-16 | owner 关闭与外部 GC 竞态：owner 先 unlink 再释放；已打开旧 inode 的 GC 复核失败后退出，不删除任何内容 |
| AC-17 | io/ 删除失败时保留 owner.lock 供后续重试；审计日志或回调故障不导致锁提前释放或监督超时失效 |
| AC-18 | 文件系统门禁：nfs4、fuse.* 及未知类型下 GC 返回 0、不打开任何锁文件、记录 WARN；写入正常 |
| AC-19 | 新旧版本并存：新 GC 不打开旧 `.paimon-io-*.tapdata-owner.lock`、不删除旧目录；旧清理器不进入新子根；回退不误删新目录 |
| AC-20 | 引擎级审计：创建、回收、跳过（限流）事件出现在 SLF4J 日志中，含 REQ-10 字段 |
| AC-21 | AC-03/04/06/09/15/16 在 Linux + JDK 17 上用真实子进程执行；macOS 结果只作补充，未测组合不宣称支持 |
| AC-22 | 启停、多表、多根重复运行后，正常关闭无 lease 残留、无活跃 channel 泄漏；保留项与审计一致 |

代码级模拟和真实进程测试分别报告；不能用 mock tryLock 替代 OS 行为。完整 Flink 集群故障注入不是本次 Connector 改动的必需门禁，Flink 只作为固定版本源码参考。

## 9. Success Criteria

1. AC-01–22 全部通过；跳过项须说明原因，且不能覆盖核心多 ClassLoader / 多 JVM / Linux 证明。
2. 原缺陷复现转为持续保护：扫描不使活跃 owner 丢锁，真实 Paimon spill 不再因本协议误删而 ENOENT。
3. 进程退出后的新协议残留可以回收；本 JVM FAILED_RETAINED 保留符合已确认取舍；网络/FUSE 文件系统上不自动回收。
4. 不改变现有 STOP 默认值、SYNC 限制、业务提交/取消来源、pending/offset 语义；原有定向回归和模块 package 通过。
5. 不依赖 MBeanServer、Engine 改动、新服务、远程端口或常驻线程。
6. 审计能在引擎日志中定位谁创建、谁保留、谁回收了某个 lease。
7. 发布说明写明：本次修复本地目录所有权，不承诺全局表单写者 fencing；部署建议见 REQ-11；对 2026-11 现场故障的结论以 OQ-04 取证结果为准。

## 10. Code Style

遵循现有模块的 Java 8、四空格缩进、UpperCamelCase 类型名、lowerCamelCase 方法/变量名及具有实际含义的异常。不得引入 record、var、Java 9+ 集合工厂等高版本语法/API。

以下是现有 PaimonCompactionLifecycle 的真实风格片段，强调身份校验与明确动作：

~~~java
public void beginFinal(PaimonStopController.FinalAttempt attempt) {
    if (!attempt.belongsTo(this)) {
        throw new IllegalArgumentException("Foreign lifecycle");
    }
    if (executor != null) {
        executor.beginFinal(attempt);
    }
}
~~~

代码须区分请求动作与已证明结果，例如 requestCancellation 与 isTerminated。错误输出应包含 leaseId/阶段和原因，不使用含糊的“close success”掩盖部分失败；不得打印原始连接配置。

## 11. Commands 与执行前提

以下命令在目标仓库执行，所需内部 Maven 依赖及构建 JDK 必须可用。当前源码保持 Java 8；现有测试文档使用 JDK 17，模块为 JDK 9+ 提供 add-opens profile。AC-21 需要在 Linux + JDK 17 环境执行同一组命令。

~~~sh
cd /Users/conradjam/codes/javaProjects/tapdata-connectors

# 核对实际构建环境
java -version
mvn -version

# 单模块及必要依赖编译
mvn -B -pl connectors/paimon-plus-connector -am -DskipTests compile

# 目录/锁相关定向测试（新增协议测试类名在 Plan 中确定后加入）
mvn -B -pl connectors/paimon-plus-connector -am -DskipTests=false -Dtest='PaimonSpillDirCleanerTest,PaimonSpillDirCleanerProcessIntegrationTest,PaimonServiceStaleSpillCleanupTest' -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test

# 停止与真实 spill 回归
mvn -B -pl connectors/paimon-plus-connector -am -DskipTests=false -Dtest='PaimonBoundedStopTest,PaimonStopControllerTest,PaimonNativeStopContractTest,PaimonCompactionSpillLifecycleIntegrationTest,PaimonTableWriteContextFactoryTest,PaimonFiniteReadLifecycleTest,PaimonConnectorStopTest' -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test

# 最终模块构建及测试
mvn -B -pl connectors/paimon-plus-connector -am -DskipTests=false package

# 检查补丁空白及意外修改
git diff --check
git status --short
~~~

不要使用 Paimon 仓库的 fast-build / Flink profile 作为 TapData 构建参数；本轮没有核实该模块有独立 lint 命令，不编造命令替代 Maven 检查。

本轮只修订 Spec，上述编译/测试命令未执行，不记录为通过。§3.3 的锁丢失探针是独立程序，不属于模块测试。

## 12. Boundaries

### Always

- 固定 Paimon 1.3.2 源码及解析依赖，保持现有停止契约。
- 先按目录名排除本 jvmId，再做任何文件打开。
- 删除前取得 OS 锁并复核；任何歧义保留并记录。
- 为真实跨 ClassLoader、跨 JVM、Linux 锁行为建立测试，不只测 static 集合。
- 先核对用户已有改动，不覆盖无关文件，不把历史 Spec 的测试成绩算作新版本证据。
- 目录协议和关闭行为发生变化时先更新 Spec。

### Ask first

- 使用只读 RuntimeMXBean 以外的 JMX API、父加载器注入、全局共享运行时或新依赖。
- 增加 JVM shutdown hook、常驻线程或 grace/TTL 类时间判断。
- 调整文件系统白名单，或在网络/FUSE 文件系统上开启回收。
- 改 STOP 预算、FAILED_RETAINED 恢复行为、提交/offset 语义。
- 扩大到跨机器协调、同表全局 fencing、自动重启 Engine。
- 自动处理旧平铺布局目录或损坏的新布局目录。
- 改动 CI/JDK 支持范围。

### Never

- 在本 JVM 命名空间下打开任何 owner.lock，或先打开锁文件再判断是否属于本 JVM。
- 仅凭 mtime、目录年龄、PID、心跳、isDone、cancel 返回值或 ClassLoader.close 删除目录。
- 把 OverlappingFileLockException + close 当作对活跃 owner 无副作用的探测。
- GC 创建或重建 owner.lock，或复用已退役 leaseId。
- 无关闭证明就释放 owner lock。
- 在失败路径用 Thread.stop、System.exit、Runtime.halt 或杀 Engine 补足生命周期保证。
- 把 Spec 草案、原型或历史测试报告描述为生产修复已经完成。

## 13. Open Questions 与审批

| ID | 问题 | 当前处理 |
| --- | --- | --- |
| OQ-01 | jvmId 来源 | **已决**（2026-11-12）：只读 RuntimeMXBean name + startTime，见 REQ-02 |
| OQ-02 | 锁文件退休能否安全且无 inode 重建 | **已决**：GC 不带 CREATE 打开 + leaseId 一次性 + 持锁后 fileKey 复核 + owner 先 unlink 后释放，见 REQ-05/06/08，由 AC-06/16 验证 |
| OQ-03 | 宿主卸载路径是否可能在真实 worker 结束前回收 owner channel | Plan/实现门禁：AC-15 与源码审计验证；不能凭 static 注释推定安全 |
| OQ-04 | 2026-11 现场：可访问 diskTmpDir 的 JVM 数量、文件系统类型、生产锁状态、删除者日志 | 按 §3.4 取证；不阻塞 Plan，阻塞对外宣称“已根治” |
| OQ-05 | 生产 OS 与文件系统白名单最终清单 | Plan 阶段固定；未列入的类型一律关闭回收 |

**审批记录：** 2026-11-12 用户批准 V0.2（含 §1.2），进入技术计划。计划获批前不修改生产代码。OQ-03/05 是计划与实现门禁，若不能满足，需要回到 Spec 重新确认方向。

## 14. 来源

- [当前停止主 Spec V1.2](https://github.com/tapdata/tapdata-connectors/blob/e3f6829863dab17a40904f98639e55224f625f53/connectors/paimon-plus-connector/src/doc/specs/SPEC-paimon-spill-sync-graceful-stop.md)
- [当前 PaimonSpillDirCleaner](https://github.com/tapdata/tapdata-connectors/blob/e3f6829863dab17a40904f98639e55224f625f53/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/util/PaimonSpillDirCleaner.java)（:269、:284、:350、:402、:413、:584）
- [当前 PaimonService 初始化清理](https://github.com/tapdata/tapdata-connectors/blob/e3f6829863dab17a40904f98639e55224f625f53/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java#L307)（:307、:352）
- [当前 PaimonConnector.connectionTest → onStart](https://github.com/tapdata/tapdata-connectors/blob/e3f6829863dab17a40904f98639e55224f625f53/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/PaimonConnector.java#L128)
- [当前 PaimonTableWriteContext.closeForStop](https://github.com/tapdata/tapdata-connectors/blob/e3f6829863dab17a40904f98639e55224f625f53/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContext.java#L354)
- [当前 PDK DependencyURLClassLoader](https://github.com/tapdata/tapdata-common-lib/blob/b09e683c27808598c01d85ab53cb29d500956d8f/plugin-kit/tapdata-pdk-runner/src/main/java/io/tapdata/pdk/core/classloader/DependencyURLClassLoader.java#L99)、[ExternalJarManager 每次加载新建 ClassLoader](https://github.com/tapdata/tapdata-common-lib/blob/b09e683c27808598c01d85ab53cb29d500956d8f/plugin-kit/tapdata-pdk-runner/src/main/java/io/tapdata/pdk/core/classloader/ExternalJarManager.java#L231)
- 本地 Engine 源码 tapdata@44769ce000：iengine/iengine-common/src/main/java/io/tapdata/flow/engine/V2/util/PdkUtil.java:99/101（下载 jar 后 refreshJars）
- [P1：StoreSinkWriteImpl](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreSinkWriteImpl.java#L75)（:75、:177）
- [P2：CompactFutureManager](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L33)
- [P3：AbstractFileStoreWrite.close](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L303)
- [P4：AppendCompactWorkerOperator.close](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/AppendCompactWorkerOperator.java#L105)
- [P5：CDC 多表关闭顺序](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/sink/cdc/CdcRecordStoreMultiWriteOperator.java#L226)
- [P6：Paimon FileChannelManagerImpl（保留 hook 注释但无 hook）](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java#L62)
- [F1：Flink 1.20.1 Task，cancellation watchdog](https://github.com/apache/flink/blob/release-1.20.1/flink-runtime/src/main/java/org/apache/flink/runtime/taskmanager/Task.java#L1775)
- [F2：Flink FileChannelManagerImpl shutdown hook](https://github.com/apache/flink/blob/release-1.20.1/flink-runtime/src/main/java/org/apache/flink/runtime/io/disk/FileChannelManagerImpl.java#L76)
- [F3：Flink WorkingDirectory 启动时清空 tmp](https://github.com/apache/flink/blob/release-1.20.1/flink-runtime/src/main/java/org/apache/flink/runtime/entrypoint/WorkingDirectory.java#L43)
- [F4：Flink tm_<resourceId> 工作目录](https://github.com/apache/flink/blob/release-1.20.1/flink-runtime/src/main/java/org/apache/flink/runtime/entrypoint/ClusterEntrypointUtils.java#L171)
- [F5：Flink TaskManager 级 IOManagerAsync](https://github.com/apache/flink/blob/release-1.20.1/flink-runtime/src/main/java/org/apache/flink/runtime/taskexecutor/TaskManagerServices.java#L342)
- [J1：Java 8 FileLock 平台语义](https://docs.oracle.com/javase/8/docs/api/java/nio/channels/FileLock.html)
- [J3：RuntimeMXBean](https://docs.oracle.com/javase/8/docs/api/java/lang/management/RuntimeMXBean.html)
- [J4：FileStore.type（格式依实现而定，须实测）](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileStore.html#type--)

Flink 行号来自本地 flink-runtime-1.20.1-sources.jar；Paimon 行号来自本地 release-1.3.2 标签。

## 15. 修订记录

### V0.2（2026-11-12）

依据 2026-11 现场故障分析与 Flink 对照方案修订：

- **方向**：从“lease 加锁 + 同 JVM 排除 + 跨 ClassLoader GC 串行”调整为 Flink 式“进程命名空间 + 死亡 owner 回收”；明确表级隔离不是删除安全依据。
- **证据**：§3.3 补充同 ClassLoader 注册竞态、JDK 11/17 锁丢失实测、现场包版本定位、“删除必须由另一进程完成”的推导与按部署事实分类的结论；新增 §3.4 现场取证。
- **布局**：lease 下增加 io/ 与 tableTag；不再要求 lease.json。
- **身份**：OQ-01 已决为只读 RuntimeMXBean；获取失败时初始化失败。
- **删除**：V0.1 REQ-05（intern monitor）改为并发回收容忍与 fileKey 复核；取消 mtime/grace；明确 owner 先 unlink 再释放；允许 rmdir 外部空 lease/命名空间目录。
- **新增**：REQ-07 文件系统白名单门禁、REQ-10 引擎级审计、REQ-11 部署规范；HASH_DYNAMIC 预检纳入 lease。
- **验收**：AC 由 18 项调整为 22 项，新增同 ClassLoader 竞态、owner 关闭竞态、文件系统门禁、引擎级审计、Linux + JDK 17 必测。
- **待定项**：OQ-02 已决；新增 OQ-04（现场取证）、OQ-05（白名单）；§1.2 列出本稿补全、待确认的细节。

### V0.1（2026-11-12）

初稿：lease 锁协议、同 JVM 排除、跨 ClassLoader GC 串行、JVM 身份来源待定。
