# Spec：Paimon 本地 Spill 的进程级命名空间与基于 OS 记录锁的租约回收协议

> Spec ID：paimon-spill-owner-isolation\
> 版本：V0.3，2026-11-12；状态：已按本版在本地实现（本地 PR #4–#15，集成分支 `hotfix/paimon-spill-owner-isolation`）；Linux 验收（AC-24）待 OQ-06。实现记录见 §17，发布说明见 §18，修订记录见 §16。\
> 目标模块：tapdata-connectors / connectors/paimon-plus-connector。\
> 证据与评审：[REVIEW-paimon-spill-owner-isolation-2026-11-12.md](../reviews/REVIEW-paimon-spill-owner-isolation-2026-11-12.md)\
> 生命周期图与修改前后对比：[SPILL-所有权隔离-修改前后对比与生命周期.md](../reviews/SPILL-所有权隔离-修改前后对比与生命周期.md)\
> 实施计划：[plan.md](../../../tasks/spill-owner-isolation/plan.md)；任务清单：[todo.md](../../../tasks/spill-owner-isolation/todo.md)\
> 本文补充 SPEC-paimon-spill-sync-graceful-stop.md V1.2，不替代其业务提交、取消来源、超时预算与 FAILED_RETAINED 契约。

## 0. 规范用语与记号

- **必须 / 禁止 / 应当 / 可以**：分别对应 RFC 2119 的 MUST / MUST NOT / SHOULD / MAY。
- **记号**：`<root>` 为一个经校验的本地 spill 根目录；`<sub>` = `<root>/tapdata-paimon-v2`；`<ns>` = `<sub>/<jvmId>`；`<lease>` = `<ns>/<tag>-<uuid>`；`<lock>` = `<lease>/owner.lock`；`<io>` = `<lease>/io`。
- **错误码**：以 POSIX errno 表述。JDK 映射为：ENOENT → `NoSuchFileException`，EEXIST → `FileAlreadyExistsException`，ENOTEMPTY → `DirectoryNotEmptyException`。
- **锁**：指 `FileChannel.tryLock()` 在 Linux/macOS 上获得的 POSIX `fcntl` 排他记录锁（advisory record lock），其所有者是**进程**。

## 1. 决策记录

### 1.1 已批准（V0.2，2026-11-12）

| 决策 | 内容 |
| --- | --- |
| 修改边界 | 只修改 Connector；不改 Engine、PDK 父加载器、Paimon Core；不新增服务、端口或常驻线程 |
| 协调方式 | 不注册 MBean，不使用 MBeanServer；只读 `RuntimeMXBean` 仅用于派生 jvmId |
| 所有权模型 | 对齐 Flink：按进程划分命名空间；删除权只属于 owner，或属于已证明 owner 进程终止的回收者 |
| 表级隔离 | 每个写上下文/预检各持一个 lease，用于归因与资源边界，不作为删除安全依据 |
| 部署范围 | 同机本地文件系统；多 JVM；同 JVM 多个隔离插件 ClassLoader；网络/FUSE 文件系统上关闭回收 |
| 回收取舍 | 本 JVM 的失败残留保留到进程退出，由其他进程或重启后的新 JVM 回收 |
| 停止语义 | 沿用 V1.2：stopTimeoutSeconds=180、finalCompactionTimeoutSeconds=120、compactionCancelGraceSeconds=30；不自动终止 Engine |
| V0.2 细节 | 身份获取失败即初始化失败；不读 mtime、不设 grace；文件系统白名单；不注册 shutdown hook |

### 1.2 V0.3 修订决策（源自评审，处置明细见证据文档 §7）

| ID | 决策 |
| --- | --- |
| D1 | jvmId = H(pid, JVM 启动时间)，**不含主机名**；pid 通过反射调用 `RuntimeMXBean.getPid()` 取得（JDK ≥ 10），Java 8 运行时退回 `getName()` 中 `@` 之前的部分 |
| D2 | owner 侧 ENOENT 语义：创建阶段换新 leaseId 有界重试；退役阶段最后的 rmdir 遇 ENOENT 视为完成 |
| D3 | 多根先规范化为真实路径并去重，保持原顺序 |
| D4 | jvmId 派生函数与目录命名属于协议的一部分，任何变更必须升级子根版本 |
| D5 | 释放非空且未登记的路径时，判为“释放不完整”，不再静默成功 |
| D6 | IOManager 已构造后若某根未物化，按 V1.2 语义保留资源 |
| D7 | 审计中的主机名只读环境变量 `HOSTNAME`，不做名称解析 |
| D8 | 每次回收调用设处理上限，剩余留给下一次调用 |
| D9 | 每个实施步骤以本地 PR 交付，并可复查（§9） |

## 2. 问题陈述

### 2.1 失效模式

一个仍被 IOManager 使用的 `paimon-io-<UUID>` 目录被删除。Paimon 1.3.2 的 `FileChannelManagerImpl` 只在构造时 `mkdirs`，之后不会重建目录。下一次 `MergeSorter.spill` → `IOManagerImpl.createBufferFileWriter` → `AbstractFileIOChannel:62` 以 ENOENT 失败，异常经 compaction Future 传到 `prepareCommit`，导致任务失败。现场 27 个 Paimon 帧已与 release-1.3.2 逐行核对（证据 §2）。

### 2.2 根因

缺陷由以下四个条件共同构成（证据 §3，F1–F8）：

1. **JVM 级锁表与进程级 OS 锁**：同一 JVM 内对已持锁文件的再次 `tryLock` 被 JDK 的 `FileLockTable` 以 `OverlappingFileLockException` 拒绝；探测方随后 `close(2)` 自己的 fd，按 POSIX 语义释放了**本进程**在该 inode 上的全部记录锁。owner 的 `FileLock.isValid()` 仍为 true，无法察觉锁已丢失。
2. **以 ClassLoader 为作用域的存活登记**：`LIVE_DIRS`/`OWNER_LOCKS` 是 static 字段，其作用域是定义它的 ClassLoader。PDK 重新加载 jar 后，新 ClassLoader 看不到旧 ClassLoader 的活跃目录。在同一 ClassLoader 内，“先取锁、后登记”的注册顺序也留有窗口。
3. **以时间推断存活的跨进程回收**：另一进程取得锁后，以“目录最近修改早于 10 分钟”判定目录已失效。但“空闲”不等于“owner 已死”。
4. **目录不可恢复**：Paimon 不重建 spill 目录，失败是永久性的。

### 2.3 触发条件与结论适用性

故障发生需要三者同时成立：（同进程探测）∧（同一文件系统上存在另一个执行旧清理器的进程）∧（目录空闲 ≥ grace）。本协议消除第一个条件、取消第三个条件，并通过 §4.6 的准入控制约束第二个条件在不可靠文件系统上的影响。

对 2026-11 现场故障是否“根治”，取决于部署事实（OQ-04，证据 §8）。

### 2.4 范围

- **包含**：
  - Connector 管理的 Paimon IOManager 目录，及经受限 `tempDirs` 视图放入其中的 RocksDB 目录；
  - 写上下文 Factory、HASH_DYNAMIC 预检、STOP、Factory 回滚、DDL/rebuild、有限读等已有资源接缝；
  - 全局与表级 diskTmpDir；
  - 协议实现、审计、文档和测试。
- **不包含**：
  - Engine 调度与进程管理；
  - 网络/FUSE 文件系统上的自动回收；
  - 同表跨进程写入 fencing；
  - 现有提交、取消、pending、offset 语义的变更；
  - 旧平铺布局目录的自动删除；
  - reaper、TTL 或 shutdown hook 删除；
  - append / Data Evolution 写入语义的推广。

## 3. 系统模型、故障模型与平台前提

### 3.1 系统模型

- **主机**：一台主机运行一个内核，上面有若干 JVM 进程 P₁…Pₙ。
- **ClassLoader**：每个进程内可有多个插件 ClassLoader，各自持有一份 Connector static 状态。
- **共享文件系统**：所有进程共享一组本地文件系统，记录锁与 inode 语义由内核统一提供。
- **租约（lease）**：一个写上下文或预检在一个 `<root>` 下的私有目录，其生命周期见 §4.3。

### 3.2 故障模型与信任边界

- **故障类型**：进程可能 crash-stop（OOM、SIGKILL、`System.exit`）；不考虑 Byzantine 行为。
- **参与者**：所有参与者要么实现本协议，要么是只扫描根目录下平铺 `paimon-io-*` 的旧版本。
- **信任**：目录由受信任的运行用户管理。恶意程序、运维 `rm`、系统临时目录清理、失效挂载、不遵守锁协议的工具，不在互斥保证范围内。
- **身份与死亡**：jvmId 不同只说明“可能是其他进程”，不证明该进程已死；进程是否终止只由内核在进程退出时释放锁来证明。

### 3.3 平台前提（每项由已合入的特征化测试固定）

| ID | 前提 | 测试 |
| --- | --- | --- |
| A1 | 记录锁归属于进程；其他进程关闭自己的 fd 不释放 owner 的锁 | C2 |
| A2 | 同进程对已持锁文件探测后关闭，会释放本进程的锁（本协议必须使其不可能发生） | C1 |
| A3 | 父目录被删除后，`O_CREAT\|O_EXCL` 与 `mkdir` 返回 ENOENT | C4 |
| A4 | rmdir 对非空目录返回 ENOTEMPTY，对缺失目录返回 ENOENT | C5 |
| A5 | 不带 `O_CREAT` 的 open 不会产生 inode | C6 |
| A6 | 先 unlink 再释放时，迟到的打开者只能锁住已 unlink 的 inode，按路径复核必然失败；`fileKey` 可用 | C7 |
| A7 | `FileStore.type()` 可观测 | C8 |
| A8 | pid 与 JVM 启动时间在进程生命周期内不变；`RuntimeMXBean.getName()` 会变 | C3 |
| A9 | Paimon 1.3.2 IOManager 对每个根条目（包括重复项）各建一个目录，且不会重建已删除的目录 | C9、C10 |
| A10 | 网络/FUSE 文件系统上跨主机的记录锁不可依赖（不可在本地验证） | 由 §4.6 准入控制排除 |

任何一项测试失效，都表示协议前提改变，必须先修订本 Spec。

## 4. 协议设计

### 4.1 命名空间划分

~~~text
<root>/tapdata-paimon-v2/          # 版本子根；协议版本只由此名表达
  <jvmId>/                         # 进程命名空间，正则 j[0-9a-f]{32}
    <tag>-<uuid>/                  # lease；tag = 表标识 SHA-256 前 12 位十六进制
      owner.lock                   # 存活证明，由 owner 进程独占持锁
      io/                          # 传给 IOManager.create 的根
        paimon-io-<UUID>/...       # Paimon 自有目录；受限的 rocksdb-* 也在其中
~~~

- 不使用元数据文件，路径本身编码了 jvmId 与 leaseId。
- 不以配置中的表名拼接路径；tag 与表标识的映射只写入审计日志。
- 一个写上下文或预检在每个根下各建一个 lease 目录（名称各自生成 UUID，重试时更换）、各持一把锁；对应关系由 owner 索引与审计记录。

### 4.2 JVM 实例标识（jvmId）

**定义**：jvmId = `"j"` + hex(SHA-256(`"tapdata-paimon-v2|" + pid + "|" + startTimeMillis`))[0:32]。

- **pid**：JDK ≥ 10 时，通过反射调用 `RuntimeMXBean#getPid()`；其实现为 `ProcessHandle.current().pid()`，不做名称解析。Java 8 运行时取 `getName()` 中第一个 `@` 之前的十进制串；若格式不符则视为获取失败。
- **startTimeMillis**：取 `RuntimeMXBean#getStartTime()`。
- **禁止**使用主机名、系统属性、环境变量或插件内随机数参与派生。

**性质**：

- **稳定性**：输入在进程生命周期内不变（A8），且与 ClassLoader 无关，因此同一 JVM 的任意 ClassLoader、在任意时刻计算都得到同一值。
- **区分性**：同一内核上，存活进程的 pid 唯一；pid 被复用时，启动时间不同。不同 PID namespace（容器）共享同一目录时，若 pid 与启动毫秒都相同，会发生碰撞。
- **碰撞后果**：碰撞的两个进程互相视为“本进程”而跳过对方，只降低回收的活性，不破坏安全性。

**协议版本规则**：派生函数、前缀常量、目录命名规则都属于协议的一部分。任何改动都必须同时把子根升级为新版本（如 `tapdata-paimon-v3`），并由固定期望值测试守护。否则，同一 JVM 中新旧两个版本的 ClassLoader 会算出不同的 jvmId，彼此探测对方的锁文件，缺陷重现。

**失败处理**：获取失败时，写上下文/预检初始化失败，不创建 lease，不回退旧布局，不执行回收。

### 4.3 租约状态机

| 状态 | 文件系统表示 | 说明 |
| --- | --- | --- |
| ABSENT | `<lease>` 不存在 | 初始或终态 |
| PROVISIONING | `<lease>` 存在，`<lock>` 不存在 | owner 刚 mkdir；或 owner 在 unlink 锁文件之后、rmdir 之前崩溃 |
| ACTIVE | `<lock>` 存在且被 owner 进程持锁 | 资源可能正在使用 |
| RETAINED | 同 ACTIVE | FAILED_RETAINED；锁保持到进程退出 |
| RETIRING | owner 持锁，`<io>` 已删除，`<lock>` 已 unlink | 只由 owner 进入 |
| ORPHANED | `<lock>` 存在且未被任何进程持锁 | owner 进程已终止（内核已释放锁） |
| RECLAIMING | 回收者持锁并已复核 | 只由回收者进入 |

| 迁移 | 执行者 | 前置条件 | 动作 | 后置条件 |
| --- | --- | --- | --- | --- |
| ABSENT → PROVISIONING | owner | `<ns>` 已建 | `mkdir(<lease>)` | lease 为空目录 |
| PROVISIONING → ABSENT | 回收者（外部命名空间） | `<lock>` 不存在 | `rmdir(<lease>)`；ENOTEMPTY/ENOENT 时跳过 | owner 下一步得到 ENOENT，换新 leaseId 重试 |
| PROVISIONING → ACTIVE | owner | — | `open(<lock>, O_CREAT\|O_EXCL)` 后 `tryLock`，再 `mkdir(<io>)` | 锁被持有 |
| ACTIVE → RETAINED | owner | 关闭证明不足或超时 | 不做任何文件操作 | 锁保持 |
| ACTIVE → RETIRING | owner | 关闭证明完整（§4.4） | 删除 `<io>` 子树（不跟随符号链接）；持锁 `unlink(<lock>)` | — |
| RETIRING → ABSENT | owner | — | 释放锁并关闭 channel；`rmdir(<lease>)`，ENOENT 视为完成；随后依次尝试 rmdir 空的 `<ns>` 与 `<sub>` | 终态；正常停止不留空目录 |
| ACTIVE/RETAINED → ORPHANED | 内核 | owner 进程终止 | 释放进程的全部记录锁 | — |
| ORPHANED → RECLAIMING | 回收者 | jvmId ≠ 自身；文件系统准入；`open(<lock>)` 不带 `O_CREAT`、`O_NOFOLLOW`；`tryLock` 成功；fileKey 复核一致 | — | 回收者独占 |
| RECLAIMING → ABSENT | 回收者 | — | 删除 `<io>`（失败则保留 `<lock>` 并释放锁）；持锁 `unlink(<lock>)`；释放；`rmdir(<lease>)` | 终态 |

### 4.4 Owner 协议

**Provision(roots, label)**：

1. 对 roots 做规范化（`createDirectories` 后取 `toRealPath`），按真实路径去重，保持原顺序。
2. 计算 jvmId。
3. 对每个根，最多尝试 3 次：`createDirectories(<ns>)`（`<sub>` 与 `<ns>` 不得是符号链接）→ `createDirectory(<lease>)` → `open(<lock>, CREATE_NEW, WRITE)` → `tryLock`（必须成功）→ `createDirectory(<io>)`。前三步遇到 ENOENT（空的 `<sub>`、`<ns>` 或 lease 刚被外部回收者或本进程退役的 owner 删除）时，换新 leaseId 从头重试；其他错误即失败。
4. 任一根失败：对已就绪的根执行退役。此时尚未构造 IOManager，不存在异步使用者，退役是安全的。
5. 以全部 `<io>` 为根执行 `IOManager.create`，并物化目录；每个根必须恰好有一个 `paimon-io-*`，否则按 V1.2 语义保留全部资源并失败（D6）。
6. 登记 owner 索引（每个 ClassLoader 一份，以 `paimon-io-*` 的规范路径为键），供本 ClassLoader 的 owner 退役使用。回收者禁止读取该索引。

**Retire(spillDirs, deleted, controller)**（只在 V1.2 关闭证明成立、且 IOManager 关闭成功后调用）：

1. spillDirs 为空时直接返回完成；含未登记路径时返回“不完整”（D5）。
2. 对每个 lease：删除 `<io>` 子树 → 持锁 `unlink(<lock>)` → 释放锁并关闭 channel → `rmdir(<lease>)`。
3. 释放锁之前任一步失败：保留锁，返回不完整（沿用 ReleaseResult 与 FAILED_RETAINED 语义）。
4. 释放锁之后的 rmdir：ENOENT 视为完成（外部回收者已删除空目录）；其他错误只记审计告警，不影响完成判定。此时目录已无锁，残留按 PROVISIONING 状态处理。lease 删除后依次尝试 rmdir 空的 `<ns>` 与 `<sub>`，使正常停止不留空目录；并发的 Provision 由第 3 步的重试兜底。

**不注册 shutdown hook**：hook 在进程退出时与仍在运行的 compaction 线程并发执行，删除目录会违反“使用者退出先于删除”的前提。进程退出后，锁由内核释放，残留由后继进程回收，等效于 Flink 的 `tm_<resourceId>/tmp` 在重启时被清空。

### 4.5 回收者协议

**Reclaim(roots, budget)**：由 `PaimonService` 的初始化清理入口调用；不新增线程。

1. 对每个根：`<sub>` 不存在或是符号链接则跳过；文件系统未通过准入（§4.6）则审计一次后跳过。
2. 列出 `<sub>` 下的命名空间：名称等于自身 jvmId、不符合正则、或是符号链接的，一律跳过；**自身命名空间不列出其内容，不打开其中任何文件**。
3. 对外部命名空间中名称合规的 lease：先取得 JVM 全局互斥监视器，覆盖属性读取、open、tryLock、删除、close 的整个周期。所有插件 ClassLoader 共享同一个固定 intern 字符串监视器；不能使用类内独立 static 锁。单次 lease 处理结束后释放监视器。
   - `<lock>` 不存在：仅执行 `rmdir(<lease>)`，失败即跳过。
   - `<lock>` 存在：先记录 fileKey（NOFOLLOW），再以不带 CREATE、带 NOFOLLOW 的方式打开并 `tryLock`。返回 null 则视为活跃；锁异常保守跳过。权限/读取异常保留原始 cause、计入 failed 并限频 WARN。遵守全局互斥的回收者之间不应出现 `OverlappingFileLockException`。
   - 取得锁后复核：路径仍存在，且 fileKey 与先前一致；否则释放并跳过。
   - 按 §4.3 的 RECLAIMING → ABSENT 执行删除。
4. 命名空间内的 lease 处理完后，尝试 `rmdir(<ns>)`（非空时自然失败）。
5. 每次调用最多回收 budget 个 lease（默认 64），剩余留给下一次调用。
6. 审计事件先收集，在 channel 关闭之后统一写出；临界区内不调用日志后端和回调。
7. **禁止**读取 mtime 或任何时钟值参与判定。

### 4.6 文件系统准入控制

- 以 `Files.getFileStore(<sub>).type()` 做大小写不敏感匹配。
- **白名单**（初始）：ext2、ext3、ext4、xfs、btrfs、zfs、tmpfs、overlay、apfs。
- 其他任何类型（nfs*、cifs/smb*、fuse.*、ceph、glusterfs、9p、未知），以及类型获取失败、`fileKey` 为 null 的情况：关闭回收并限流告警。owner 侧的创建与写入不受影响。
- 白名单的最终内容由 OQ-05 确定。

### 4.7 审计

- **事件**：lease 的创建、保留、退役、回收开始/完成/失败、跳过（限流）、准入拒绝（限流）、旧布局清单（限流）。
- **出口**：写入 SLF4J logger `io.tapdata.connector.paimon.spill`（引擎级）；任务 TapLog 只写摘要。
- **字段**：协议版本、pid、`HOSTNAME`（仅环境变量）、jvmId、leaseId、tag 及创建时的表标识映射、ClassLoader 标识、根目录、文件系统类型、动作来源、锁检查结果、V1.2 关闭阶段与 executor 终止状态、保留时长、失败路径与异常。
- **约束**：不输出连接配置；审计失败不影响协议结果。

## 5. 不变量与正确性性质

| 编号 | 不变量 |
| --- | --- |
| INV-01 | 除 lease 创建者以 `O_CREAT\|O_EXCL` 打开并独占持有自己的锁文件外，任何代码都不打开本 JVM 命名空间下的任何文件；排除依据是目录名，判定发生在任何打开之前，不依赖 static 登记、ClassLoader 数量或注册顺序 |
| INV-02 | 处于 ACTIVE/RETAINED 的 lease，其锁对象的可达性不短于 lease 内任何资源使用者的生命周期 |
| INV-03 | 唯一的存活证明是 OS 记录锁；mtime、目录年龄、pid 存在性、心跳、取消结果、PDK unregister、ClassLoader.close、写入 API 返回值均不参与删除判定 |
| INV-04 | 回收者不创建、不重建锁文件；leaseId 一次性使用；回收者持锁后必须复核路径存在且 fileKey 一致 |
| INV-05 | 同 JVM 所有 ClassLoader 的回收者在 open 前取得共享监视器，直到 close 后才允许下一回收者打开锁文件；禁止通过 Overlapping 后 close 的方式协调。因为第二个 channel 的 close 可能释放第一个回收者的真实 OS 锁，PROVISIONING owner 随后可能取得锁进入 ACTIVE |
| INV-06 | 实际使用者退出的证明先于 writer.close；writer/committer/IOManager 关闭成功先于 Retire（沿用 V1.2） |
| INV-07 | RETAINED 不迁移到其他状态，直至进程退出；迟到完成不改写为 SUCCESS |
| INV-08 | 准入失败、路径或身份无法确定时，关闭回收并保留现状，不回退到时间判断 |
| INV-09 | 新代码不打开旧平铺布局的任何锁文件，不删除旧目录 |
| INV-10 | 本地 lease 不构成全局表写入锁或 exactly-once 保证 |

**安全性（S1）**：任何遵守本协议的参与者都不会删除处于 ACTIVE 或 RETAINED 的 lease。

论证：

- 删除 `<io>` 只发生在两处：Retire（只由 owner 在 RETIRING 中执行）和 RECLAIMING（要求回收者持有 `<lock>` 的记录锁，并复核路径与 fileKey）。
- 在 ACTIVE/RETAINED 期间，owner 进程持有该锁。由 A1，其他进程无法取得锁。
- 由 INV-01，owner 进程中的回收者不打开本命名空间锁文件；由 INV-05，回收进程中不会有另一个回收者打开并关闭正在被持锁的 inode。A2 所描述的失效路径被排除。
- 因此任何回收者都无法进入 RECLAIMING。
- 已经打开旧 inode 的迟到回收者，由 A6 与 INV-04 的复核排除。

**活性（L1）**：若 owner 进程终止，且之后某个 jvmId 不同、文件系统通过准入的进程在调用回收时预算未用尽，该 lease 最终会被回收。依据：进程退出时内核释放其全部记录锁。

**有界性（L2）**：Provision 在 3 次尝试内结束；Reclaim 每次调用至多成功回收 budget 个 lease；该预算不限制扫描数、单个目录删除耗时或等待 JVM 互斥的时间。

## 6. 功能要求

| ID | 要求 |
| --- | --- |
| REQ-01 | 实现 §4.1 的布局；HASH_DYNAMIC 预检使用独立 lease；RocksDB 目录必须位于 `<io>` 内（沿用 `withConfinedTempDirs`） |
| REQ-02 | 实现 §4.2 的 jvmId；提供固定期望值测试；获取失败即初始化失败 |
| REQ-03 | 实现 §4.4 Provision：根规范化去重、有界重试、O_EXCL、IOManager 构造后的保留语义 |
| REQ-04 | 回收者与 owner 退役是两个独立入口；static 集合只供 owner 索引使用，禁止参与回收判定 |
| REQ-05 | 实现 §4.5 的并发容忍：ENOENT/ENOTEMPTY 容忍、Overlapping 跳过、fileKey 复核、部分删除不计成功 |
| REQ-06 | 实现 §4.5 Reclaim，包括每次调用的预算；禁止使用时间输入 |
| REQ-07 | 实现 §4.6 文件系统准入控制 |
| REQ-08 | 实现 §4.4 Retire；保持 V1.2 的关闭顺序、取消来源桥接、FAILED_RETAINED 与超时后不开始新动作；不注册 shutdown hook |
| REQ-09 | 滚动升级：不打开旧锁文件、不删除旧目录；旧布局只做限流清单告警；子根版本未知时拒绝读取与回收；回退时不迁移、不删除新目录；发布说明要求升级后重启引擎 |
| REQ-10 | 实现 §4.7 审计 |
| REQ-11 | 部署规范：推荐每个引擎独占 diskTmpDir（K8s 用 per-pod emptyDir 或 subPath，Docker 用独立 volume）；写入连接器文档与发布说明，代码不强制 |
| REQ-12 | 交付：按 §9 以本地 PR 逐步交付 |

## 7. 与现有代码的接缝

| 位置（相对模块 `src/main/java/io/tapdata/connector/paimon/`） | 要求 |
| --- | --- |
| `util/PaimonSpillDirCleaner.java` | 保留外观 API（`resolveTmpDirs`、`splitTmpDirRoots`、`saturatedAdd`、`resolveAndCreateIOManager`、`withConfinedTempDirs`、`releaseAfterClose`）；删除平铺扫描、mtime 判定与旧注册逻辑 |
| `util/PaimonJvmIdentity.java`、`PaimonSpillLease.java`、`PaimonSpillReclaimer.java`、`PaimonSpillAudit.java`（新增） | 分别实现 §4.2、§4.4、§4.5–4.6、§4.7 |
| `write/PaimonTableWriteContextFactory.java`、`service/PaimonDynamicBucketPreflight.java` | 传入 owner 标签；其余沿用 |
| `write/PaimonTableWriteContext.java` | 构造签名不变；`closeForStop` 经外观调用 Retire |
| `service/PaimonService.java` | 初始化清理改为调用 Reclaim；旧布局清单；引擎级审计 |
| `service/PaimonStopController.java`、`PaimonStopResources.java`、`write/PaimonCompactionExecutor.java`、`PaimonCompactionLifecycle.java` | 不改语义 |

若需要修改 Engine、PDK 父加载器、注入全局对象或新增服务，属于方向变更，必须先更新 Spec 并重新确认。

## 8. 验收标准

C1–C10 与 R1–R3 已由本地 PR #2 合入（证据 §4），作为下列验收的平台前提。

| AC | 场景与断言 |
| --- | --- |
| AC-01 | jvmId：两个隔离 ClassLoader 结果相同；子进程结果不同；用 `jdk.net.hosts.file` 切换主机名解析，结果不变；固定输入的期望值固定 |
| AC-02 | 本 JVM 命名空间的打开次数为 0（同 ClassLoader 与跨 ClassLoader，用可计数打开器断言） |
| AC-03 | 现场回放回归：与 R1 相同的步骤下，外部进程删除数为 0，A 的真实 IOManager 建 channel 成功，外部 `tryLock` 失败 |
| AC-04 | 同 ClassLoader 下，owner 停在“已加锁、未返回”时并发回收，打开次数为 0 |
| AC-05 | 两个 ClassLoader 或线程并发回收同一 ORPHANED lease：无异常逃逸，至多一次成功 |
| AC-06 | 两个回收者进程处理同一 ORPHANED lease：至多一个完成，全程不新建锁文件 |
| AC-07 | 活跃 lease 的 mtime 被改为很早的时间，仍不删除；jvmId 碰撞时跳过 |
| AC-08 | 缺锁文件、符号链接、名称不合规、未知子根时保守处理；jvmId 获取失败时初始化失败且不产生 lease |
| AC-09 | owner 进程被 SIGKILL 后由其他进程回收；模拟重启：子进程 1 被杀，子进程 2 回收 |
| AC-10 | RETAINED 的 lease 被反复回收仍保持持锁 |
| AC-11 | 已取消但 worker 仍在运行时：不关闭 rewriter、不删除目录、锁保持 |
| AC-12 | V1.2 全部回归测试通过 |
| AC-13 | Factory 多根部分失败、DDL/rebuild、有限读失败时，未取得关闭证明的 lease 保持持锁 |
| AC-14 | `rocksdb-*` 位于 `<io>/paimon-io-*` 内；所有 bucket 策略与预检一致 |
| AC-15 | loader A 中保留一个活跃使用者线程，只丢弃 PDK 侧引用并施加 GC 压力，锁保持；所有引用都丢弃后锁可以被释放（允许） |
| AC-16 | 迟到回收者已打开锁文件，owner 执行 Retire 后它取得锁但复核失败，不删除任何内容 |
| AC-17 | Provision 与回收者 rmdir 空 lease/命名空间竞态时，有界重试后成功；Retire 最后 rmdir 遇 ENOENT 时为 SUCCESS，不出现 FAILED_RETAINED |
| AC-18 | `<io>` 删除失败时保留锁文件；审计 sink 抛异常不影响协议结果 |
| AC-19 | 准入：nfs4、fuse.*、未知类型、探针异常时回收数为 0、打开次数为 0、告警一次 |
| AC-20 | 重复或互为别名的根被去重，IOManager 收到的根唯一 |
| AC-21 | 释放非空的未登记路径时返回不完整 |
| AC-22 | 新旧版本并存：新回收器不打开旧锁文件；冻结的旧清理器副本不进入新子根；回退时不误删 |
| AC-23 | 引擎级审计字段齐全，任务日志只有摘要 |
| AC-24 | AC-01/02/03/06/09/15/16/17 在 Linux + JDK 17 上通过真实子进程执行 |
| AC-25 | 多表、多根反复启停后无 lease 残留、无 channel 泄漏；预算限制生效 |

## 9. 交付流程（本地 PR）

1. **分支**：集成分支 `hotfix/paimon-spill-owner-isolation`（2026-11-12 由 `feature/` 更名，推送到 fork 远端 `czy006`），位于独立 worktree，基于 develop `e3f68298`。每个步骤在主题分支 `pr/spill-owner-isolation/<NN>-<slug>` 上完成。
2. **提交**：采用约定式提交（`docs|test|feat|fix|refactor(paimon): …`），一个提交只做一件事。只提交计划内文件；工作区中他人的改动不得进入提交。
3. **合入**：以 `git merge --no-ff` 合入集成分支。合并提交正文即 PR 描述，必须包含：范围、动机、执行的命令与结果（测试数/失败数/跳过数）、变异检验结果（生产代码 PR 必做：反转一个新增条件，确认至少一个测试失败，再还原）、未覆盖项、复查命令。
4. **门禁**：合入前，定向测试必须全绿，`git diff --check` 必须干净；检查点 PR 还必须通过模块 `package`。
5. **复查**：
   - `git log --first-parent --merges hotfix/paimon-spill-owner-isolation` 列出全部 PR；
   - `git show <merge>` 查看 PR 描述；
   - `git diff <merge>^1 <merge>` 查看该 PR 的全部改动。
6. **不可变性**：已合入的 PR 不得改写历史；需要修正时提交新的 PR。
7. **远端**：集成分支推送到 fork 远端 `czy006`（与既有 `hotfix/paimon-*` 分支一致）；创建上游 PR 须用户明确要求。

## 10. 成功标准

1. AC-01–25 全部通过；跳过项须书面说明，且不能覆盖核心的多 ClassLoader、多进程与 Linux 证明。
2. AC-03 把 R1 的结果反转，即原缺陷转为持续回归保护。
3. 不改变 V1.2 的默认值、SYNC 限制、提交与取消来源、pending/offset 语义。
4. 不依赖 MBeanServer、Engine 改动、新服务、远程端口或常驻线程。
5. 每个步骤都有可复查的本地 PR 记录。
6. 发布说明写明：本次修复的是本地目录所有权，不提供全局 fencing；对 2026-11 现场故障的结论以 OQ-04 取证为准。

## 11. 代码规范

- **语言与 API 基线**：生产代码以 `<release>8` 编译，禁止 Java 9+ 语法与 API，必须用到时通过反射调用（如 `RuntimeMXBean#getPid`）；测试以 `<testRelease>11` 编译。
- **格式**：四空格缩进，命名遵循现有模块。
- **测试接缝**：只用包内可见的构造器或重载注入，禁止使用可变 static 覆盖。
- **结果表达**：代码必须区分“请求的动作”与“已证明的结果”；错误信息包含 leaseId、阶段与原因；禁止打印连接配置。

## 12. 构建与命令

本机须使用 JDK 17（connector-core 的编译目标为 17）。shell 中的 `mvn` 若被 `mvn-or-mvnw` 别名或 jenv 固定到 JDK 11，应直接调用 Maven 二进制：

~~~sh
export JAVA_HOME=<JDK 17>
MVN=<maven-3.9>/bin/mvn
# 定向测试
$MVN -o -B -pl connectors/paimon-plus-connector -am -DskipTests=false -Dtest='<类名列表>' \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test
# 模块全量
$MVN -o -B -pl connectors/paimon-plus-connector -am -DskipTests=false package
git diff --check
~~~

基线（集成分支 `213f9c49`，JDK 17.0.20，macOS）：模块 `package` 成功；paimon-plus-connector 1255 项，0 失败、0 错误，1 项跳过（`PaimonRustFsIntegrationTest` 依赖外部服务，类级跳过，与本次无关），耗时 129 s。

## 13. 边界

### Always

- 先按目录名排除本 jvmId，再做任何文件打开。
- 删除前必须持有 OS 锁，并复核路径与 fileKey；遇到任何歧义就保留现状并审计。
- 新行为以测试先行；特征化测试一旦失败，先修订 Spec。
- 每个步骤以本地 PR 合入，并附验证证据。

### Ask first

- 使用只读 `RuntimeMXBean` 以外的 JMX API；注入父加载器；引入新依赖。
- 增加 shutdown hook、常驻线程或任何时间型判断。
- 修改文件系统白名单，或在网络/FUSE 上开启回收。
- 修改 STOP 预算、FAILED_RETAINED、提交或 offset 语义。
- 修改 jvmId 派生或命名规则（必须连同子根版本一起升级）。
- 自动处理旧平铺布局目录或损坏的新布局目录。

### Never

- 除 lease 创建者打开自己的锁文件外，打开本 JVM 命名空间下的任何文件；或先打开锁文件、再判断它是否属于本 JVM。
- 让主机名或任何会变化的值参与 jvmId 派生。
- 仅凭 mtime、目录年龄、pid、心跳、isDone、cancel 返回值或 ClassLoader.close 删除目录。
- 回收者创建或重建锁文件；复用已退役的 leaseId。
- 没有关闭证明就释放 owner 锁。
- 用 Thread.stop、System.exit、Runtime.halt 或终止 Engine 来补足生命周期保证。
- 改写已合入的本地 PR 历史；把草案或原型描述为已完成的生产修复。

## 14. Open Questions

| ID | 问题 | 阻塞范围 |
| --- | --- | --- |
| OQ-03 | 宿主卸载是否可能在 worker 结束前回收 owner channel | 实现：由 AC-15 与源码审计验证 |
| OQ-04 | 现场取证：可访问 diskTmpDir 的 JVM 数量、文件系统类型、生产锁状态、删除者日志（证据 §8） | 只阻塞对外宣称“已根治” |
| OQ-05 | 生产 OS 与文件系统白名单的最终清单 | 发布 |
| OQ-06 | Linux + JDK 17 验收环境（GitHub `ubuntu-latest`、office-build runner 或本机 Docker） | AC-24 |

已决：OQ-01（jvmId 来源，见 §4.2）；OQ-02（锁文件退休，见 §4.3–4.5 与 A6）。

## 15. 来源

- 证据与逐项核实：[REVIEW-paimon-spill-owner-isolation-2026-11-12.md](../reviews/REVIEW-paimon-spill-owner-isolation-2026-11-12.md)
- 当前实现：[PaimonSpillDirCleaner](https://github.com/tapdata/tapdata-connectors/blob/e3f6829863dab17a40904f98639e55224f625f53/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/util/PaimonSpillDirCleaner.java)、[PaimonService](https://github.com/tapdata/tapdata-connectors/blob/e3f6829863dab17a40904f98639e55224f625f53/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java#L307)、[PaimonTableWriteContext.closeForStop](https://github.com/tapdata/tapdata-connectors/blob/e3f6829863dab17a40904f98639e55224f625f53/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContext.java#L354)
- 停止主 Spec：[SPEC-paimon-spill-sync-graceful-stop.md V1.2](SPEC-paimon-spill-sync-graceful-stop.md)
- Paimon 1.3.2：[FileChannelManagerImpl](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java#L62)、[AbstractFileIOChannel](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/AbstractFileIOChannel.java#L62)、[GlobalIndexAssigner](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L136)、[AbstractFileStoreWrite.close](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L304)
- Paimon Flink Connector 1.3.2：[StoreSinkWriteImpl](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreSinkWriteImpl.java#L75)、[GlobalIndexAssignerOperator](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/index/GlobalIndexAssignerOperator.java#L59)、[FileStoreLookupFunction](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/lookup/FileStoreLookupFunction.java#L164)、[AppendCompactWorkerOperator](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/AppendCompactWorkerOperator.java#L105)、[CdcRecordStoreMultiWriteOperator](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/sink/cdc/CdcRecordStoreMultiWriteOperator.java#L227)
- Flink 1.20.1：[FileChannelManagerImpl](https://github.com/apache/flink/blob/release-1.20.1/flink-runtime/src/main/java/org/apache/flink/runtime/io/disk/FileChannelManagerImpl.java#L76)、[WorkingDirectory](https://github.com/apache/flink/blob/release-1.20.1/flink-runtime/src/main/java/org/apache/flink/runtime/entrypoint/WorkingDirectory.java#L43)、[ClusterEntrypointUtils](https://github.com/apache/flink/blob/release-1.20.1/flink-runtime/src/main/java/org/apache/flink/runtime/entrypoint/ClusterEntrypointUtils.java#L171)、[TaskManagerServices](https://github.com/apache/flink/blob/release-1.20.1/flink-runtime/src/main/java/org/apache/flink/runtime/taskexecutor/TaskManagerServices.java#L342)
- JDK：[FileLock](https://docs.oracle.com/javase/8/docs/api/java/nio/channels/FileLock.html)、[RuntimeMXBean](https://docs.oracle.com/en/java/javase/17/docs/api/java.management/java/lang/management/RuntimeMXBean.html)、[FileStore.type](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileStore.html#type--)；JDK 17 源码行号见证据 §3、§5

## 16. 修订记录

- **V0.3（2026-11-12）**：吸收评审意见（证据 §7）——
  - jvmId 改为由 pid 与启动时间派生，并加入协议版本规则；
  - 补充 owner 侧 ENOENT 语义、根目录去重、未登记路径的释放即失败、构造后保留语义、审计主机名不做解析、回收预算；
  - 修正 INV-01 的措辞矛盾；明确 AC-15；
  - 引入租约状态机、安全性/活性论证、平台前提 A1–A10 与本地 PR 交付流程；
  - 故障叙述与证据迁入评审文档。
- **V0.2（2026-11-12）**：按 Flink 对照方案改为“进程命名空间 + 死亡 owner 回收”；用户批准。
- **V0.1（2026-11-12）**：初稿。

## 17. 实现记录（2026-11-12）

### 17.1 源码位置

| 协议部分 | 源码（模块 `src/main/java/io/tapdata/connector/paimon/`） | 本地 PR |
| --- | --- | --- |
| §4.2 jvmId | `util/PaimonJvmIdentity.java` | #4 |
| §4.7 审计 | `util/PaimonSpillAudit.java` | #5 |
| §4.3–4.4 租约、owner 协议 | `util/PaimonSpillLease.java`、`util/PaimonSpillFiles.java` | #6、#8 |
| §4.5–4.6 回收者、准入 | `util/PaimonSpillReclaimer.java` | #9 |
| §7 外观 | `util/PaimonSpillDirCleaner.java`：owner 侧切换（#8）；`reclaimOrphanedLeases`，删除平铺扫描（#10）；文件由 624 行降为 396 行 | #8、#10 |
| owner 标签 | `write/PaimonTableWriteContextFactory.java`（tableKey）、`service/PaimonDynamicBucketPreflight.java`（`preflight:` + tableKey） | #8 |
| 启动回收入口 | `service/PaimonService.java#reclaimOrphanedSpillLeases` | #10 |

### 17.2 验收映射

| AC | 测试（模块 `src/test/java/io/tapdata/connector/paimon/`） |
| --- | --- |
| AC-01 | `util/PaimonJvmIdentityTest` |
| AC-02 | `util/PaimonSpillReclaimerTest#ownNamespaceIsNeverOpenedEvenWhenItsLockIsFree`、`util/PaimonSpillIsolatedClassLoaderIntegrationTest#foreignLoaderNeverOpensFilesInTheSharedJvmNamespace` |
| AC-03 | `util/PaimonSpillOwnerIsolationRegressionTest`（与 PR #2 的现场复现步骤相同，结论反转） |
| AC-04 | `util/PaimonSpillIsolatedClassLoaderIntegrationTest#reclaimDuringProvisioningWindowDoesNotTouchTheNewLease` |
| AC-05 | `util/PaimonSpillReclaimerTest#concurrentReclaimersInOneJvmReclaimAtMostOnce`、`util/PaimonSpillIsolatedClassLoaderIntegrationTest#twoLoadersReclaimTheSameOrphanAtMostOnce` |
| AC-06 | `util/PaimonSpillProcessIntegrationTest#twoReclaimerProcessesReclaimTheSameOrphanOnce` |
| AC-07 | `util/PaimonSpillReclaimerTest#liveForeignLeaseWithAncientMtimesIsKept`、`util/PaimonSpillProcessIntegrationTest#otherJvmCleanerMustKeepLiveOwnerEvenWithZeroGrace`、`util/PaimonSpillReclaimerTest#reclaimerHasNoTemporalInput` |
| AC-08 | `util/PaimonSpillReclaimerTest#malformedNamesSymbolicLinksAndUnknownVersionsAreIgnored`、`#leaseWithoutLockFileIsOnlyRemovedWhenEmpty`、`#identityFailureDisablesReclaimEntirely`、`util/PaimonSpillLeaseTest#identityFailureCreatesNoProtocolDirectory` |
| AC-09 | `util/PaimonSpillProcessIntegrationTest#cleanerMustReclaimCrashedJvmResidueButKeepUnmarkedLegacyDirectory`、`#restartedEngineReclaimsThePreviousInstanceLease` |
| AC-10 | `service/PaimonDynamicBucketPreflightCleanupTest`（保留场景） |
| AC-11 | `write/PaimonCompactionSpillLifecycleIntegrationTest`、`service/PaimonServiceSyncStopIntegrationTest` |
| AC-12 | 模块全量回归（§17.4） |
| AC-13 | `write/PaimonTableWriteContextFactoryTest#rollbackCloseFailureMustRetainCauseAndReportIncompleteCleanup`、`#keyDynamicBootstrapErrorMustRetainIoWhenAssignerCleanupIsIncomplete`、`util/PaimonSpillLeaseTest#partialMultiRootFailureRetiresProvisionedRoots` |
| AC-14 | `write/PaimonTableWriteContextIntegrationTest#keyDynamicRocksDbIndexMustLiveInsideTheSpillDirectory`、`service/PaimonDynamicBucketPreflightCleanupTest`（受限视图位于租约 io/ 内） |
| AC-15 | `util/PaimonSpillIsolatedClassLoaderIntegrationTest#ownerLockSurvivesLoaderDereferenceWhileAUserIsAlive` |
| AC-16 | `util/PaimonSpillProcessIntegrationTest#lateReclaimerProcessCannotActAfterOwnerRetires`、`util/PaimonSpillReclaimerTest#lockFileRetiredBeforeLockingIsSkipped` |
| AC-17 | `util/PaimonSpillLeaseTest`（4 个竞态用例）、`util/PaimonSpillProcessIntegrationTest#provisionSurvivesAnotherProcessRemovingItsEmptyLease`、`#retireSurvivesAnotherProcessRemovingTheEmptiedLease` |
| AC-18 | `util/PaimonSpillLeaseTest#ioDeletionFailureRetainsOwnerLock`、`util/PaimonSpillReclaimerTest#ioDeletionFailureKeepsLockFileForRetry`、`util/PaimonSpillAuditTest#sinkFailuresNeverEscape` |
| AC-19 | `util/PaimonSpillReclaimerTest#unsupportedFileSystemsDisableReclaim` |
| AC-20 | `util/PaimonSpillLeaseTest#duplicateAndAliasedRootsAreProvisionedOnce`、`util/PaimonSpillDirCleanerTest#duplicateRootsShareOneLeaseRoot` |
| AC-21 | `util/PaimonSpillDirCleanerTest#releasingAnUnregisteredDirectoryIsIncomplete` |
| AC-22 | `util/PaimonSpillLegacyCoexistenceTest`、`util/PaimonSpillReclaimerTest#legacyFlatLayoutIsReportedButNeverTouched`、`util/PaimonSpillOwnerIsolationRegressionTest`（同 JVM 中的旧版本） |
| AC-23 | `util/PaimonSpillAuditTest`（字段与限流）；在真实 Engine 中查看引擎日志**未执行** |
| AC-24 | **未执行**：本机没有 Linux 环境（OQ-06） |
| AC-25 | `util/PaimonSpillResidueTest`、`util/PaimonSpillReclaimerTest#budgetBoundsReclaimsPerInvocation` |

### 17.3 实现中对 V0.3 的细化

- 多个根下的 lease 目录名各自生成（重试时更换），对应关系由 owner 索引与审计记录（PR #6，§4.1 已同步）。
- owner 退役后依次 rmdir 空的命名空间与子根，正常停止不留空目录；Provision 把 `createDirectories(<ns>)` 纳入 ENOENT 重试以兜底该竞态（PR #8，§4.3/§4.4 已同步）。
- T09 先于 T08 合入（Plan 依赖图允许），使 T08 的进程级测试可直接使用新回收器。

### 17.4 验证

- 最终全量（集成分支 `0510d607`，macOS / Oracle JDK 17.0.20 / apfs，Maven 3.9.16 离线）：`package` 成功，paimon-plus-connector 1307 项，0 失败，0 错误，1 跳过（`PaimonRustFsIntegrationTest`，依赖外部服务，与本次无关）。基线为 1255 项。
- 每个修改生产代码的 PR 都做了变异检验，所有变异均被至少一个测试检出（明细见各合并提交说明）。其中跨 ClassLoader 用例在“去掉本命名空间排除”时报出“owner 的 OS 锁必须仍被持有 ==> expected <false> but was <true>”，即原缺陷重现。
- 未覆盖：AC-24（Linux）；AC-23 的真实 Engine 日志检查；Java 8 运行时的 `getName()` 回退只有单元测试；OQ-04 现场取证；OQ-05 白名单的生产确认。

### 17.5 本地 PR

`git log --first-parent --merges hotfix/paimon-spill-owner-isolation` 可列出全部合并。对应关系：#1 `2e2f66dd`、#2 `213f9c49`、#3 `c5600c59`、#3.1 `e4032180`、#3.2 `bb245dca`、#4 `1fdfd5a2`、#5 `4bc8fe9e`、#6 `ca5457b7`、#7 `57edbcef`、#9 `80b90fd5`、#8 `db9679b2`、#10 `be8adb37`、#11 `6a990ccc`、#12 `65a1823c`、#13 `b7e2d844`、#14 `0510d607`、#15（本次文档 PR）。

## 18. 发布说明（草稿）

**Paimon 连接器：修复本地 Spill 目录被误删导致的 `Channel to path ... could not be opened (No such file or directory)`。**

- **问题：** 旧版本按“目录 10 分钟未修改”判断 Spill 目录已失效，并在同一 JVM 内重复打开其他任务（或升级后旧版本连接器）的锁文件，导致锁被操作系统释放；同机另一个引擎随后可能删除仍在使用的目录，compaction spill 失败、任务报错。
- **修复：** Spill 目录改为 `<diskTmpDir>/tapdata-paimon-v2/<进程标识>/<租约>/` 结构，只以操作系统文件锁判断目录是否在使用；本进程永不探测自己的目录；崩溃残留由同机下一次启动的引擎按锁回收。正常停止后不留任何目录。
- **兼容：** 旧版本在 `diskTmpDir` 根下创建的 `paimon-io-*` 目录与锁文件不会被打开或删除，可在所有引擎升级并重启后手动清理（见连接器文档）。升级后请重启引擎。
- **限制：** `diskTmpDir` 位于 NFS、CIFS、FUSE、Ceph 等文件系统时不自动回收遗留目录；本次修复的是本地目录所有权，不提供跨引擎的表级写入 fencing。对 2026-11 现场故障是否已根治，以现场取证（OQ-04）为准。
