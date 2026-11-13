# 三个 Paimon 工单的后续修复（基于 develop dedf5c26）

本轮修正的是综合 Review 中有证据的缺口，不据此认定客户三张工单的所有触发条件均已覆盖。

## TAP-12775：并发回收器释放彼此的 POSIX 锁

同 JVM 的回收器 B 打开与 A 相同的 inode，在 tryLock 冲突后关闭自己的 channel，会释放 A 在内核中的记录锁；A 的 Java FileLock 仍可能显示有效。外部正在 provision 的 owner 随后可以取得锁创建 io，而 A 仍继续删除。

修复在读取锁属性之前取得全 JVM 共享的 intern 字符串监视器，直到 channel 关闭后才放行。固定 key 跨 ClassLoader 共享；它保护所有根，覆盖别名路径和 inode 相同的情况。owner 仍按进程命名空间整体跳过，不探测自己的锁。没有依据 mtime、年龄或 FileLock.isValid() 判断存活。

回归 PaimonReclaimerLockSafetyTest 让 A 在持锁删除前暂停，启动同 loader / 隔离 loader 的 B，并由真实外部 JVM 尝试加锁。修复后 B 在 open 前等待，外部 JVM 无法取锁。移除互斥的负向对照，两种测试都观察到 acquired=true，证明它们能捕捉真实 OS 锁失效。

代价：同 JVM 的回收阶段按 lease 串行；大量残留/慢盘会增加任务启动等待，未做 300 任务压力验收。升级需重启引擎，避免旧 ClassLoader 的回收器仍在运行；旧代码不遵守新增监视器协议。

## TAP-13250：验证实际 bucket 生效的缓冲目录

Hadoop 在 S3A 初始化时把 fs.s3a.bucket.<bucket>.buffer.dir 传播到全局值。仅校验全局值会漏掉最终目录。

现在在 warehouse 配置阶段识别 s3a 的 bucket 覆盖，并在 PaimonS3AFileSystem.initialize 中按实际 authority 再次传播、验证，早于网络访问。规范化空 bucket 值到全局目录，并同步规范化副本中的 bucket 值，避免 super.initialize 再传播时覆盖校验结果。原配置不被跨 authority 改写。目录校验复用 PaimonLocalDirectories；NIO 创建失败保留原始 OS 异常。

测试覆盖无效 bucket 路径、两个 bucket 各自的目录、重复 Hadoop 传播、空值回退、无效但未使用的全局值与有效 bucket 覆盖。原有全局目录验证与回滚用例保留。

边界：这是 Hadoop s3a:// 路径；原生 s3:// 插件的 shaded 实现不经此入口，不能承诺其所有配置规则已被覆盖。写入探针只验证当时能写一字节，不预留 64 MiB，也不证明并发时持续可用。

## 环境诊断与 TAP-13254

- 根目录创建/解析与 JVM identity 失败新增 PROVISION_FAILED，带 owner、阶段、目录状态、原始 cause。
- 锁属性/打开失败不再记为普通 INFO 跳过：限频 WARN RECLAIM_INSPECTION_FAILED，计入失败摘要，保留 cause。
- FileStore 探针失败保留异常；IOManager 创建/登记失败记录目录状态。登记失败将证据附到 suppressed，任务完整异常栈也能看到。
- 目录状态包含 path、exists、directory、最近现存祖先的真实路径、FileStore 名称/类型、readOnly、usableBytes、totalBytes，观察失败单独记录。它们是现场观察，不能推断具体删除者或唯一根因；不采集连接密钥或整份配置。
- writer 初始化失败另经任务 Log 输出表名、bucketMode、diskTmpDir、原始异常，避免仅依赖专用 SLF4J audit。复用现有有界异步日志队列，日志后端阻塞或异常不改变资源保留决策；队列满时仍可能丢弃，沿用 droppedCount 语义。
- IncompleteCleanupException 保留类型和 cause，顶层消息增加原始失败，保持上层的关闭安全判断。
- 孤立 LATEST 回退成功时，限频 WARN 记录 hint、物理目录、扫描发现的实际 latest。TAP-13254 既有修复与实际快照/数据损坏的边界不变。不删除提示或重置提交编号。

仍需在实际引擎检查任务日志与 io.tapdata.connector.paimon.spill / PaimonS3AFileSystem / HintFileUtils 的 logger 路由。单元测试不能证明客户 appender 生效，也不能替代真实 S3 和客户挂载环境验收。

## 部署与现场取证

建议每个引擎使用独占、容量充足、持久的本地 scratch 目录；配置 diskTmpDir 与 S3A buffer 后，同时检查实际 bucket 覆盖。/tmp 可能受 systemd-tmpfiles 或容器生命周期清理，应用文件锁不能阻止外部删除者。NFS/FUSE 等不在白名单的文件系统不会自动回收；旧 paimon-io-* 只告警不删除。升级不会自动腾出这些残留空间。

遇到故障，在相同容器/挂载命名空间和引擎账号下检查日志中的目录：df -hT、df -i、findmnt -T、namei -l，并检查 systemd-tmpfiles 规则及容器 volume。容量不足、配额、inode 耗尽和挂载丢失需结合这些证据判断；应用观察不能标识执行 rm 的具体进程。外部删除者的识别需要事先开启系统审计。
