package io.tapdata.connector.paimon.util;

import io.tapdata.connector.paimon.service.PaimonStopResources;
import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Paimon spill 目录的外观（SPEC-paimon-spill-owner-isolation §7）。
 *
 * <p>owner 侧：{@link #resolveAndCreateIOManager} 先以 {@link PaimonSpillLease} 建立进程命名空间中的租约并持有
 * OS 记录锁，再以各租约的 {@code io/} 为根创建 Paimon IOManager；{@link #releaseAfterClose} 在 IOManager 关闭
 * 成功之后退役租约。owner 索引只供本 ClassLoader 的退役使用，回收者从不读取（INV-01、INV-03）。
 */
public final class PaimonSpillDirCleaner {

    /** Prefix of spill directories created by Paimon IOManager: {@code paimon-io-<uuid>}. */
    static final String SPILL_DIR_PREFIX = "paimon-io-";


    /**
     * owner 索引：{@code paimon-io-*} 规范路径 → 所属租约根。强引用保证锁对象可达性不短于租约（INV-02）；
     * 只供本 ClassLoader 的 {@link #releaseAfterClose} 使用，不参与任何删除判定。
     */
    private static final Map<String, PaimonSpillLease.Root> OWNERS = new ConcurrentHashMap<>();

    private PaimonSpillDirCleaner() {
    }

    /**
     * Returns the sum of {@code left} and {@code right} unless it would overflow {@link Long#MAX_VALUE},
     * in which case {@code Long.MAX_VALUE} is returned. {@code right <= 0} is returned as {@code left + right}
     * (the non-overflowing direction).
     */
    public static long saturatedAdd(long left, long right) {
        if (right > 0L && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    /**
     * Resolve and validate the complete list before any caller creates directories.
     * Blank configuration uses the JVM temporary directory, then /tmp; never the working directory.
     * Absolute local paths are a connector contract: Paimon itself accepts relative roots.
     */
    public static String resolveTmpDirs(String configuredTmpDirs) {
        String resolved = configuredTmpDirs;
        if (resolved == null || resolved.trim().isEmpty()) {
            resolved = System.getProperty("java.io.tmpdir");
            if (resolved == null || resolved.trim().isEmpty()) { resolved = "/tmp"; }
        }
        // Match Paimon 1.3.2 separators but preserve empty entries so trailing separators fail too.
        // Source: https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java
        String[] roots = resolved.split(",|" + java.util.regex.Pattern.quote(File.pathSeparator), -1);
        for (int i = 0; i < roots.length; i++) {
            String root = roots[i].trim();
            try {
                if (root.isEmpty() || !java.nio.file.Paths.get(root).isAbsolute()
                        || root.chars().anyMatch(Character::isISOControl)) {
                    throw new IllegalArgumentException("Invalid temporary directory");
                }
            } catch (IllegalArgumentException invalid) {
                // Do not echo the value: an accidentally pasted configuration may contain secrets.
                throw new IllegalArgumentException("diskTmpDir entry " + (i + 1)
                        + " must be an absolute local directory, not table properties, a URI or a relative path");
            }
            roots[i] = root;
        }
        // Hadoop S3A accepts comma-separated roots; use one normalized representation for both users.
        // Source: https://hadoop.apache.org/docs/r3.3.6/hadoop-aws/tools/hadoop-aws/index.html#Buffering_upload_data_on_disk_fs.s3a.fast.upload.buffer.3Ddisk
        return String.join(",", roots);
    }

    /**
     * Splits a resolved temporary-directory list (as produced by {@link #resolveTmpDirs(String)}) into
     * individual roots. This is a thin wrapper over Paimon's {@link IOManagerImpl#splitPaths}: it
     * splits on comma / path-separator and does not trim whitespace or drop empty segments, so
     * callers that need clean roots must sanitize the entries themselves.
     */
    public static String[] splitTmpDirRoots(String resolvedTmpDirs) {
        return IOManagerImpl.splitPaths(resolvedTmpDirs);
    }

    /** 独立调用者（测试、诊断）使用的版本：以独立 scope 记账。 */
    public static IOManagerBuildResult resolveAndCreateIOManager(String configuredTmpDirs, String ownerLabel) {
        try {
            return resolveAndCreateIOManager(configuredTmpDirs, ownerLabel,
                    PaimonStopResources.Scope.standalone("spill-build"));
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * Provision 租约（Spec §4.4），以各租约的 {@code io/} 为根创建 IOManager，物化目录并登记 owner 索引。
     * 租约在 IOManager 之前绑定进 scope，超时冻结时迟到的资源同样被强引用保留。
     */
    public static IOManagerBuildResult resolveAndCreateIOManager(String configuredTmpDirs, String ownerLabel,
            PaimonStopResources.Scope scope) throws Exception {
        String[] roots = splitTmpDirRoots(resolveTmpDirs(configuredTmpDirs));
        PaimonSpillLease lease = scope.create("provision spill lease",
                () -> PaimonSpillLease.provision(Arrays.asList(roots), ownerLabel));
        IOManager[] created = new IOManager[1];
        IOManager manager;
        try {
            // IOManager 的目录是懒创建的；先绑定原始对象，再物化目录/登记 owner。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java#L60
            manager = scope.create("allocate IOManager", () -> created[0] = IOManager.create(lease.ioRoots()));
        } catch (Exception | Error failure) {
            if (created[0] == null) {
                PaimonSpillAudit.defaultAudit().record(PaimonSpillAudit.Event.IO_MANAGER_BUILD_FAILED,
                        PaimonSpillAudit.details("owner", ownerLabel, "roots",
                                Arrays.asList(lease.ioRoots()), "directoryState",
                                PaimonLocalDirectories.describeRoots(Arrays.asList(lease.ioRoots()))), failure);
                // 尚无 IOManager，也就没有任何使用者：立即退役全部根（Spec §4.4 第 4 步）。
                for (PaimonSpillLease.Root root : lease.roots()) {
                    Throwable rollback = root.retire();
                    if (rollback != null) {
                        failure.addSuppressed(rollback);
                    }
                }
            }
            throw failure;
        }
        return registerCreatedIOManager(manager, lease, scope);
    }

    private static IOManagerBuildResult registerCreatedIOManager(IOManager ioManager, PaimonSpillLease lease,
            PaimonStopResources.Scope scope) {
        try {
            scope.check("materialize spill directories");
            List<String> paths = spillDirPaths(ioManager);
            Map<String, PaimonSpillLease.Root> owned = ownedRoots(paths, lease);
            scope.run("register spill owners", () -> OWNERS.putAll(owned));
            PaimonSpillAudit.defaultAudit().record(PaimonSpillAudit.Event.IO_MANAGER_CREATED,
                    PaimonSpillAudit.details("owner", lease.ownerLabel(), "spillDirs", paths));
            return new IOManagerBuildResult(ioManager, paths);
        } catch (Exception | Error failure) {
            // IOManager 已构造：按 V1.2 语义保留租约与锁，不 close、不递归删除（Spec D6）。
            List<java.util.Map<String, Object>> state =
                    PaimonLocalDirectories.describeRoots(Arrays.asList(lease.ioRoots()));
            failure.addSuppressed(new IOException("Paimon spill directory evidence: owner="
                    + lease.ownerLabel() + ", directoryState=" + state));
            PaimonSpillAudit.defaultAudit().record(PaimonSpillAudit.Event.SPILL_REGISTRATION_INCOMPLETE,
                    PaimonSpillAudit.details("owner", lease.ownerLabel(), "directoryState", state), failure);
            scope.retain(failure);
            if (failure instanceof Error) { throw (Error) failure; }
            throw new IllegalStateException("Paimon spill registration incomplete; lease retained", failure);
        }
    }

    /**
     * 每个租约根必须恰好对应一个位于其 {@code io/} 之下的 {@code paimon-io-*}。Paimon 1.3.2 mkdir 失败时会静默
     * 跳过该根，构造之后也不再补建（FileChannelManagerImpl#createFiles）。
     * https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java#L72
     */
    private static Map<String, PaimonSpillLease.Root> ownedRoots(List<String> paths, PaimonSpillLease lease) {
        Map<String, PaimonSpillLease.Root> owned = new LinkedHashMap<>();
        Set<PaimonSpillLease.Root> covered = Collections.newSetFromMap(new IdentityHashMap<>());
        for (String path : paths) {
            Path parent = Paths.get(path).getParent();
            PaimonSpillLease.Root match = null;
            for (PaimonSpillLease.Root root : lease.roots()) {
                if (root.io.equals(parent)) {
                    match = root;
                }
            }
            if (match == null || !covered.add(match)) {
                throw new IllegalStateException("Paimon spill directory is outside its lease: " + path);
            }
            owned.put(path, match);
        }
        if (covered.size() != lease.roots().size()) {
            throw new IllegalStateException("Paimon did not materialize every spill lease root: " + paths);
        }
        return owned;
    }

    /** {@link #resolveAndCreateIOManager} 的产物：IOManager 与已登记的 {@code paimon-io-*} 规范路径。 */
    public static final class IOManagerBuildResult {
        private final IOManager ioManager;
        private final List<String> spillDirs;

        private IOManagerBuildResult(IOManager ioManager, List<String> spillDirs) {
            this.ioManager = ioManager;
            this.spillDirs = spillDirs;
        }

        public IOManager ioManager() {
            return ioManager;
        }

        public List<String> spillDirs() {
            return spillDirs;
        }
    }

    /**
     * Delegating {@link IOManager} whose {@link IOManager#tempDirs()} reports the canonical
     * spill directories instead of the raw configured roots. Paimon 1.3.2's
     * {@code GlobalIndexAssigner} picks {@code ioManager.tempDirs()} to create its
     * {@code rocksdb-<uuid>} index directory; without this confinement that directory is a
     * sibling of the lease under the raw root — outside the owner lock's protection, never
     * deleted by the IOManager close barrier, and never reclaimed after a crash. Spill channels
     * keep flowing through the delegate's FileChannelManager (real {@code paimon-io-*}
     * directories inside the lease) unchanged.
     *
     * <p>Source: {@code paimon-core/src/main/java/org/apache/paimon/crosspartition/
     * GlobalIndexAssigner.java#open}, lines 136-142. Baseline:
     * 已与 Paimon 1.3.2 sources JAR 逐字节核对。
     * https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L136
     */
    public static IOManager withConfinedTempDirs(IOManager delegate, List<String> spillDirs) {
        return new ConfinedTempDirsIOManager(delegate, spillDirs);
    }

    private static final class ConfinedTempDirsIOManager implements IOManager {
        private final IOManager delegate;
        private final String[] confinedDirs;

        private ConfinedTempDirsIOManager(IOManager delegate, List<String> spillDirs) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            if (spillDirs == null || spillDirs.isEmpty()) {
                throw new IllegalArgumentException("Confined temp dirs must not be empty");
            }
            this.confinedDirs = spillDirs.toArray(new String[0]);
        }

        @Override
        public FileIOChannel.ID createChannel() {
            return delegate.createChannel();
        }

        @Override
        public FileIOChannel.ID createChannel(String prefix) {
            return delegate.createChannel(prefix);
        }

        @Override
        public String[] tempDirs() {
            return confinedDirs.clone();
        }

        @Override
        public FileIOChannel.Enumerator createChannelEnumerator() {
            return delegate.createChannelEnumerator();
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID)
                throws IOException {
            return delegate.createBufferFileWriter(channelID);
        }

        @Override
        public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID)
                throws IOException {
            return delegate.createBufferFileReader(channelID);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    /**
     * 调用者已证明没有访问者、且 IOManager 已关闭后退役租约。退役失败保留锁直到进程退出；
     * 访问者仍可能存在时不能调用。Paimon IOManager.close 的目录删除可抛 IOException。
     * https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java#L125
     */
    public static ReleaseResult releaseAfterClose(List<String> spillDirs, boolean deleted) {
        return releaseAfterClose(spillDirs, deleted, null);
    }

    public static ReleaseResult releaseAfterClose(List<String> spillDirs, boolean deleted,
            io.tapdata.connector.paimon.service.PaimonStopController controller) {
        List<String> retained = new ArrayList<>();
        Throwable failure = null;
        if (spillDirs != null) {
            for (String path : spillDirs) {
                PaimonSpillLease.Root root = OWNERS.get(path);
                if (root == null) {
                    // 生产中的 IOManager 都经租约创建；未登记即调用方不变量被破坏，不能静默报告成功（Spec D5）。
                    PaimonSpillAudit.defaultAudit().record(PaimonSpillAudit.Event.UNREGISTERED_SPILL_DIR,
                            PaimonSpillAudit.details("dir", path));
                    retained.add(path);
                    failure = PaimonFailures.append(failure,
                            new IllegalStateException("Unregistered Paimon spill directory: " + path));
                    continue;
                }
                if (!deleted) {
                    retained.add(path);
                    continue;
                }
                Throwable retireFailure;
                try {
                    retireFailure = controller == null ? root.retire()
                            : controller.call("retire spill lease " + path, root::retire);
                    if (controller != null) { controller.checkAction("publish spill owner close proof"); }
                } catch (Exception | Error rejected) { retireFailure = rejected; }
                if (retireFailure != null) {
                    retained.add(path);
                    failure = PaimonFailures.append(failure, retireFailure);
                    continue;
                }
                OWNERS.remove(path, root);
            }
        }
        if (!retained.isEmpty() && failure == null) {
            PaimonSpillAudit.defaultAudit().record(PaimonSpillAudit.Event.RELEASE_INCOMPLETE,
                    PaimonSpillAudit.details("retained", retained, "reason", "missing-close-proof"));
            failure = new IOException("Paimon spill release lacks close proof: " + retained);
        }
        return new ReleaseResult(retained, failure);
    }

    public static final class ReleaseResult {
        private final List<String> retained;
        private final Throwable failure;
        private ReleaseResult(List<String> retained, Throwable failure) {
            this.retained = java.util.Collections.unmodifiableList(retained);
            this.failure = failure;
        }
        public boolean complete() { return retained.isEmpty() && failure == null; }
        public List<String> retainedPaths() { return retained; }
        public Throwable failure() { return failure; }
    }

    private static List<String> spillDirPaths(IOManager ioManager) {
        List<String> paths = new ArrayList<>();
        if (ioManager instanceof IOManagerImpl) {
            // Note: getSpillingDirectories() lazily creates the dirs if absent, which is what we want
            // so the dir exists and is protected from the moment a sibling cleanup could observe it.
            File[] dirs = ((IOManagerImpl) ioManager).getSpillingDirectories();
            if (dirs != null) {
                for (File dir : dirs) {
                    paths.add(canonical(dir));
                }
            }
        }
        return paths;
    }

    /**
     * 启动时回收其他 JVM 已终止后遗留的租约（Spec §4.5–4.6）。本 JVM 的命名空间按名称跳过、不打开任何文件；
     * 旧平铺布局只计数，不打开、不删除（INV-09）。
     *
     * @param roots       已规范化的根目录
     * @param onReclaimed 每个被回收的租约回调 (lease 路径, 删除的普通文件字节数)，在其锁释放之后调用
     */
    public static ReclaimSummary reclaimOrphanedLeases(Collection<String> roots, BiConsumer<String, Long> onReclaimed) {
        PaimonSpillReclaimer.Result result = PaimonSpillReclaimer.standard().reclaim(roots);
        if (onReclaimed != null) {
            for (PaimonSpillReclaimer.Reclaimed reclaimed : result.reclaimed) {
                onReclaimed.accept(reclaimed.lease, reclaimed.bytes);
            }
        }
        return new ReclaimSummary(result.reclaimed.size(), result.failed, result.legacyDirectories,
                result.budgetExhausted);
    }

    private static final java.util.concurrent.atomic.AtomicInteger RECLAIM_THREAD_SEQ =
            new java.util.concurrent.atomic.AtomicInteger();

    /**
     * 与 PaimonCompactionExecutor 保持同一约定：扫描线程显式沿用提交方的 TCCL，并在线程名里带上实例标识。
     * 计数器只在一个 ClassLoader 内递增，因此名字里再加提交方 ClassLoader 的 identity hash，
     * 否则几百个连接器实例都叫 paimon-spill-reclaim-1，线程栈无法归属到具体实例。
     * 显式设置 TCCL 而不是依赖 new Thread 的隐式继承：行为相同，但意图可见，
     * 将来改用非继承的线程工厂/线程组时不会静默改变。空闲 worker 会在 30s 内被下一个提交复用，
     * 那时携带的是最初的提交方 loader。
     */
    static Thread newReclaimThread(Runnable task) {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        Thread thread = new Thread(task, "paimon-spill-reclaim-"
                + Integer.toHexString(System.identityHashCode(contextLoader))
                + "-" + RECLAIM_THREAD_SEQ.incrementAndGet());
        thread.setContextClassLoader(contextLoader);
        thread.setDaemon(true);
        return thread;
    }

    private static final java.util.concurrent.ThreadPoolExecutor RECLAIM_WORKER =
            new java.util.concurrent.ThreadPoolExecutor(0, 1, 30, java.util.concurrent.TimeUnit.SECONDS,
                    new java.util.concurrent.SynchronousQueue<>(), PaimonSpillDirCleaner::newReclaimThread,
                    new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());

    /**
     * Bound task startup waiting to 200 ms. At most one scan per ClassLoader is in flight,
     * with no pending queue; busy callers defer to the next startup. A running scan is never
     * cancelled: its OS lock and the cross-ClassLoader monitor protect deletion to completion.
     */
    public static void reclaimForStartup(Collection<String> roots, io.tapdata.entity.logger.Log log) {
        reclaimForStartup(roots, log, RECLAIM_WORKER, 200);
    }

    static void reclaimForStartup(Collection<String> roots, io.tapdata.entity.logger.Log log,
            java.util.concurrent.ExecutorService worker, long waitMillis) {
        final List<String> snapshot = new ArrayList<>(roots);
        java.util.concurrent.Future<?> scan;
        try {
            scan = worker.submit(() -> {
                try {
                    PaimonSpillReclaimer.Result result = PaimonSpillReclaimer
                            .standard(PaimonSpillAudit.withTaskLog(log)).reclaim(snapshot);
                    io.tapdata.connector.paimon.service.PaimonStopLog.offer(log,
                            "Paimon spill reclaim finished: roots=" + snapshot + ", reclaimed="
                                    + result.reclaimed.size() + ", failed=" + result.failed
                                    + ", legacyDirectories=" + result.legacyDirectories
                                    + ", budgetExhausted=" + result.budgetExhausted, null);
                    if (result.legacyDirectories > 0) {
                        io.tapdata.connector.paimon.service.PaimonStopLog.offerWarn(log,
                                "Legacy Paimon spill directories remain under " + snapshot
                                        + "; observedBytes=" + result.legacyBytes
                                        + ", sizeIncomplete=" + result.legacySizeIncomplete
                                        + "; upgrade does not free their disk space. Stop all engines and old tasks "
                                        + "using these roots, inspect du -sh -- <root>/paimon-io-*, then remove only "
                                        + "confirmed unused directories. See connector documentation.", null);
                    }
                } catch (RuntimeException | LinkageError failure) {
                    // The caller may already have timed out: the Future alone is not a log outlet.
                    io.tapdata.connector.paimon.service.PaimonStopLog.offerError(log,
                            "Paimon spill background reclaim failed: roots=" + snapshot, failure);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException busy) {
            if (worker.isShutdown()) {
                // 异常路径：worker 已关闭，本次没有扫描。生产路径不会走到（RECLAIM_WORKER 不关闭）。
                io.tapdata.connector.paimon.service.PaimonStopLog.offerWarn(log,
                        "Paimon spill reclaim skipped: reclaim worker unavailable; roots=" + snapshot, null);
                return;
            }
            // 正常路径：同一 ClassLoader 一次只跑一个扫描，后续启动顺延到下次。
            io.tapdata.connector.paimon.service.PaimonStopLog.offer(log,
                    "Paimon spill reclaim deferred: another scan is running; roots=" + snapshot, null);
            return;
        }
        try {
            scan.get(waitMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException pending) {
            // 正常路径：慢盘或多任务同时启动时每次都会走到这里，不应记 WARN。
            io.tapdata.connector.paimon.service.PaimonStopLog.offer(log,
                    "Paimon spill reclaim continues in background after " + waitMillis
                            + " ms; task startup proceeds; roots=" + snapshot, null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            io.tapdata.connector.paimon.service.PaimonStopLog.offerWarn(log,
                    "Paimon spill reclaim wait interrupted; scan keeps its lock until finished", interrupted);
        } catch (java.util.concurrent.ExecutionException failure) {
            io.tapdata.connector.paimon.service.PaimonStopLog.offerError(log,
                    "Paimon spill reclaim failed: roots=" + snapshot, failure.getCause());
        }
    }

    /** 一次启动回收的摘要，供任务 TapLog 输出；逐条审计已写入引擎日志。 */
    public static final class ReclaimSummary {
        private final int reclaimed;
        private final int failed;
        private final int legacyDirectories;
        private final boolean budgetExhausted;

        private ReclaimSummary(int reclaimed, int failed, int legacyDirectories, boolean budgetExhausted) {
            this.reclaimed = reclaimed;
            this.failed = failed;
            this.legacyDirectories = legacyDirectories;
            this.budgetExhausted = budgetExhausted;
        }

        public int reclaimed() { return reclaimed; }
        public int failed() { return failed; }
        public int legacyDirectories() { return legacyDirectories; }
        public boolean budgetExhausted() { return budgetExhausted; }
    }

    static String canonical(File file) {
        try {
            return file.getCanonicalPath();
        } catch (IOException e) {
            return file.getAbsolutePath();
        }
    }
}
