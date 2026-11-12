package io.tapdata.connector.paimon.util;

import io.tapdata.connector.paimon.service.PaimonStopResources;
import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
    static final String OWNER_LOCK_SUFFIX = ".tapdata-owner.lock";

    /** An unlocked spill dir untouched for longer than this is treated as stale. */
    public static final long DEFAULT_STALE_GRACE_MS = TimeUnit.MINUTES.toMillis(10);

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
            return new IOManagerBuildResult(ioManager, paths);
        } catch (Exception | Error failure) {
            // IOManager 已构造：按 V1.2 语义保留租约与锁，不 close、不递归删除（Spec D6）。
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
     * sibling of {@code paimon-io-*} under the raw root — invisible to the live-dir registry,
     * never deleted by the IOManager close barrier, and never reclaimed by the stale cleaner
     * after a crash. Spill channels keep flowing through the delegate's FileChannelManager
     * (real {@code paimon-io-*} directories) unchanged.
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
     * Delete stale {@code paimon-io-*} spill directories under the given roots. A directory is
     * deleted only when it is not owned by a live IOManager in this JVM, its cross-process owner
     * lock can be acquired, and it has not been modified within {@code graceMs}.
     *
     * @param roots     temp roots to scan
     * @param graceMs   freshness window protecting recently active / racing dirs
     * @param onDeleted optional callback invoked per deleted dir with (canonicalPath, bytesDeleted)
     * @return number of stale directories deleted
     */
    public static int cleanupStaleSpillDirs(String[] roots, long graceMs, BiConsumer<String, Long> onDeleted) {
        return cleanupStaleSpillDirs(roots, graceMs, onDeleted, Files::delete);
    }

    static int cleanupStaleSpillDirs(
            String[] roots,
            long graceMs,
            BiConsumer<String, Long> onDeleted,
            DeleteAction deleteAction) {
        if (roots == null) {
            return 0;
        }
        if (deleteAction == null) {
            throw new IllegalArgumentException("Delete action must not be null");
        }
        int deleted = 0;
        long now = System.currentTimeMillis();
        for (String root : roots) {
            if (root == null || root.trim().isEmpty()) {
                continue;
            }
            File rootDir = new File(root.trim());
            File[] children = rootDir.listFiles((dir, name) -> name.startsWith(SPILL_DIR_PREFIX));
            if (children == null) {
                continue;
            }
            for (File child : children) {
                Path spillPath = child.toPath().toAbsolutePath().normalize();
                if (!Files.isDirectory(spillPath, LinkOption.NOFOLLOW_LINKS)) {
                    continue;
                }
                String path = canonical(child);
                File ownerFile = lockFile(spillPath.toString());
                if (!ownerFile.isFile()) {
                    // Rolling-upgrade compatibility: older connector versions did not publish an
                    // owner lock. Such a directory may still be active in an old JVM, so absence of
                    // a lock file is not permission to delete it. Legacy leftovers require an
                    // operator-controlled cleanup after all old tasks have stopped.
                    continue;
                }
                OwnerLock cleanupLock = OwnerLock.tryAcquire(ownerFile);
                if (cleanupLock == null) {
                    // Another JVM still owns this spill directory. Age alone is never sufficient
                    // evidence that a RocksDB/IOManager directory is inactive.
                    continue;
                }
                boolean removeOwnerFile = false;
                try {
                    long newestModified;
                    try {
                        newestModified = newestModified(spillPath);
                    } catch (IOException | SecurityException inspectionFailure) {
                        // Fail closed. Keep the owner marker so a later scan can retry.
                        continue;
                    }
                    if (now - newestModified < graceMs) {
                        continue;
                    }
                    PaimonSpillFiles.DeletionResult result = deleteRecursively(spillPath, deleteAction);
                    if (result.success) {
                        removeOwnerFile = true;
                        deleted++;
                        if (onDeleted != null) {
                            onDeleted.accept(path, result.bytesDeleted);
                        }
                    }
                } finally {
                    cleanupLock.close();
                    if (removeOwnerFile) {
                        deleteQuietly(cleanupLock.file);
                    }
                }
            }
        }
        return deleted;
    }

    /** Newest lastModified across the dir and its direct children without following links. */
    private static long newestModified(Path dir) throws IOException {
        long newest =
                Files.getLastModifiedTime(dir, LinkOption.NOFOLLOW_LINKS).toMillis();
        try (DirectoryStream<Path> children = Files.newDirectoryStream(dir)) {
            for (Path child : children) {
                long m =
                        Files.getLastModifiedTime(child, LinkOption.NOFOLLOW_LINKS).toMillis();
                if (m > newest) {
                    newest = m;
                }
            }
        }
        return newest;
    }

    /** Delete a tree without following symbolic links. */
    private static PaimonSpillFiles.DeletionResult deleteRecursively(Path root, DeleteAction deleteAction) {
        return PaimonSpillFiles.deleteTree(root, deleteAction::delete);
    }

    static String canonical(File file) {
        try {
            return file.getCanonicalPath();
        } catch (IOException e) {
            return file.getAbsolutePath();
        }
    }

    private static File lockFile(String canonicalSpillDir) {
        File spillDir = new File(canonicalSpillDir);
        return new File(
                spillDir.getParentFile(), "." + spillDir.getName() + OWNER_LOCK_SUFFIX);
    }

    private static void deleteQuietly(File file) {
        if (file != null && file.exists()) {
            // Best effort. A stale unlocked owner file is harmless and is reused by the next scan.
            file.delete();
        }
    }

    @FunctionalInterface
    interface DeleteAction {
        void delete(Path path) throws IOException;
    }

    private static final class OwnerLock {
        private final File file;
        private final RandomAccessFile randomAccessFile;
        private final FileChannel channel;
        private final FileLock lock;

        private OwnerLock(
                File file,
                RandomAccessFile randomAccessFile,
                FileChannel channel,
                FileLock lock) {
            this.file = file;
            this.randomAccessFile = randomAccessFile;
            this.channel = channel;
            this.lock = lock;
        }

        private static OwnerLock tryAcquire(File file) {
            RandomAccessFile randomAccessFile = null;
            FileChannel channel = null;
            try {
                randomAccessFile = new RandomAccessFile(file, "rw");
                channel = randomAccessFile.getChannel();
                FileLock lock = channel.tryLock();
                if (lock == null) {
                    closeQuietly(channel, randomAccessFile);
                    return null;
                }
                return new OwnerLock(file, randomAccessFile, channel, lock);
            } catch (OverlappingFileLockException e) {
                closeQuietly(channel, randomAccessFile);
                return null;
            } catch (IOException | RuntimeException e) {
                closeQuietly(channel, randomAccessFile);
                // Cleanup must fail closed: inability to prove exclusive ownership means skip.
                return null;
            }
        }

        private Throwable close() {
            Throwable failure = null;
            try { lock.release(); } catch (IOException | RuntimeException e) { failure = e; }
            try { channel.close(); } catch (IOException | RuntimeException e) {
                if (failure == null) { failure = e; } else { PaimonFailures.append(failure, e); }
            }
            try { randomAccessFile.close(); } catch (IOException | RuntimeException e) {
                if (failure == null) { failure = e; } else { PaimonFailures.append(failure, e); }
            }
            return failure;
        }

        private static void closeQuietly(
                FileChannel channel, RandomAccessFile randomAccessFile) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // Best effort.
                }
            }
            if (randomAccessFile != null) {
                try {
                    randomAccessFile.close();
                } catch (IOException ignored) {
                    // Best effort.
                }
            }
        }
    }
}
