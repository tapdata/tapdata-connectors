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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Tracks Paimon {@code paimon-io-<uuid>} spill directories owned by live IOManagers and cleans up
 * stale ones left behind by abnormally terminated JVMs (OOM/crash/SIGKILL).
 *
 * <p>The normal task-stop path closes the IOManager, which deletes its own spill dir. When the JVM
 * dies abnormally that cleanup never runs, so the dirs accumulate and exhaust local disk. Startup
 * cleanup removes such leftovers. A JVM registry handles local ownership and a sibling advisory
 * file lock proves cross-process ownership before age-based deletion is allowed.
 */
public final class PaimonSpillDirCleaner {

    /** Prefix of spill directories created by Paimon IOManager: {@code paimon-io-<uuid>}. */
    static final String SPILL_DIR_PREFIX = "paimon-io-";
    static final String OWNER_LOCK_SUFFIX = ".tapdata-owner.lock";

    /** An unlocked spill dir untouched for longer than this is treated as stale. */
    public static final long DEFAULT_STALE_GRACE_MS = TimeUnit.MINUTES.toMillis(10);

    /** Canonical paths of spill dirs owned by live IOManagers in this JVM. */
    private static final Set<String> LIVE_DIRS = ConcurrentHashMap.newKeySet();
    /** Cross-process advisory owner locks keyed by canonical spill directory. */
    private static final Map<String, OwnerLock> OWNER_LOCKS = new ConcurrentHashMap<>();

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
     * Resolves the configured temporary-directory list to a non-blank value. When {@code configuredTmpDirs}
     * is blank, falls back to {@code java.io.tmpdir} and then to the process working directory. The
     * returned value may be a comma-separated multi-path string; callers that need individual roots
     * should use {@link #splitTmpDirRoots(String)}.
     *
     * <p>The working-directory default (rather than {@code "/tmp"}) is intentional: Paimon spill and
     * S3A upload buffers are meant to share the same disk, and {@code /tmp} frequently lives on a
     * separate, smaller partition.
     */
    public static String resolveTmpDirs(String configuredTmpDirs) {
        if (configuredTmpDirs == null || configuredTmpDirs.trim().isEmpty()) {
            return System.getProperty("java.io.tmpdir", new File(".").getAbsolutePath());
        }
        return configuredTmpDirs;
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

    /**
     * Resolves the temporary-directory list, creates a Paimon {@link IOManager} over it, and registers
     * the resulting spill directories with {@link #registerLiveDirs(IOManager)}. The returned
     * {@link IOManagerBuildResult} 保存 manager 与已注册路径。调用者在全部访问者终止后
     * 关闭 manager，再以实际目录删除结果调用 {@link #releaseAfterClose(List, boolean)}。
     */
    public static IOManagerBuildResult resolveAndCreateIOManager(String configuredTmpDirs) {
        try {
            return resolveAndCreateIOManager(configuredTmpDirs, PaimonStopResources.Scope.standalone("spill-build"));
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new IllegalStateException(failure); }
    }

    public static IOManagerBuildResult resolveAndCreateIOManager(String configuredTmpDirs,
            PaimonStopResources.Scope scope) throws Exception {
        String[] roots = splitTmpDirRoots(resolveTmpDirs(configuredTmpDirs));
        // IOManager 的目录是懒创建的；先绑定原始对象，再物化目录/登记 owner。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java#L67
        IOManager manager = scope.create("allocate IOManager", () -> IOManager.create(roots));
        return registerCreatedIOManager(manager, scope);
    }

    static IOManagerBuildResult registerCreatedIOManager(IOManager ioManager) {
        PaimonStopResources.Scope scope = PaimonStopResources.Scope.standalone("spill-register");
        scope.reserve("IOManager").bind(ioManager);
        return registerCreatedIOManager(ioManager, scope);
    }

    private static IOManagerBuildResult registerCreatedIOManager(IOManager ioManager,
            PaimonStopResources.Scope scope) {
        List<String> registered = new ArrayList<>();
        try {
            scope.check("materialize spill directories");
            List<String> paths = spillDirPaths(ioManager);
            scope.run("register spill owners", () -> registerPaths(paths, registered));
            return new IOManagerBuildResult(ioManager, paths);
        } catch (Exception | Error failure) {
            // 任一路径注册失败后完整保留；IOManager.close 会覆盖全部 root，不能删除外部 owner
            // 的目录。这里既不猜测局部关闭证明，也不通过递归删除绕过 manager 生命周期。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java#L74
            scope.retain(failure);
            if (failure instanceof Error) { throw (Error) failure; }
            throw new IllegalStateException("Paimon spill registration incomplete; resources retained: " + registered, failure);
        }
    }

    /** Carries the products of {@link #resolveAndCreateIOManager(String)} for caller cleanup. */
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
     * Materialize and register the spill directories owned by the given IOManager so startup
     * cleanup never deletes them while they are in use by this JVM.
     *
     * @return canonical paths of the registered spill directories (to be passed to {@link #releaseAfterClose})
     */
    public static List<String> registerLiveDirs(IOManager ioManager) {
        List<String> paths = spillDirPaths(ioManager);
        List<String> registered = new ArrayList<>();
        try {
            registerPaths(paths, registered);
            return paths;
        } catch (RuntimeException failure) {
            // 独立调用者仍持有 IOManager；这里不擅自删除目录，marker 留给其显式清理。
            releaseAfterClose(registered, false);
            throw failure;
        }
    }

    private static void registerPaths(List<String> paths, List<String> registered) {
        RuntimeException failure = null;
        for (String path : paths) {
            OwnerLock ownerLock = OwnerLock.tryAcquire(lockFile(path));
            if (ownerLock == null) {
                if (failure == null) {
                    failure = new IllegalStateException("Paimon spill directory is already owned: " + path);
                }
                continue;
            }
            OwnerLock raced = OWNER_LOCKS.putIfAbsent(path, ownerLock);
            if (raced != null) {
                ownerLock.close();
                if (failure == null) {
                    failure = new IllegalStateException("Paimon spill directory is already registered in this JVM: " + path);
                }
                continue;
            }
            LIVE_DIRS.add(path);
            registered.add(path);
        }
        if (failure != null) { throw failure; }
    }

    /**
     * 调用者已证明没有访问者后收尾。删除失败保留 live/owner/marker，直到进程退出；
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
                if (!deleted) {
                    retained.add(path);
                    continue;
                }
                OwnerLock ownerLock = OWNER_LOCKS.get(path);
                Throwable closeFailure;
                try {
                    closeFailure = controller == null ? (ownerLock == null ? null : ownerLock.close())
                            : controller.call("close spill owner " + path, () -> ownerLock == null ? null : ownerLock.close());
                    if (controller != null) { controller.checkAction("publish spill owner close proof"); }
                } catch (Exception | Error rejected) { closeFailure = rejected; }
                if (closeFailure != null) {
                    retained.add(path);
                    if (failure == null) { failure = closeFailure; }
                    else { PaimonFailures.append(failure, closeFailure); }
                    continue;
                }
                if (ownerLock != null) { OWNER_LOCKS.remove(path, ownerLock); }
                LIVE_DIRS.remove(path);
                if (ownerLock != null) { deleteQuietly(ownerLock.file); }
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
                if (LIVE_DIRS.contains(path)) {
                    continue;
                }
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
                    DeletionResult result = deleteRecursively(spillPath, deleteAction);
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
    private static DeletionResult deleteRecursively(Path root, DeleteAction deleteAction) {
        DeletingFileVisitor visitor = new DeletingFileVisitor(deleteAction);
        try {
            Files.walkFileTree(root, visitor);
        } catch (IOException | SecurityException traversalFailure) {
            visitor.failed = true;
        }
        boolean rootDeleted = Files.notExists(root, LinkOption.NOFOLLOW_LINKS);
        return new DeletionResult(!visitor.failed && rootDeleted, visitor.bytesDeleted);
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

    private static final class DeletingFileVisitor extends SimpleFileVisitor<Path> {
        private final DeleteAction deleteAction;
        private long bytesDeleted;
        private boolean failed;

        private DeletingFileVisitor(DeleteAction deleteAction) {
            this.deleteAction = deleteAction;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
            long fileBytes = attributes.isRegularFile() ? attributes.size() : 0L;
            delete(file, fileBytes);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException failure) {
            failed = true;
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException failure) {
            if (failure != null) {
                failed = true;
            }
            delete(dir, 0L);
            return FileVisitResult.CONTINUE;
        }

        private void delete(Path path, long fileBytes) {
            try {
                deleteAction.delete(path);
                bytesDeleted = saturatedAdd(bytesDeleted, fileBytes);
            } catch (IOException | SecurityException deleteFailure) {
                failed = true;
            }
        }
    }

    private static final class DeletionResult {
        private final boolean success;
        private final long bytesDeleted;

        private DeletionResult(boolean success, long bytesDeleted) {
            this.success = success;
            this.bytesDeleted = bytesDeleted;
        }
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
