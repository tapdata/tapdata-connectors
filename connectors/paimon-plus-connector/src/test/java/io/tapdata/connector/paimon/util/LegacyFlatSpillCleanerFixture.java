package io.tapdata.connector.paimon.util;

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
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * develop {@code e3f68298} 中 {@code PaimonSpillDirCleaner} 平铺扫描逻辑的冻结副本（逐字复制自该提交第 370 行起），
 * 只用于模拟同机仍在运行旧版本连接器的引擎（AC-22）与复现原缺陷。生产代码已在 Plan T10 删除该逻辑。
 *
 * <p>旧版本的 {@code LIVE_DIRS} 属于旧引擎自己的进程；作为另一个 JVM 中的扫描者，这里恒为空。
 */
final class LegacyFlatSpillCleanerFixture {

    static final String SPILL_DIR_PREFIX = "paimon-io-";
    static final String OWNER_LOCK_SUFFIX = ".tapdata-owner.lock";
    private static final Set<String> LIVE_DIRS = Collections.emptySet();

    private LegacyFlatSpillCleanerFixture() {
    }

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
                bytesDeleted = PaimonSpillDirCleaner.saturatedAdd(bytesDeleted, fileBytes);
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
