package io.tapdata.connector.paimon.service;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    static final long DEFAULT_STALE_GRACE_MS = TimeUnit.MINUTES.toMillis(10);

    /** Canonical paths of spill dirs owned by live IOManagers in this JVM. */
    private static final Set<String> LIVE_DIRS = ConcurrentHashMap.newKeySet();
    /** Cross-process advisory owner locks keyed by canonical spill directory. */
    private static final Map<String, OwnerLock> OWNER_LOCKS = new ConcurrentHashMap<>();

    private PaimonSpillDirCleaner() {
    }

    /**
     * Materialize and register the spill directories owned by the given IOManager so startup
     * cleanup never deletes them while they are in use by this JVM.
     *
     * @return canonical paths of the registered spill directories (to be passed to {@link #unregisterLiveDirs})
     */
    public static List<String> registerLiveDirs(IOManager ioManager) {
        List<String> paths = spillDirPaths(ioManager);
        List<String> registered = new ArrayList<>();
        try {
            for (String path : paths) {
                OwnerLock ownerLock = OwnerLock.tryAcquire(lockFile(path));
                if (ownerLock == null) {
                    throw new IllegalStateException(
                            "Paimon spill directory is already owned by another process");
                }
                OwnerLock raced = OWNER_LOCKS.putIfAbsent(path, ownerLock);
                if (raced != null) {
                    ownerLock.close();
                    throw new IllegalStateException(
                            "Paimon spill directory is already registered in this JVM");
                }
                LIVE_DIRS.add(path);
                registered.add(path);
            }
            return paths;
        } catch (RuntimeException e) {
            unregisterLiveDirs(registered);
            throw e;
        }
    }

    /** Remove previously registered spill directories from the live set. */
    public static void unregisterLiveDirs(List<String> spillDirs) {
        if (spillDirs != null) {
            for (String path : spillDirs) {
                LIVE_DIRS.remove(path);
                OwnerLock ownerLock = OWNER_LOCKS.remove(path);
                if (ownerLock != null) {
                    ownerLock.close();
                    deleteQuietly(ownerLock.file);
                }
            }
        }
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
        if (roots == null) {
            return 0;
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
                if (!child.isDirectory()) {
                    continue;
                }
                String path = canonical(child);
                if (LIVE_DIRS.contains(path)) {
                    continue;
                }
                File ownerFile = lockFile(path);
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
                try {
                    if (now - newestModified(child) < graceMs) {
                        continue;
                    }
                    long size = deleteRecursively(child);
                    if (size >= 0) {
                        deleted++;
                        if (onDeleted != null) {
                            onDeleted.accept(path, size);
                        }
                    }
                } finally {
                    cleanupLock.close();
                    deleteQuietly(cleanupLock.file);
                }
            }
        }
        return deleted;
    }

    /** Newest lastModified across the dir and its direct children. */
    private static long newestModified(File dir) {
        long newest = dir.lastModified();
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                long m = f.lastModified();
                if (m > newest) {
                    newest = m;
                }
            }
        }
        return newest;
    }

    /**
     * Recursively delete a file/dir.
     *
     * @return total bytes deleted, or -1 if any deletion failed
     */
    private static long deleteRecursively(File file) {
        long total = 0;
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                long sub = deleteRecursively(child);
                if (sub < 0) {
                    return -1;
                }
                total += sub;
            }
        }
        long len = file.isFile() ? file.length() : 0;
        if (!file.delete()) {
            return -1;
        }
        return total + len;
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

        private void close() {
            try {
                lock.release();
            } catch (IOException ignored) {
                // Best effort; closing the channel also releases the process lock.
            }
            closeQuietly(channel, randomAccessFile);
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
