package io.tapdata.connector.paimon.service;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Tracks Paimon {@code paimon-io-<uuid>} spill directories owned by live IOManagers in this JVM
 * and cleans up stale ones left behind by abnormally terminated JVMs (OOM/crash/SIGKILL).
 *
 * <p>The normal task-stop path closes the IOManager, which deletes its own spill dir. When the JVM
 * dies abnormally that cleanup never runs, so the dirs accumulate and exhaust local disk. Startup
 * cleanup removes such leftovers, while the live registry guarantees a sibling task's in-use dir in
 * the same JVM is never deleted.
 */
public final class PaimonSpillDirCleaner {

    /** Prefix of spill directories created by Paimon IOManager: {@code paimon-io-<uuid>}. */
    static final String SPILL_DIR_PREFIX = "paimon-io-";

    /** A spill dir not owned by this JVM and untouched for longer than this is treated as stale. */
    static final long DEFAULT_STALE_GRACE_MS = TimeUnit.MINUTES.toMillis(10);

    /** Canonical paths of spill dirs owned by live IOManagers in this JVM. */
    private static final Set<String> LIVE_DIRS = ConcurrentHashMap.newKeySet();

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
        LIVE_DIRS.addAll(paths);
        return paths;
    }

    /** Remove previously registered spill directories from the live set. */
    public static void unregisterLiveDirs(List<String> spillDirs) {
        if (spillDirs != null) {
            LIVE_DIRS.removeAll(spillDirs);
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
     * deleted only when it is NOT owned by a live IOManager in this JVM AND has not been modified
     * within {@code graceMs}.
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
}
