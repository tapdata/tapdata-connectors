package io.tapdata.connector.paimon.util;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * 回收其他 JVM 已终止后遗留的 Spill 租约（SPEC-paimon-spill-owner-isolation §4.5–4.6）。
 *
 * <p>安全性只依赖 OS 记录锁：本 JVM 的命名空间按目录名整体跳过，在任何文件打开之前完成（INV-01），因此本进程
 * 永不探测、也就永不会因关闭探测 channel 而释放自己的锁；外部 lease 只有在锁可取得、且持锁后路径与 fileKey
 * 复核一致时才删除（INV-04）。存活判断不读取 mtime 或时钟（INV-03）；单调时钟仅限制旧目录体积观察预算。
 */
final class PaimonSpillReclaimer {

    // String.intern is shared by all ClassLoaders in this JVM. A class-local monitor is
    // insufficient: closing ANY fd for an inode releases this process's POSIX locks on it.
    // Keep this key stable across connector releases using this protocol. Serialize the
    // entire attribute/open/lock/delete/close cycle, even when roots alias the same inode.
    private static final Object RECLAIM_MONITOR =
            "io.tapdata.connector.paimon.spill.v2.reclaimer.open-close".intern();

    static final int DEFAULT_BUDGET = 64;
    static final Set<String> SUPPORTED_FILE_STORE_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "tmpfs", "overlay", "apfs")));
    private static final String LEGACY_PREFIX = "paimon-io-";

    @FunctionalInterface
    interface FileStoreTypeProbe {
        String type(Path path) throws IOException;
    }

    @FunctionalInterface
    interface LockFileOpener {
        FileChannel open(Path lockFile) throws IOException;
    }

    /** {@code FileStore.type()} 的格式依实现而定，只用作白名单匹配。 */
    static final FileStoreTypeProbe DEFAULT_TYPE_PROBE = path -> Files.getFileStore(path).type();

    /** 不带 CREATE：回收者永不创建或重建锁文件（INV-04，PaimonSpillPlatformContractTest C6）。 */
    static final LockFileOpener DEFAULT_OPENER = lockFile -> FileChannel.open(lockFile,
            StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);

    private final Supplier<String> selfIdentity;
    private final FileStoreTypeProbe typeProbe;
    private final LockFileOpener opener;
    private final PaimonSpillFiles.DeleteAction deleteAction;
    private final PaimonSpillAudit audit;
    private final int budget;

    PaimonSpillReclaimer(Supplier<String> selfIdentity, FileStoreTypeProbe typeProbe, LockFileOpener opener,
            PaimonSpillFiles.DeleteAction deleteAction, PaimonSpillAudit audit, int budget) {
        this.selfIdentity = selfIdentity;
        this.typeProbe = typeProbe;
        this.opener = opener;
        this.deleteAction = deleteAction;
        this.audit = audit;
        this.budget = budget;
    }

    static PaimonSpillReclaimer standard() {
        return standard(PaimonSpillAudit.defaultAudit());
    }

    static PaimonSpillReclaimer standard(PaimonSpillAudit audit) {
        return new PaimonSpillReclaimer(PaimonJvmIdentity::current, DEFAULT_TYPE_PROBE, DEFAULT_OPENER,
                PaimonSpillFiles.FILES_DELETE, audit, DEFAULT_BUDGET);
    }

    Result reclaim(Collection<String> roots) {
        Result result = new Result();
        String self;
        try {
            self = selfIdentity.get();
        } catch (RuntimeException failure) {
            // 身份不可用时不知道哪些命名空间属于本进程，必须完全关闭回收（Spec §4.2）。
            audit.record(PaimonSpillAudit.Event.ADMISSION_DENIED,
                    PaimonSpillAudit.details("reason", "identity-unavailable"), failure);
            return result;
        }
        if (!PaimonJvmIdentity.isNamespaceName(self)) {
            audit.record(PaimonSpillAudit.Event.ADMISSION_DENIED,
                    PaimonSpillAudit.details("reason", "identity-malformed"));
            return result;
        }
        for (String configured : roots) {
            if (result.budgetExhausted) {
                break;
            }
            reclaimRoot(Paths.get(configured), self, result);
        }
        return result;
    }

    private void reclaimRoot(Path root, String self, Result result) {
        reportLegacyLayout(root, result);
        Path subroot = root.resolve(PaimonSpillLease.SUBROOT);
        if (!Files.isDirectory(subroot, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        if (!admitted(root, subroot)) {
            return;
        }
        for (Path namespace : list(subroot)) {
            if (Thread.interrupted()) {
                audit.record(PaimonSpillAudit.Event.RECLAIM_INTERRUPTED,
                        PaimonSpillAudit.details("root", root, "reclaimed", result.reclaimed.size()));
                Thread.currentThread().interrupt();
                return;
            }
            String name = namespace.getFileName().toString();
            if (name.equals(self)) {
                continue;
            }
            if (!PaimonJvmIdentity.isNamespaceName(name) || !Files.isDirectory(namespace, LinkOption.NOFOLLOW_LINKS)) {
                continue;
            }
            for (Path lease : list(namespace)) {
                if (Thread.interrupted()) {
                    audit.record(PaimonSpillAudit.Event.RECLAIM_INTERRUPTED,
                            PaimonSpillAudit.details("root", root, "reclaimed", result.reclaimed.size()));
                    Thread.currentThread().interrupt();
                    return;
                }
                if (result.reclaimed.size() >= budget) {
                    result.budgetExhausted = true;
                    audit.record(PaimonSpillAudit.Event.RECLAIM_BUDGET_EXHAUSTED, PaimonSpillAudit.details(
                            "root", root, "budget", budget));
                    return;
                }
                if (PaimonSpillLease.LEASE_NAME.matcher(lease.getFileName().toString()).matches()
                        && Files.isDirectory(lease, LinkOption.NOFOLLOW_LINKS)) {
                    reclaimLease(root, lease, result);
                }
            }
            PaimonSpillFiles.removeEmptyDirectory(namespace);
        }
    }

    private boolean admitted(Path root, Path subroot) {
        String type;
        Throwable probeFailure = null;
        try {
            type = typeProbe.type(subroot);
        } catch (IOException | RuntimeException failure) {
            type = null;
            probeFailure = failure;
        }
        String normalized = type == null ? "unknown" : type.trim().toLowerCase(Locale.ROOT);
        if (SUPPORTED_FILE_STORE_TYPES.contains(normalized)) {
            return true;
        }
        audit.recordRateLimited(PaimonSpillAudit.Event.ADMISSION_DENIED, root.toString(),
                PaimonSpillAudit.details("root", root, "fileStoreType", normalized), probeFailure);
        return false;
    }

    private void reclaimLease(Path root, Path lease, Result result) {
        synchronized (RECLAIM_MONITOR) {
            reclaimLeaseExclusively(root, lease, result);
        }
    }

    private void reclaimLeaseExclusively(Path root, Path lease, Result result) {
        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            return;
        }
        Path lockFile = lease.resolve(PaimonSpillLease.LOCK_FILE);
        BasicFileAttributes before;
        try {
            before = Files.readAttributes(lockFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException noLock) {
            // PROVISIONING 或 owner 退役中途崩溃：只允许 rmdir 空目录（Spec §4.3）。
            PaimonSpillFiles.removeEmptyDirectory(lease);
            return;
        } catch (IOException | RuntimeException failure) {
            inspectionFailed(root, lease, "lock-attributes-unreadable", failure, result);
            return;
        }
        if (!before.isRegularFile() || before.fileKey() == null) {
            skip(root, lease, "lock-file-unsupported", result);
            return;
        }
        FileChannel channel;
        try {
            channel = opener.open(lockFile);
        } catch (NoSuchFileException retiredMeanwhile) {
            return;
        } catch (IOException | RuntimeException failure) {
            inspectionFailed(root, lease, "lock-open-failed", failure, result);
            return;
        }
        Outcome outcome;
        try {
            outcome = reclaimLocked(lease, lockFile, before, channel);
        } finally {
            try {
                // JVM-wide monitor prevents another reclaimer from opening/closing this inode until here.
                channel.close();
            } catch (IOException ignored) {
                // 关闭失败不改变已完成的删除，也不会影响 owner。
            }
        }
        // 审计与回调都在 channel 关闭之后执行（Spec §4.5 第 6 步）。
        switch (outcome.kind) {
            case RECLAIMED:
                if (PaimonSpillFiles.removeEmptyDirectory(lease) == PaimonSpillFiles.EmptyDirectoryRemoval.KEPT) {
                    audit.record(PaimonSpillAudit.Event.LEASE_RESIDUE,
                            PaimonSpillAudit.details("root", root, "lease", lease, "reason", "lease-directory-not-removed"));
                }
                result.reclaimed.add(new Reclaimed(lease.toString(), outcome.bytes));
                audit.record(PaimonSpillAudit.Event.LEASE_RECLAIMED,
                        PaimonSpillAudit.details("root", root, "lease", lease, "bytes", outcome.bytes));
                break;
            case FAILED:
                result.failed++;
                audit.record(PaimonSpillAudit.Event.RECLAIM_FAILED,
                        PaimonSpillAudit.details("root", root, "lease", lease), outcome.failure);
                break;
            case LIVE:
                result.live++;
                break;
            default:
                skip(root, lease, outcome.kind.name().toLowerCase(Locale.ROOT), result);
                break;
        }
    }

    private Outcome reclaimLocked(Path lease, Path lockFile, BasicFileAttributes before, FileChannel channel) {
        FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException otherReclaimerInThisJvm) {
            return Outcome.of(Kind.OVERLAPPING);
        } catch (IOException | RuntimeException failure) {
            return Outcome.failed(failure);
        }
        if (lock == null) {
            return Outcome.of(Kind.LIVE);
        }
        BasicFileAttributes after;
        try {
            after = Files.readAttributes(lockFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException retired) {
            // owner 已持锁 unlink 并退役；本回收者锁住的是旧 inode（C7）。
            return Outcome.of(Kind.RETIRED);
        } catch (IOException | RuntimeException failure) {
            return Outcome.failed(failure);
        }
        // fileKey 只能在文件存续期间保证唯一（BasicFileAttributes#fileKey 文档）；协议中无人会在同一路径重建
        // 锁文件，故此处只用于识别“已被替换”的异常情况。
        if (!Objects.equals(before.fileKey(), after.fileKey())) {
            return Outcome.of(Kind.REPLACED);
        }
        PaimonSpillFiles.DeletionResult io = PaimonSpillFiles.deleteTreeIfPresent(
                lease.resolve(PaimonSpillLease.IO_DIR), deleteAction);
        if (!io.success) {
            String failedEntries = String.join("; ", io.failureSummaries());
            return Outcome.failed(new IOException("Cannot delete orphaned Paimon spill io under " + lease
                    + (failedEntries.isEmpty() ? "" : ": " + failedEntries)));
        }
        try {
            Files.deleteIfExists(lockFile);
        } catch (IOException | RuntimeException failure) {
            return Outcome.failed(failure);
        }
        return Outcome.reclaimed(io.bytesDeleted);
    }

    private void inspectionFailed(Path root, Path lease, String reason, Throwable failure, Result result) {
        result.failed++;
        audit.recordRateLimited(PaimonSpillAudit.Event.RECLAIM_INSPECTION_FAILED, root + "|" + reason,
                PaimonSpillAudit.details("root", root, "lease", lease, "reason", reason,
                        "directoryState", PaimonLocalDirectories.describe(root)), failure);
    }

    private void skip(Path root, Path lease, String reason, Result result) {
        result.skipped++;
        audit.recordRateLimited(PaimonSpillAudit.Event.RECLAIM_SKIPPED, root + "|" + reason,
                PaimonSpillAudit.details("root", root, "lease", lease, "reason", reason));
    }

    /** 旧平铺布局只列清单，不打开、不删除（INV-09）。 */
    private void reportLegacyLayout(Path root, Result result) {
        int legacy = 0;
        PaimonLegacySpillSize size = new PaimonLegacySpillSize();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(root, LEGACY_PREFIX + "*")) {
            for (Path entry : entries) {
                if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
                    legacy++;
                    size.inspect(entry);
                }
            }
        } catch (IOException | RuntimeException unreadable) {
            return;
        }
        if (legacy > 0) {
            result.legacyDirectories += legacy;
            result.legacyBytes += size.bytes;
            result.legacySizeIncomplete |= size.incomplete;
            audit.recordRateLimited(PaimonSpillAudit.Event.LEGACY_LAYOUT_FOUND, root.toString(),
                    PaimonSpillAudit.details("root", root, "legacyDirectories", legacy,
                            "observedBytes", size.bytes, "sizeIncomplete", size.incomplete));
        }
    }

    /** 先完整列出再处理，避免在遍历目录的同时修改它。列举失败不静默：该命名空间本次启动不可见。 */
    private List<Path> list(Path directory) {
        List<Path> entries = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                entries.add(entry);
            }
        } catch (IOException | RuntimeException unreadable) {
            audit.recordRateLimited(PaimonSpillAudit.Event.NAMESPACE_LIST_FAILED, directory.toString(),
                    PaimonSpillAudit.details("directory", directory), unreadable);
            return Collections.emptyList();
        }
        return entries;
    }

    private enum Kind { RECLAIMED, FAILED, LIVE, OVERLAPPING, RETIRED, REPLACED }

    private static final class Outcome {
        final Kind kind;
        final long bytes;
        final Throwable failure;

        private Outcome(Kind kind, long bytes, Throwable failure) {
            this.kind = kind;
            this.bytes = bytes;
            this.failure = failure;
        }

        static Outcome of(Kind kind) {
            return new Outcome(kind, 0L, null);
        }

        static Outcome reclaimed(long bytes) {
            return new Outcome(Kind.RECLAIMED, bytes, null);
        }

        static Outcome failed(Throwable failure) {
            return new Outcome(Kind.FAILED, 0L, failure);
        }
    }

    static final class Reclaimed {
        final String lease;
        final long bytes;

        Reclaimed(String lease, long bytes) {
            this.lease = lease;
            this.bytes = bytes;
        }
    }

    static final class Result {
        final List<Reclaimed> reclaimed = new ArrayList<>();
        int live;
        int skipped;
        int failed;
        int legacyDirectories;
        long legacyBytes;
        boolean legacySizeIncomplete;
        boolean budgetExhausted;
    }
}
