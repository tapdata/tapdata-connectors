package io.tapdata.connector.paimon.util;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Spill 租约的 owner 侧协议（SPEC-paimon-spill-owner-isolation §4.1、§4.3、§4.4）。
 *
 * <pre>
 * &lt;root&gt;/tapdata-paimon-v2/&lt;jvmId&gt;/&lt;tag&gt;-&lt;uuid&gt;/owner.lock   存活证明：owner 进程持有的 POSIX 记录锁
 *                                                  /io/          传给 Paimon IOManager.create 的根
 * </pre>
 *
 * <p>owner 只打开自己以 {@code O_CREAT|O_EXCL} 创建的锁文件（{@link StandardOpenOption#CREATE_NEW}），且只经
 * 该唯一 channel 加锁与释放：JDK {@code FileLock} 文档指出“某些系统上关闭 channel 会释放 JVM 在该文件上的
 * 全部锁”，并建议同一程序只用唯一 channel 加锁。
 * https://docs.oracle.com/javase/8/docs/api/java/nio/channels/FileLock.html
 */
final class PaimonSpillLease {

    static final String SUBROOT = "tapdata-paimon-v2";
    static final String LOCK_FILE = "owner.lock";
    static final String IO_DIR = "io";
    static final int MAX_PROVISION_ATTEMPTS = 3;
    static final Pattern LEASE_NAME = Pattern.compile(
            "[0-9a-f]{12}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

    /** 仅供测试在 Provision 的竞态窗口插入外部动作；生产使用 {@link #NO_HOOK}。 */
    interface ProvisionHook {
        default void beforeLeaseDirectory(Path namespace) throws IOException {
        }

        default void afterLeaseDirectory(Path lease) throws IOException {
        }

        default void afterOwnerLock(Path lease) throws IOException {
        }
    }

    /** 仅供测试在 Retire 释放锁之后、rmdir 之前插入外部动作。 */
    interface RetireHook {
        default void afterRelease(Path lease) throws IOException {
        }
    }

    static final ProvisionHook NO_HOOK = new ProvisionHook() { };
    static final RetireHook NO_RETIRE_HOOK = new RetireHook() { };

    private final String ownerLabel;
    private final String tag;
    private final List<Root> roots;

    private PaimonSpillLease(String ownerLabel, String tag, List<Root> roots) {
        this.ownerLabel = ownerLabel;
        this.tag = tag;
        this.roots = Collections.unmodifiableList(roots);
    }

    static PaimonSpillLease provision(List<String> configuredRoots, String ownerLabel) throws IOException {
        return provision(configuredRoots, ownerLabel, PaimonJvmIdentity::current,
                PaimonSpillAudit.defaultAudit(), NO_HOOK);
    }

    /**
     * Provision（Spec §4.4）。jvmId 获取失败时在创建任何协议目录之前抛出。任一根失败时，已就绪的根尚未交给
     * IOManager、没有异步使用者，立即退役后抛出。
     */
    static PaimonSpillLease provision(List<String> configuredRoots, String ownerLabel, Supplier<String> jvmId,
            PaimonSpillAudit audit, ProvisionHook hook) throws IOException {
        List<Path> roots;
        String namespace;
        try {
            roots = normalizeRoots(configuredRoots);
            namespace = jvmId.get();
            if (!PaimonJvmIdentity.isNamespaceName(namespace)) {
                throw new IllegalStateException("Invalid Paimon spill namespace identity");
            }
        } catch (IOException | RuntimeException failure) {
            audit.record(PaimonSpillAudit.Event.PROVISION_FAILED, PaimonSpillAudit.details(
                    "owner", ownerLabel, "stage", "normalize-roots-or-identity", "roots", configuredRoots,
                    "directoryState", configuredRoots == null ? null
                            : PaimonLocalDirectories.describeRoots(configuredRoots)), failure);
            throw failure;
        }
        String label = ownerLabel == null ? "unlabeled" : ownerLabel;
        String tag = tag(label);
        List<Root> provisioned = new ArrayList<>(roots.size());
        try {
            for (Path root : roots) {
                provisioned.add(provisionRoot(root, namespace, tag, hook, audit));
            }
        } catch (IOException | RuntimeException | Error failure) {
            audit.record(PaimonSpillAudit.Event.PROVISION_ROLLED_BACK, PaimonSpillAudit.details(
                    "roots", roots, "rolledBack", provisioned.size(), "tag", tag, "owner", label,
                    "directoryState", roots.stream().map(PaimonLocalDirectories::describe)
                            .collect(java.util.stream.Collectors.toList())), failure);
            for (Root ready : provisioned) {
                Throwable rollback = ready.retire(PaimonSpillFiles.FILES_DELETE, NO_RETIRE_HOOK);
                if (rollback != null) {
                    failure.addSuppressed(rollback);
                }
            }
            throw failure;
        }
        PaimonSpillLease lease = new PaimonSpillLease(label, tag, provisioned);
        for (Root root : provisioned) {
            audit.record(PaimonSpillAudit.Event.LEASE_CREATED, PaimonSpillAudit.details(
                    "root", root.root, "lease", root.lease.getFileName(), "tag", tag, "owner", label));
        }
        return lease;
    }

    private static Root provisionRoot(Path root, String namespace, String tag, ProvisionHook hook,
            PaimonSpillAudit audit) throws IOException {
        Path subroot = root.resolve(SUBROOT);
        Path namespaceDir = subroot.resolve(namespace);
        NoSuchFileException lastRace = null;
        for (int attempt = 1; attempt <= MAX_PROVISION_ATTEMPTS; attempt++) {
            Path lease = namespaceDir.resolve(tag + "-" + UUID.randomUUID());
            FileChannel channel;
            try {
                Files.createDirectories(namespaceDir);
                requireRealDirectory(subroot);
                requireRealDirectory(namespaceDir);
                hook.beforeLeaseDirectory(namespaceDir);
                Files.createDirectory(lease);
                hook.afterLeaseDirectory(lease);
                channel = FileChannel.open(lease.resolve(LOCK_FILE),
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            } catch (NoSuchFileException removedByReclaimer) {
                // 其他参与者只会 rmdir 空的子根、命名空间或 lease（外部回收者，或本进程刚退役最后一个
                // lease 的 owner，Spec §4.3）；此时尚无任何资源，换新 leaseId 从头重试。
                audit.record(PaimonSpillAudit.Event.PROVISION_RETRY, PaimonSpillAudit.details(
                        "root", root, "attempt", attempt, "reason", "enoent-race"));
                lastRace = removedByReclaimer;
                continue;
            }
            Root provisioned = null;
            try {
                FileLock lock = channel.tryLock();
                if (lock == null) {
                    IOException contended = new IOException(
                            "Fresh Paimon spill owner lock is held by another process: " + lease);
                    audit.record(PaimonSpillAudit.Event.PROVISION_LOCK_HELD, PaimonSpillAudit.details(
                            "lease", lease), contended);
                    throw contended;
                }
                provisioned = new Root(root, lease, channel, lock, audit);
                hook.afterOwnerLock(lease);
                Files.createDirectory(provisioned.io);
                return provisioned;
            } catch (IOException | RuntimeException | Error failure) {
                if (provisioned == null) {
                    closeQuietly(channel);
                } else {
                    Throwable rollback = provisioned.retire(PaimonSpillFiles.FILES_DELETE, NO_RETIRE_HOOK);
                    if (rollback != null) {
                        failure.addSuppressed(rollback);
                    }
                }
                throw failure;
            }
        }
        audit.record(PaimonSpillAudit.Event.PROVISION_EXHAUSTED, PaimonSpillAudit.details(
                "root", root, "attempts", MAX_PROVISION_ATTEMPTS), lastRace);
        throw new IOException("Cannot provision a Paimon spill lease under " + root + " after "
                + MAX_PROVISION_ATTEMPTS + " attempts", lastRace);
    }

    /** 创建并规范化根目录，按真实路径去重且保持原顺序（Spec D3；Paimon 自身不去重，见 C9）。 */
    static List<Path> normalizeRoots(List<String> configuredRoots) throws IOException {
        if (configuredRoots == null || configuredRoots.isEmpty()) {
            throw new IllegalArgumentException("Paimon spill roots must not be empty");
        }
        Set<Path> unique = new LinkedHashSet<>();
        for (String configured : configuredRoots) {
            Path root = Paths.get(configured);
            if (!root.isAbsolute()) {
                throw new IllegalArgumentException("Paimon spill root must be absolute");
            }
            try {
                Files.createDirectories(root);
            } catch (java.nio.file.FileAlreadyExistsException exists) {
                // 并发创建或目录已存在但作为符号链接，toRealPath 会解析真实路径去重
            }
            unique.add(root.toRealPath());
        }
        return new ArrayList<>(unique);
    }

    static String tag(String ownerLabel) {
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256").digest(ownerLabel.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
        StringBuilder tag = new StringBuilder(12);
        for (int i = 0; i < 6; i++) {
            tag.append(Character.forDigit((digest[i] >> 4) & 0xF, 16)).append(Character.forDigit(digest[i] & 0xF, 16));
        }
        return tag.toString();
    }

    private static void requireRealDirectory(Path directory) throws IOException {
        if (Files.isSymbolicLink(directory) || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Paimon spill protocol directory must be a real directory: " + directory);
        }
    }

    private static void closeQuietly(FileChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {
            // 新建且未加锁的 channel；失败不影响任何既有锁。
        }
    }

    String ownerLabel() {
        return ownerLabel;
    }

    String tag() {
        return tag;
    }

    List<Root> roots() {
        return roots;
    }

    /** 传给 {@code IOManager.create(String[])} 的根目录，顺序与 {@link #roots()} 一致。 */
    String[] ioRoots() {
        String[] ioRoots = new String[roots.size()];
        for (int i = 0; i < ioRoots.length; i++) {
            ioRoots[i] = roots.get(i).io.toString();
        }
        return ioRoots;
    }

    /** 一个根目录下的 lease；锁对象由本实例强引用，直到 Retire 成功（INV-02）。 */
    static final class Root {
        final Path root;
        final Path lease;
        final Path lockFile;
        final Path io;
        private final FileChannel channel;
        private final FileLock lock;
        private final PaimonSpillAudit audit;
        private boolean retired;

        private Root(Path root, Path lease, FileChannel channel, FileLock lock, PaimonSpillAudit audit) {
            this.root = root;
            this.lease = lease;
            this.lockFile = lease.resolve(LOCK_FILE);
            this.io = lease.resolve(IO_DIR);
            this.channel = channel;
            this.lock = lock;
            this.audit = audit;
        }

        Throwable retire() {
            return retire(PaimonSpillFiles.FILES_DELETE, NO_RETIRE_HOOK);
        }

        /**
         * Retire（Spec §4.4）：删除 io → 持锁 unlink 锁文件 → 释放锁 → rmdir lease。只在 IOManager 关闭成功
         * 之后调用。释放锁之前任一步失败都保留锁并返回失败；释放之后的 rmdir 只尽力而为，ENOENT 视为完成。
         *
         * @return null 表示已完成；否则为失败原因（锁仍被持有或状态未知，调用方必须保留）。
         */
        synchronized Throwable retire(PaimonSpillFiles.DeleteAction deleteAction, RetireHook hook) {
            if (retired) {
                return null;
            }
            PaimonSpillFiles.DeletionResult ioDeletion = PaimonSpillFiles.deleteTreeIfPresent(io, deleteAction);
            if (!ioDeletion.success) {
                IOException failure = new IOException("Cannot delete Paimon spill io directory " + io);
                audit.record(PaimonSpillAudit.Event.LEASE_RETAINED,
                        PaimonSpillAudit.details("lease", lease, "reason", "io-deletion-failed",
                                "failedEntries", ioDeletion.failureSummaries()), failure);
                return failure;
            }
            try {
                // 持锁 unlink：已打开旧 inode 的迟到回收者随后即使取得锁，也会在路径复核时退出（C7）。
                Files.deleteIfExists(lockFile);
            } catch (IOException | RuntimeException failure) {
                audit.record(PaimonSpillAudit.Event.LEASE_RETAINED,
                        PaimonSpillAudit.details("lease", lease, "reason", "unlink-failed"), failure);
                return failure;
            }
            Throwable releaseFailure = null;
            try {
                lock.release();
            } catch (IOException | RuntimeException failure) {
                releaseFailure = failure;
            }
            try {
                channel.close();
            } catch (IOException | RuntimeException failure) {
                releaseFailure = PaimonFailures.append(releaseFailure, failure);
            }
            if (releaseFailure != null) {
                audit.record(PaimonSpillAudit.Event.LEASE_RETAINED,
                        PaimonSpillAudit.details("lease", lease, "reason", "release-failed"), releaseFailure);
                return releaseFailure;
            }
            retired = true;
            try {
                hook.afterRelease(lease);
            } catch (IOException | RuntimeException ignored) {
                // 仅测试钩子。
            }
            // 锁已释放、目录已空：外部回收者可能已先 rmdir（PROVISIONING → ABSENT），ENOENT 视为完成。
            if (PaimonSpillFiles.removeEmptyDirectory(lease) == PaimonSpillFiles.EmptyDirectoryRemoval.KEPT) {
                audit.record(PaimonSpillAudit.Event.LEASE_RESIDUE,
                        PaimonSpillAudit.details("lease", lease, "reason", "lease-directory-not-removed"));
            } else {
                // 正常停止不留空目录：命名空间与子根为空时一并移除（rmdir 只删空目录）；本进程并发中的
                // Provision 会因此得到 ENOENT 并重试（Spec §4.4）。
                Path namespace = lease.getParent();
                if (PaimonSpillFiles.removeEmptyDirectory(namespace) == PaimonSpillFiles.EmptyDirectoryRemoval.REMOVED) {
                    PaimonSpillFiles.removeEmptyDirectory(namespace.getParent());
                }
                audit.record(PaimonSpillAudit.Event.LEASE_RETIRED, PaimonSpillAudit.details(
                        "lease", lease, "bytes", ioDeletion.bytesDeleted));
            }
            return null;
        }

        /** JDK 视角的锁状态；不能证明 OS 锁仍被持有（C1），仅用于诊断。 */
        boolean lockValidInJvm() {
            return lock.isValid();
        }
    }
}
