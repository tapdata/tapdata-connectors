package io.tapdata.connector.paimon.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** T09：回收者协议（Spec §4.5–4.6；AC-02、AC-05、AC-07、AC-08、AC-18、AC-19 的单进程部分）。 */
@DisabledOnOs(OS.WINDOWS)
class PaimonSpillReclaimerTest {

    private static final String SELF = PaimonJvmIdentity.derive(1001L, 1700000000000L);
    private static final String FOREIGN = PaimonJvmIdentity.derive(2002L, 1700000000000L);
    private static final FileTime ANCIENT = FileTime.fromMillis(1L);

    private final List<Path> opened = new CopyOnWriteArrayList<>();
    private final List<PaimonSpillAudit.Event> events = new CopyOnWriteArrayList<>();
    private final PaimonSpillAudit audit = new PaimonSpillAudit(
            (event, fields, failure) -> events.add(event), System::nanoTime, name -> null);

    @Test
    void reclaimsOrphanedForeignLeaseAndItsEmptyNamespace(@TempDir Path root) throws Exception {
        Path lease = lease(root, FOREIGN, new byte[] {1, 2, 3});

        PaimonSpillReclaimer.Result result = reclaimer().reclaim(roots(root));

        assertEquals(1, result.reclaimed.size());
        assertEquals(3L, result.reclaimed.get(0).bytes);
        assertFalse(Files.exists(lease));
        assertFalse(Files.exists(lease.getParent()), "空的外部命名空间随之移除");
        assertTrue(events.contains(PaimonSpillAudit.Event.LEASE_RECLAIMED));
    }

    /** AC-02（同 ClassLoader）：本进程命名空间按名称跳过，其中任何文件都不会被打开。 */
    @Test
    void ownNamespaceIsNeverOpenedEvenWhenItsLockIsFree(@TempDir Path root) throws Exception {
        Path own = lease(root, SELF, new byte[] {9});
        Path foreign = lease(root, FOREIGN, new byte[] {1});

        PaimonSpillReclaimer.Result result = reclaimer().reclaim(roots(root));

        assertEquals(1, result.reclaimed.size());
        assertTrue(Files.isRegularFile(own.resolve("owner.lock")));
        assertTrue(Files.isDirectory(own.resolve("io")));
        assertFalse(Files.exists(foreign));
        for (Path path : opened) {
            assertFalse(path.startsWith(own.getParent()), "打开了本进程命名空间中的文件: " + path);
        }
    }

    /** AC-07：外部进程持锁的活跃 lease，即使所有 mtime 都极早，也不删除。 */
    @Test
    void liveForeignLeaseWithAncientMtimesIsKept(@TempDir Path root) throws Exception {
        Path lease = lease(root, FOREIGN, new byte[] {7});
        try (SpillProbeProcess.Handle owner = SpillProbeProcess.start(
                Collections.emptyList(), "hold-lock", lease.resolve("owner.lock").toString())) {
            owner.expectLine("locked");
            ageTree(lease);

            PaimonSpillReclaimer.Result result = reclaimer().reclaim(roots(root));

            assertEquals(0, result.reclaimed.size());
            assertEquals(1, result.live);
            assertTrue(Files.isDirectory(lease.resolve("io")));
            owner.send("release");
            owner.awaitSuccess();
        }
        assertEquals(1, reclaimer().reclaim(roots(root)).reclaimed.size(), "owner 退出后可回收");
    }

    @Test
    void leaseWithoutLockFileIsOnlyRemovedWhenEmpty(@TempDir Path root) throws Exception {
        Path empty = Files.createDirectories(namespace(root, FOREIGN).resolve(leaseName()));
        Path withData = Files.createDirectories(namespace(root, FOREIGN).resolve(leaseName()).resolve("io"));

        PaimonSpillReclaimer.Result result = reclaimer().reclaim(roots(root));

        assertEquals(0, result.reclaimed.size());
        assertFalse(Files.exists(empty));
        assertTrue(Files.isDirectory(withData), "无锁文件的非空 lease 不具备删除证明");
        assertTrue(opened.isEmpty());
    }

    /** AC-08：名称不合规、符号链接、未知子根版本一律保守处理。 */
    @Test
    void malformedNamesSymbolicLinksAndUnknownVersionsAreIgnored(@TempDir Path root) throws Exception {
        Path badNamespace = Files.createDirectories(root.resolve("tapdata-paimon-v2").resolve("jNOT-A-NAMESPACE"));
        Files.createDirectories(badNamespace.resolve(leaseName()).resolve("io"));
        Files.write(badNamespace.resolve(leaseName() + "-x"), new byte[0]);
        Path badLease = Files.createDirectories(namespace(root, FOREIGN).resolve("not-a-lease"));
        Files.write(badLease.resolve("owner.lock"), new byte[0]);
        Path unknownVersion = Files.createDirectories(root.resolve("tapdata-paimon-v3").resolve(FOREIGN)
                .resolve(leaseName()).resolve("io"));
        Path target = Files.createDirectories(root.resolve("outside"));
        Files.write(target.resolve("keep.channel"), new byte[] {1});
        boolean linked = link(namespace(root, FOREIGN).resolve(leaseName()), target);
        Path linkedLock = lease(root, FOREIGN, new byte[] {1});
        Files.delete(linkedLock.resolve("owner.lock"));
        linked &= link(linkedLock.resolve("owner.lock"), target.resolve("keep.channel"));

        PaimonSpillReclaimer.Result result = reclaimer().reclaim(roots(root));

        assertEquals(0, result.reclaimed.size());
        assertTrue(Files.isDirectory(badLease));
        assertTrue(Files.isDirectory(unknownVersion));
        assertTrue(Files.isRegularFile(target.resolve("keep.channel")), "符号链接目标不得被删除");
        if (linked) {
            assertTrue(Files.isDirectory(linkedLock.resolve("io")));
        }
        assertTrue(opened.isEmpty(), "不合规的条目不得被打开: " + opened);
    }

    /** AC-19：白名单外的文件系统类型、未知类型或探针异常时整体关闭回收，不打开任何文件，告警限流。 */
    @Test
    void unsupportedFileSystemsDisableReclaim(@TempDir Path root) throws Exception {
        Path lease = lease(root, FOREIGN, new byte[] {1});
        for (PaimonSpillReclaimer.FileStoreTypeProbe probe : new PaimonSpillReclaimer.FileStoreTypeProbe[] {
                path -> "nfs4", path -> "fuse.sshfs", path -> "CEPH", path -> null,
                path -> { throw new IOException("statfs failed"); }}) {
            PaimonSpillReclaimer reclaimer = new PaimonSpillReclaimer(() -> SELF, probe, countingOpener(),
                    PaimonSpillFiles.FILES_DELETE, audit, PaimonSpillReclaimer.DEFAULT_BUDGET);
            assertEquals(0, reclaimer.reclaim(roots(root)).reclaimed.size());
        }
        assertTrue(Files.isDirectory(lease));
        assertTrue(opened.isEmpty());
        assertEquals(1, events.stream().filter(e -> e == PaimonSpillAudit.Event.ADMISSION_DENIED).count(),
                "同一根目录的准入告警限流");
        assertTrue(PaimonSpillReclaimer.SUPPORTED_FILE_STORE_TYPES.contains(
                Files.getFileStore(root).type().toLowerCase(java.util.Locale.ROOT)),
                "本机临时目录类型必须在白名单内，否则其余用例不成立");
    }

    /** AC-18：io 删除失败时保留锁文件，供后续调用重试。 */
    @Test
    void ioDeletionFailureKeepsLockFileForRetry(@TempDir Path root) throws Exception {
        Path lease = lease(root, FOREIGN, new byte[] {1});
        PaimonSpillReclaimer failing = new PaimonSpillReclaimer(() -> SELF, path -> "apfs", countingOpener(),
                path -> {
                    if (path.getFileName().toString().endsWith(".channel")) {
                        throw new IOException("injected");
                    }
                    Files.delete(path);
                }, audit, PaimonSpillReclaimer.DEFAULT_BUDGET);

        PaimonSpillReclaimer.Result result = failing.reclaim(roots(root));

        assertEquals(1, result.failed);
        assertTrue(Files.isRegularFile(lease.resolve("owner.lock")));
        assertTrue(events.contains(PaimonSpillAudit.Event.RECLAIM_FAILED));
        assertEquals(1, reclaimer().reclaim(roots(root)).reclaimed.size());
    }

    @Test
    void budgetBoundsReclaimsPerInvocation(@TempDir Path root) throws Exception {
        for (int i = 0; i < 3; i++) {
            lease(root, FOREIGN, new byte[] {1});
        }
        PaimonSpillReclaimer bounded = new PaimonSpillReclaimer(() -> SELF, path -> "apfs", countingOpener(),
                PaimonSpillFiles.FILES_DELETE, audit, 2);

        PaimonSpillReclaimer.Result first = bounded.reclaim(roots(root));
        PaimonSpillReclaimer.Result second = bounded.reclaim(roots(root));

        assertEquals(2, first.reclaimed.size());
        assertTrue(first.budgetExhausted);
        assertTrue(events.contains(PaimonSpillAudit.Event.RECLAIM_BUDGET_EXHAUSTED),
                "预算耗尽必须留下 warn 审计");
        assertEquals(1, second.reclaimed.size());
        assertFalse(second.budgetExhausted);
    }

    /** 列举失败的命名空间不再静默消失：留下限流审计，剩余回收继续。 */
    @Test
    void unreadableNamespaceIsReportedNotSilentlyIgnored(@TempDir Path root) throws Exception {
        assumeTrue(!"root".equals(System.getProperty("user.name")), "root 不受目录权限约束");
        Path lease = lease(root, FOREIGN, new byte[] {1});
        Path namespace = lease.getParent();
        Files.setPosixFilePermissions(namespace, java.nio.file.attribute.PosixFilePermissions.fromString("---------"));
        try {
            PaimonSpillReclaimer.Result result = reclaimer().reclaim(roots(root));

            assertTrue(events.contains(PaimonSpillAudit.Event.NAMESPACE_LIST_FAILED));
            assertEquals(0, result.reclaimed.size());
        } finally {
            Files.setPosixFilePermissions(namespace, java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    /** AC-05（线程）：同一 JVM 两个回收者并发处理同一 ORPHANED lease，无异常逃逸、至多一次成功。 */
    @Test
    void concurrentReclaimersInOneJvmReclaimAtMostOnce(@TempDir Path root) throws Exception {
        Path lease = lease(root, FOREIGN, new byte[] {1, 2});
        CyclicBarrier bothOpened = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<PaimonSpillReclaimer.Result>> results = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                results.add(pool.submit(() -> {
                    bothOpened.await(10, TimeUnit.SECONDS);
                    return new PaimonSpillReclaimer(() -> SELF, path -> "apfs",
                            countingOpener(), PaimonSpillFiles.FILES_DELETE, audit, 64).reclaim(roots(root));
                }));
            }
            int reclaimed = 0;
            for (Future<PaimonSpillReclaimer.Result> result : results) {
                reclaimed += result.get(30, TimeUnit.SECONDS).reclaimed.size();
            }
            assertEquals(1, reclaimed);
            assertFalse(Files.exists(lease));
        } finally {
            pool.shutdownNow();
        }
    }

    /** 持锁后复核：锁文件已被替换为新 inode 时跳过，不删除任何内容。 */
    @Test
    void replacedLockFileIsDetectedByFileKey(@TempDir Path root) throws Exception {
        Path lease = lease(root, FOREIGN, new byte[] {1});
        Path lockFile = lease.resolve("owner.lock");
        PaimonSpillReclaimer.LockFileOpener replacing = path -> {
            FileChannel stale = PaimonSpillReclaimer.DEFAULT_OPENER.open(path);
            Files.delete(path);
            Files.write(path, new byte[0]);
            return stale;
        };

        PaimonSpillReclaimer.Result result = new PaimonSpillReclaimer(() -> SELF, p -> "apfs", replacing,
                PaimonSpillFiles.FILES_DELETE, audit, 64).reclaim(roots(root));

        assertEquals(0, result.reclaimed.size());
        assertEquals(1, result.skipped);
        assertTrue(Files.isRegularFile(lockFile));
        assertTrue(Files.isDirectory(lease.resolve("io")));
    }

    /** 持锁后复核：owner 已 unlink 锁文件（退役中），迟到回收者锁住旧 inode 后退出（C7）。 */
    @Test
    void lockFileRetiredBeforeLockingIsSkipped(@TempDir Path root) throws Exception {
        Path lease = lease(root, FOREIGN, new byte[] {1});
        PaimonSpillReclaimer.LockFileOpener ownerRetiresMeanwhile = path -> {
            FileChannel stale = PaimonSpillReclaimer.DEFAULT_OPENER.open(path);
            Files.delete(path);
            return stale;
        };

        PaimonSpillReclaimer.Result result = new PaimonSpillReclaimer(() -> SELF, p -> "apfs", ownerRetiresMeanwhile,
                PaimonSpillFiles.FILES_DELETE, audit, 64).reclaim(roots(root));

        assertEquals(0, result.reclaimed.size());
        assertTrue(Files.isDirectory(lease.resolve("io")), "迟到回收者不得删除 owner 的 io");
    }

    @Test
    void identityFailureDisablesReclaimEntirely(@TempDir Path root) throws Exception {
        Path lease = lease(root, FOREIGN, new byte[] {1});
        PaimonSpillReclaimer reclaimer = new PaimonSpillReclaimer(
                () -> { throw new IllegalStateException("pid unavailable"); }, p -> "apfs", countingOpener(),
                PaimonSpillFiles.FILES_DELETE, audit, 64);

        assertEquals(0, reclaimer.reclaim(roots(root)).reclaimed.size());
        assertTrue(Files.isDirectory(lease));
        assertTrue(opened.isEmpty());
    }

    /** AC-22（部分）：旧平铺布局只报告，不打开锁文件、不删除。 */
    @Test
    void legacyFlatLayoutIsReportedButNeverTouched(@TempDir Path root) throws Exception {
        Path legacy = Files.createDirectories(root.resolve("paimon-io-" + UUID.randomUUID()));
        Files.write(legacy.resolve("a.channel"), new byte[] {1});
        Path legacyLock = Files.write(root.resolve("." + legacy.getFileName() + ".tapdata-owner.lock"), new byte[0]);
        Path bareRocksdb = Files.write(Files.createDirectories(root.resolve("rocksdb-orphan")).resolve("index.sst"),
                new byte[] {1});
        Path rocksdbMarker = Files.write(root.resolve(".rocksdb-orphan.tapdata-owner.lock"), new byte[0]);

        PaimonSpillReclaimer.Result result = reclaimer().reclaim(roots(root));

        assertEquals(1, result.legacyDirectories);
        assertTrue(Files.isDirectory(legacy));
        assertTrue(Files.isRegularFile(legacyLock));
        assertTrue(Files.exists(bareRocksdb), "根目录下的裸 rocksdb 目录不属于任何租约");
        assertTrue(Files.exists(rocksdbMarker));
        assertTrue(opened.isEmpty());
        assertTrue(events.contains(PaimonSpillAudit.Event.LEGACY_LAYOUT_FOUND));
    }

    /** INV-03：回收者源码不读取任何时间信息。 */
    @Test
    void reclaimerHasNoTemporalInput() throws Exception {
        String source = new String(Files.readAllBytes(Paths.get(
                "src/main/java/io/tapdata/connector/paimon/util/PaimonSpillReclaimer.java")), StandardCharsets.UTF_8);
        for (String forbidden : new String[] {
                "lastModified", "LastModifiedTime", "currentTimeMillis", "nanoTime", "Instant", "Clock", "grace"}) {
            assertFalse(source.contains(forbidden), "回收判定不得依赖时间: " + forbidden);
        }
    }

    private PaimonSpillReclaimer reclaimer() {
        return new PaimonSpillReclaimer(() -> SELF, PaimonSpillReclaimer.DEFAULT_TYPE_PROBE, countingOpener(),
                PaimonSpillFiles.FILES_DELETE, audit, PaimonSpillReclaimer.DEFAULT_BUDGET);
    }

    private PaimonSpillReclaimer.LockFileOpener countingOpener() {
        return lockFile -> {
            opened.add(lockFile);
            return PaimonSpillReclaimer.DEFAULT_OPENER.open(lockFile);
        };
    }

    private static List<String> roots(Path root) {
        return Collections.singletonList(root.toString());
    }

    private static Path namespace(Path root, String jvmId) {
        return root.resolve("tapdata-paimon-v2").resolve(jvmId);
    }

    private static String leaseName() {
        return "0123456789ab-" + UUID.randomUUID();
    }

    /** 按协议布局构造一个锁未被持有的 lease（等同于 owner 进程已终止）。 */
    private static Path lease(Path root, String jvmId, byte[] channelBytes) throws IOException {
        Path lease = Files.createDirectories(namespace(root, jvmId).resolve(leaseName()));
        Files.write(lease.resolve("owner.lock"), new byte[0]);
        Path spill = Files.createDirectories(lease.resolve("io").resolve("paimon-io-" + UUID.randomUUID()));
        Files.write(spill.resolve("a.channel"), channelBytes);
        return lease;
    }

    private static void ageTree(Path lease) throws IOException {
        try (java.util.stream.Stream<Path> paths = Files.walk(lease)) {
            for (Path path : (Iterable<Path>) paths::iterator) {
                Files.setLastModifiedTime(path, ANCIENT);
            }
        }
    }

    private static boolean link(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
            return true;
        } catch (IOException | UnsupportedOperationException unsupported) {
            return false;
        }
    }
}
