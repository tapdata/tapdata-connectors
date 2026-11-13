package io.tapdata.connector.paimon.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** T06：owner 侧 Provision / Retire（Spec §4.3–4.4；AC-17 单进程部分、AC-18 owner 部分、AC-20）。 */
@DisabledOnOs(OS.WINDOWS)
class PaimonSpillLeaseTest {

    private static final AtomicInteger TEST_SEQUENCE = new AtomicInteger(0);

    // 为每个测试实例生成唯一的 JVM ID，避免 CI 环境中的文件冲突
    private final String jvmId = PaimonJvmIdentity.derive(
            4242L + TEST_SEQUENCE.getAndIncrement(),
            1700000000000L + System.nanoTime());
    private final Supplier<String> identity = () -> jvmId;

    private final List<PaimonSpillAudit.Event> events = new ArrayList<>();
    private final PaimonSpillAudit audit = new PaimonSpillAudit(
            (event, fields, failure) -> events.add(event), System::nanoTime, name -> null);

    @Test
    void provisionCreatesProtocolLayoutAndHoldsOwnerLock(@TempDir Path root) throws Exception {
        PaimonSpillLease lease = provision(Collections.singletonList(root.toString()), PaimonSpillLease.NO_HOOK);
        PaimonSpillLease.Root owned = lease.roots().get(0);

        Path namespace = root.toRealPath().resolve("tapdata-paimon-v2").resolve(jvmId);
        assertEquals(namespace, owned.lease.getParent());
        assertTrue(PaimonSpillLease.LEASE_NAME.matcher(owned.lease.getFileName().toString()).matches());
        assertTrue(owned.lease.getFileName().toString().startsWith(PaimonSpillLease.tag("default.orders") + "-"));
        assertFalse(owned.lease.toString().contains("orders"), "lease 路径不得包含表名");
        assertTrue(Files.isRegularFile(owned.lockFile));
        assertTrue(Files.isDirectory(owned.io));
        assertArrayEquals(new String[] {owned.io.toString()}, lease.ioRoots());
        assertFalse(SpillProbeProcess.otherProcessAcquires(owned.lockFile), "owner 进程必须持有 OS 记录锁");
        assertEquals(Collections.singletonList(PaimonSpillAudit.Event.LEASE_CREATED), events);

        assertNull(owned.retire());
    }

    @Test
    void duplicateAndAliasedRootsAreProvisionedOnce(@TempDir Path base) throws Exception {
        Path root = Files.createDirectory(base.resolve("spill"));
        Path alias = base.resolve("alias");
        boolean linked = createSymbolicLink(alias, root);
        List<String> configured = new ArrayList<>(Arrays.asList(root.toString(), root.toString()));
        if (linked) {
            configured.add(alias.toString());
        }

        PaimonSpillLease lease = provision(configured, PaimonSpillLease.NO_HOOK);

        assertEquals(1, lease.roots().size(), "重复或互为别名的根只建一个 lease（AC-20）");
        assertEquals(1, leasesUnder(root).size());
        assertNull(lease.roots().get(0).retire());
    }

    /** AC-17：外部回收者在 owner 创建锁文件之前 rmdir 了空 lease，owner 换新 leaseId 重试成功。 */
    @Test
    void provisionRetriesWhenReclaimerRemovesEmptyLease(@TempDir Path root) throws Exception {
        List<Path> removed = new ArrayList<>();
        PaimonSpillLease.ProvisionHook reclaimerRace = new PaimonSpillLease.ProvisionHook() {
            @Override
            public void afterLeaseDirectory(Path lease) throws IOException {
                if (removed.isEmpty()) {
                    Files.delete(lease);
                    removed.add(lease);
                }
            }
        };

        PaimonSpillLease lease = provision(Collections.singletonList(root.toString()), reclaimerRace);

        Path owned = lease.roots().get(0).lease;
        assertNotEquals(removed.get(0), owned, "重试必须使用新的 leaseId");
        assertFalse(Files.exists(removed.get(0)));
        assertTrue(Files.isRegularFile(owned.resolve("owner.lock")));
        assertTrue(events.contains(PaimonSpillAudit.Event.PROVISION_RETRY), "ENOENT 竞态重试必须留下 info 审计");
        assertNull(lease.roots().get(0).retire());
    }

    /** AC-17：外部回收者 rmdir 了空的命名空间目录，owner 重建后重试成功。 */
    @Test
    void provisionRetriesWhenReclaimerRemovesEmptyNamespace(@TempDir Path root) throws Exception {
        AtomicInteger calls = new AtomicInteger();
        PaimonSpillLease.ProvisionHook reclaimerRace = new PaimonSpillLease.ProvisionHook() {
            @Override
            public void beforeLeaseDirectory(Path namespace) throws IOException {
                if (calls.getAndIncrement() == 0) {
                    Files.delete(namespace);
                }
            }
        };

        PaimonSpillLease lease = provision(Collections.singletonList(root.toString()), reclaimerRace);

        assertEquals(2, calls.get());
        assertTrue(Files.isDirectory(lease.roots().get(0).io));
        assertNull(lease.roots().get(0).retire());
    }

    @Test
    void provisionGivesUpAfterBoundedRetriesWithoutResidue(@TempDir Path root) throws Exception {
        PaimonSpillLease.ProvisionHook alwaysRemoved = new PaimonSpillLease.ProvisionHook() {
            @Override
            public void afterLeaseDirectory(Path lease) throws IOException {
                Files.delete(lease);
            }
        };

        IOException failure = assertThrows(IOException.class,
                () -> provision(Collections.singletonList(root.toString()), alwaysRemoved));

        assertTrue(failure.getMessage().contains("after 3 attempts"), failure.getMessage());
        assertTrue(events.contains(PaimonSpillAudit.Event.PROVISION_EXHAUSTED), "重试耗尽必须留下 error 审计");
        assertTrue(leasesUnder(root).isEmpty());
    }

    @Test
    void symbolicLinkNamespaceIsRejected(@TempDir Path base) throws Exception {
        Path root = Files.createDirectory(base.resolve("spill"));
        Path elsewhere = Files.createDirectory(base.resolve("elsewhere"));
        Files.createDirectories(root.resolve("tapdata-paimon-v2"));
        assumeTrue(createSymbolicLink(root.resolve("tapdata-paimon-v2").resolve(jvmId), elsewhere));

        assertThrows(IOException.class, () -> provision(Collections.singletonList(root.toString()), PaimonSpillLease.NO_HOOK));
        try (Stream<Path> entries = Files.list(elsewhere)) {
            assertEquals(0L, entries.count(), "不得在符号链接目标中创建 lease");
        }
    }

    /** 多根部分失败：已就绪的根尚无使用者，立即退役，不残留锁或目录。 */
    @Test
    void partialMultiRootFailureRetiresProvisionedRoots(@TempDir Path base) throws Exception {
        Path good = Files.createDirectory(base.resolve("good"));
        Path bad = Files.createDirectory(base.resolve("bad"));
        Files.write(bad.resolve("tapdata-paimon-v2"), new byte[0]);

        assertThrows(IOException.class,
                () -> provision(Arrays.asList(good.toString(), bad.toString()), PaimonSpillLease.NO_HOOK));

        assertTrue(events.contains(PaimonSpillAudit.Event.PROVISION_ROLLED_BACK), "部分失败回滚必须留下 warn 审计");
        assertTrue(leasesUnder(good).isEmpty(), "已就绪的根必须被退役");
    }

    @Test
    void identityFailureCreatesNoProtocolDirectory(@TempDir Path root) {
        assertThrows(IllegalStateException.class, () -> PaimonSpillLease.provision(
                Collections.singletonList(root.toString()), "default.orders",
                () -> { throw new IllegalStateException("pid unavailable"); }, audit, PaimonSpillLease.NO_HOOK));
        assertFalse(Files.exists(root.resolve("tapdata-paimon-v2")));
    }

    @Test
    void retireDeletesIoUnlinksLockReleasesThenRemovesLease(@TempDir Path root) throws Exception {
        PaimonSpillLease.Root owned = provision(Collections.singletonList(root.toString()), PaimonSpillLease.NO_HOOK)
                .roots().get(0);
        Files.write(Files.createDirectories(owned.io.resolve("paimon-io-x")).resolve("a.channel"), new byte[] {1, 2});
        List<String> order = new ArrayList<>();
        PaimonSpillLease.RetireHook observer = new PaimonSpillLease.RetireHook() {
            @Override
            public void afterRelease(Path lease) {
                order.add("released: io=" + Files.exists(owned.io) + " lock=" + Files.exists(owned.lockFile)
                        + " jvmLock=" + owned.lockValidInJvm());
            }
        };

        assertNull(owned.retire(PaimonSpillFiles.FILES_DELETE, observer));

        assertEquals(Collections.singletonList("released: io=false lock=false jvmLock=false"), order);
        assertFalse(Files.exists(owned.lease));
        assertTrue(events.contains(PaimonSpillAudit.Event.LEASE_RETIRED));
        assertNull(owned.retire(), "重复退役是幂等的");
    }

    /** AC-17：锁释放后外部回收者先 rmdir 了空 lease，owner 的 rmdir 得到 ENOENT 仍视为完成。 */
    @Test
    void retireTreatsLeaseRemovedByReclaimerAsComplete(@TempDir Path root) throws Exception {
        PaimonSpillLease.Root owned = provision(Collections.singletonList(root.toString()), PaimonSpillLease.NO_HOOK)
                .roots().get(0);
        PaimonSpillLease.RetireHook reclaimerRace = new PaimonSpillLease.RetireHook() {
            @Override
            public void afterRelease(Path lease) throws IOException {
                Files.delete(lease);
            }
        };

        assertNull(owned.retire(PaimonSpillFiles.FILES_DELETE, reclaimerRace));
        assertFalse(Files.exists(owned.lease));
        assertFalse(events.contains(PaimonSpillAudit.Event.LEASE_RESIDUE));
    }

    /** AC-18：io 删除失败时保留锁文件与锁，修复后可再次退役。 */
    @Test
    void ioDeletionFailureRetainsOwnerLock(@TempDir Path root) throws Exception {
        PaimonSpillLease.Root owned = provision(Collections.singletonList(root.toString()), PaimonSpillLease.NO_HOOK)
                .roots().get(0);
        Path channel = Files.write(Files.createDirectories(owned.io.resolve("paimon-io-x")).resolve("a.channel"),
                new byte[] {1});
        PaimonSpillFiles.DeleteAction failingOnChannel = path -> {
            if (path.equals(channel)) {
                throw new IOException("injected deletion failure");
            }
            Files.delete(path);
        };

        Throwable failure = owned.retire(failingOnChannel, PaimonSpillLease.NO_RETIRE_HOOK);

        assertNotNull(failure);
        assertTrue(Files.isRegularFile(owned.lockFile));
        assertTrue(owned.lockValidInJvm());
        assertFalse(SpillProbeProcess.otherProcessAcquires(owned.lockFile), "失败时 OS 锁必须仍被持有");
        assertTrue(events.contains(PaimonSpillAudit.Event.LEASE_RETAINED));
        assertNull(owned.retire());
        assertFalse(Files.exists(owned.lease));
    }

    /** 最后一个 lease 退役后不留空目录；仍有其他 lease 时命名空间保留。 */
    @Test
    void retireRemovesEmptyNamespaceAndSubrootOnlyWhenNoLeaseRemains(@TempDir Path root) throws Exception {
        PaimonSpillLease first = provision(Collections.singletonList(root.toString()), PaimonSpillLease.NO_HOOK);
        PaimonSpillLease second = provision(Collections.singletonList(root.toString()), PaimonSpillLease.NO_HOOK);
        Path namespace = first.roots().get(0).lease.getParent();

        assertNull(first.roots().get(0).retire());
        assertTrue(Files.isDirectory(namespace), "仍有其他 lease 时命名空间必须保留");

        assertNull(second.roots().get(0).retire());
        assertFalse(Files.exists(namespace));
        assertFalse(Files.exists(root.toRealPath().resolve("tapdata-paimon-v2")), "正常停止后不留空子根");
    }

    /** 子根被另一个参与者删除（例如本进程刚退役最后一个 lease）时，Provision 重建后成功。 */
    @Test
    void provisionRetriesWhenSubrootIsRemovedConcurrently(@TempDir Path root) throws Exception {
        AtomicInteger calls = new AtomicInteger();
        PaimonSpillLease.ProvisionHook ownerRetiredMeanwhile = new PaimonSpillLease.ProvisionHook() {
            @Override
            public void beforeLeaseDirectory(Path namespace) throws IOException {
                if (calls.getAndIncrement() == 0) {
                    Files.delete(namespace);
                    Files.delete(namespace.getParent());
                }
            }
        };

        PaimonSpillLease lease = provision(Collections.singletonList(root.toString()), ownerRetiredMeanwhile);

        assertEquals(2, calls.get());
        assertTrue(Files.isRegularFile(lease.roots().get(0).lockFile));
        assertNull(lease.roots().get(0).retire());
    }

    @Test
    void tagIsAStableDigestOfTheOwnerLabel() {
        assertEquals(PaimonSpillLease.tag("default.orders"), PaimonSpillLease.tag("default.orders"));
        assertNotEquals(PaimonSpillLease.tag("default.orders"), PaimonSpillLease.tag("preflight:default.orders"));
        assertTrue(PaimonSpillLease.tag("default.orders").matches("[0-9a-f]{12}"));
    }

    private PaimonSpillLease provision(List<String> roots, PaimonSpillLease.ProvisionHook hook) throws IOException {
        return PaimonSpillLease.provision(roots, "default.orders", identity, audit, hook);
    }

    private List<Path> leasesUnder(Path root) throws IOException {
        Path namespace = root.toRealPath().resolve("tapdata-paimon-v2").resolve(jvmId);
        if (!Files.isDirectory(namespace)) {
            return Collections.emptyList();
        }
        try (Stream<Path> entries = Files.list(namespace)) {
            return entries.collect(Collectors.toList());
        }
    }

    private static boolean createSymbolicLink(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
            return true;
        } catch (IOException | UnsupportedOperationException unsupported) {
            return false;
        }
    }
}
