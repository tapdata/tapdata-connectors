package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.FileIOChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T12：多个真实 JVM 在同一本机文件系统上验证租约协议（AC-06、AC-07、AC-09、AC-16、AC-17 跨进程部分）。
 * 这里只证明 OS 记录锁保护本地目录，不证明跨 Engine writer fencing，也不外推 NFS 锁语义。
 */
class PaimonSpillProcessIntegrationTest {

    private static final byte[] CHANNEL_CONTENT = new byte[] {11, 22, 33, 44};
    private static final FileTime OLD_TIME = FileTime.fromMillis(1L);

    @Test
    void otherJvmCleanerMustKeepLiveOwnerEvenWithZeroGrace(@TempDir Path tempDir)
            throws Exception {
        Path legacyData = createLegacyDirectoryWithoutMarker(tempDir);
        try (ChildOwner child = ChildOwner.start(tempDir)) {
            child.awaitReady();
            assertTrue(child.process.isAlive(), "握手完成时 owner 子 JVM 必须仍在运行");
            assertCrossProcessLockHeld(child.marker);
            assertDoesNotThrow(() -> UUID.fromString(child.spill.getFileName().toString()
                    .substring(PaimonSpillDirCleaner.SPILL_DIR_PREFIX.length())));

            // 父 JVM 没有子 JVM 的任何登记；跳过子 JVM 的租约只能依赖实际 OS 文件锁。
            PaimonSpillReclaimer.Result result = reclaim(tempDir);

            assertTrue(result.reclaimed.isEmpty());
            assertEquals(1, result.live);
            assertTrue(child.process.isAlive());
            assertTrue(Files.isDirectory(child.spill));
            assertTrue(Files.isRegularFile(child.marker));
            assertArrayEquals(CHANNEL_CONTENT, Files.readAllBytes(child.channel));
            assertArrayEquals(CHANNEL_CONTENT,
                    Files.readAllBytes(child.spill.resolve("rocksdb-test-index/data.sst")));
            assertTrue(Files.exists(legacyData), "旧平铺布局目录不由新协议删除");
            assertFalse(Files.exists(legacyMarker(legacyData.getParent())));

            child.closeGracefully();
            assertFalse(Files.exists(child.spill), "正常退出应由原生 IOManager.close 删除自己的目录");
            assertFalse(Files.exists(child.marker));
            assertTrue(Files.exists(legacyData));
        }
    }

    @Test
    void cleanerMustReclaimCrashedJvmResidueButKeepUnmarkedLegacyDirectory(@TempDir Path tempDir)
            throws Exception {
        Path legacyData = createLegacyDirectoryWithoutMarker(tempDir);
        try (ChildOwner child = ChildOwner.start(tempDir)) {
            child.awaitReady();
            assertCrossProcessLockHeld(child.marker);

            // 必须等待实际子进程死亡，不能以 destroyForcibly 已发出作为锁释放证明。
            child.process.destroyForcibly();
            assertTrue(child.process.waitFor(10L, TimeUnit.SECONDS), child::diagnostics);
            assertFalse(child.process.isAlive());
            assertNotEquals(0, child.process.exitValue(), "本用例必须实际模拟异常进程终止");
            assertTrue(Files.isDirectory(child.spill), "SIGKILL 不执行子 JVM 的 IO close finally");
            assertTrue(Files.isRegularFile(child.marker));
            assertArrayEquals(CHANNEL_CONTENT, Files.readAllBytes(child.channel));
            assertArrayEquals(CHANNEL_CONTENT,
                    Files.readAllBytes(child.spill.resolve("rocksdb-test-index/data.sst")));

            // 这次扫描可代表恢复后的 A（新 jvmId），也可代表能访问同一本地 root 的 B；不依赖 Engine 身份。
            PaimonSpillReclaimer.Result result = reclaim(tempDir);

            assertEquals(1, result.reclaimed.size());
            assertEquals(child.marker.getParent().getFileName().toString(),
                    java.nio.file.Paths.get(result.reclaimed.get(0).lease).getFileName().toString());
            assertEquals(2L * CHANNEL_CONTENT.length, result.reclaimed.get(0).bytes);
            assertFalse(Files.exists(child.spill));
            assertFalse(Files.exists(child.marker));
            assertTrue(Files.exists(legacyData), "旧平铺布局残留不由新协议删除");
            assertFalse(Files.exists(legacyMarker(legacyData.getParent())));
            assertEquals(0, reclaim(tempDir).reclaimed.size(), "重复恢复扫描应幂等");
        }
    }

    @Test
    void retainedOwnerAfterGcMustExcludeOtherJvmCleanerUntilActualProcessDeath(@TempDir Path tempDir) throws Exception {
        try (ChildOwner child = ChildOwner.start(tempDir, true)) {
            child.awaitReady();
            assertCrossProcessLockHeld(child.marker);
            assertEquals(0, reclaim(tempDir).reclaimed.size());
            assertTrue(Files.exists(child.spill));
            child.process.destroyForcibly();
            assertTrue(child.process.waitFor(10, TimeUnit.SECONDS)); assertFalse(child.process.isAlive());
            assertEquals(1, reclaim(tempDir).reclaimed.size());
            assertFalse(Files.exists(child.spill)); assertFalse(Files.exists(child.marker));
        }
    }

    private static Path createLegacyDirectoryWithoutMarker(Path root) throws IOException {
        Path directory = Files.createDirectory(root.resolve("paimon-io-legacy-without-marker"));
        Path data = Files.write(directory.resolve("legacy.channel"), new byte[] {5, 6});
        Files.setLastModifiedTime(data, OLD_TIME);
        Files.setLastModifiedTime(directory, OLD_TIME);
        return data;
    }

    /** AC-06：两个回收者进程同时处理同一 ORPHANED lease，恰好一个完成，全程不新建锁文件。 */
    @Test
    void twoReclaimerProcessesReclaimTheSameOrphanOnce(@TempDir Path tempDir) throws Exception {
        Path lease = orphanLease(tempDir);
        int total;
        try (SpillProbeProcess.Handle first = SpillProbeProcess.start(
                java.util.Collections.emptyList(), "reclaim", tempDir.toString());
             SpillProbeProcess.Handle second = SpillProbeProcess.start(
                     java.util.Collections.emptyList(), "reclaim", tempDir.toString())) {
            total = reclaimedCount(first.expectLine("reclaimed=")) + reclaimedCount(second.expectLine("reclaimed="));
            first.awaitSuccess();
            second.awaitSuccess();
        }
        assertEquals(1, total);
        assertFalse(Files.exists(lease));
        assertFalse(Files.exists(lease.resolve("owner.lock")), "回收者不得重建锁文件");
    }

    /** AC-09：同一引擎重启后的新 JVM（pid 与启动时间改变，jvmId 不同）回收上一实例被 SIGKILL 后的租约。 */
    @Test
    void restartedEngineReclaimsThePreviousInstanceLease(@TempDir Path tempDir) throws Exception {
        try (ChildOwner previous = ChildOwner.start(tempDir)) {
            previous.awaitReady();
            assertCrossProcessLockHeld(previous.marker);
            previous.process.destroyForcibly();
            assertTrue(previous.process.waitFor(10L, TimeUnit.SECONDS), previous::diagnostics);
            assertTrue(Files.isRegularFile(previous.marker), "SIGKILL 不执行退役");

            assertEquals("reclaimed=1", SpillProbeProcess.reclaimInOtherJvm(tempDir));
            assertFalse(Files.exists(previous.spill));
            assertFalse(Files.exists(previous.marker));
        }
    }

    /**
     * AC-16：回收者进程已打开锁文件但尚未加锁时，owner 按“持锁 unlink → 释放”退役；回收者随后取得旧 inode
     * 的锁，但路径复核失败，什么也不删除、也不重建锁文件。
     */
    @Test
    void lateReclaimerProcessCannotActAfterOwnerRetires(@TempDir Path tempDir) throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult owner =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(tempDir.toString(), "late.orders");
        Path lockFile = SpillTestSupport.ownerLockFile(owner.spillDirs().get(0));
        try (SpillProbeProcess.Handle late = SpillProbeProcess.start(
                java.util.Collections.emptyList(), "reclaim-paused", tempDir.toString())) {
            String opened = late.expectLine("opened=").substring("opened=".length());
            assertEquals(lockFile.getParent().getFileName(), Paths.get(opened).getParent().getFileName());

            owner.ioManager().close();
            assertTrue(PaimonSpillDirCleaner.releaseAfterClose(owner.spillDirs(), true).complete());
            late.send("go");

            assertEquals("reclaimed=0", late.expectLine("reclaimed="));
            late.awaitSuccess();
        }
        assertFalse(Files.exists(lockFile), "迟到的回收者不得重建锁文件");
        assertFalse(Files.exists(lockFile.getParent()));
    }

    /** AC-17（跨进程）：另一个进程在 owner 创建锁文件前删除了空 lease，Provision 换新 lease 名后成功。 */
    @Test
    void provisionSurvivesAnotherProcessRemovingItsEmptyLease(@TempDir Path tempDir) throws Exception {
        java.util.List<Path> raced = new java.util.ArrayList<>();
        java.util.List<String> outputs = new java.util.ArrayList<>();
        PaimonSpillLease.ProvisionHook otherProcess = new PaimonSpillLease.ProvisionHook() {
            @Override
            public void afterLeaseDirectory(Path lease) throws IOException {
                if (raced.isEmpty()) {
                    raced.add(lease);
                    try {
                        outputs.add(SpillProbeProcess.reclaimInOtherJvm(tempDir));
                    } catch (Exception failure) {
                        throw new IOException(failure);
                    }
                }
            }
        };
        PaimonSpillLease lease = PaimonSpillLease.provision(java.util.Collections.singletonList(tempDir.toString()),
                "race.orders", PaimonJvmIdentity::current, PaimonSpillAudit.defaultAudit(), otherProcess);
        try {
            assertEquals(java.util.Collections.singletonList("reclaimed=0"), outputs, "空 lease 只被 rmdir，不计为回收");
            assertFalse(Files.exists(raced.get(0)), "另一个进程删除了尚无锁文件的空 lease");
            assertNotEquals(raced.get(0), lease.roots().get(0).lease);
            assertFalse(SpillProbeProcess.otherProcessAcquires(lease.roots().get(0).lockFile),
                    "本进程持有的锁只能由其他进程探测（同进程探测会释放它，见 C1）");
        } finally {
            assertNull(lease.roots().get(0).retire());
        }
    }

    /** AC-17（跨进程）：owner 释放锁后、rmdir 前，另一个进程先删除了已清空的 lease，退役仍判定为完成。 */
    @Test
    void retireSurvivesAnotherProcessRemovingTheEmptiedLease(@TempDir Path tempDir) throws Exception {
        PaimonSpillLease lease = PaimonSpillLease.provision(java.util.Collections.singletonList(tempDir.toString()),
                "race.orders", PaimonJvmIdentity::current, PaimonSpillAudit.defaultAudit(), PaimonSpillLease.NO_HOOK);
        PaimonSpillLease.Root owned = lease.roots().get(0);
        java.util.List<String> observed = new java.util.ArrayList<>();
        PaimonSpillLease.RetireHook otherProcess = new PaimonSpillLease.RetireHook() {
            @Override
            public void afterRelease(Path leaseDir) {
                try {
                    observed.add(SpillProbeProcess.reclaimInOtherJvm(tempDir));
                    observed.add("lease-exists=" + Files.exists(leaseDir));
                } catch (Exception failure) {
                    observed.add("failure=" + failure);
                }
            }
        };

        assertNull(owned.retire(PaimonSpillFiles.FILES_DELETE, otherProcess));

        assertEquals(java.util.Arrays.asList("reclaimed=0", "lease-exists=false"), observed);
        assertFalse(Files.exists(owned.lease));
    }

    private static Path orphanLease(Path root) throws IOException {
        Path lease = Files.createDirectories(root.resolve("tapdata-paimon-v2")
                .resolve(PaimonJvmIdentity.derive(31L, 31L)).resolve("0123456789ab-" + UUID.randomUUID()));
        Files.write(lease.resolve("owner.lock"), new byte[0]);
        Files.write(Files.createDirectories(lease.resolve("io").resolve("paimon-io-x")).resolve("a.channel"),
                CHANNEL_CONTENT);
        return lease;
    }

    private static int reclaimedCount(String line) {
        return Integer.parseInt(line.substring("reclaimed=".length()));
    }

    private static PaimonSpillReclaimer.Result reclaim(Path root) {
        return PaimonSpillReclaimer.standard().reclaim(java.util.Collections.singletonList(root.toString()));
    }

    /** 旧平铺布局的旁置锁文件位置。 */
    private static Path legacyMarker(Path spill) {
        return spill.resolveSibling("." + spill.getFileName() + ".tapdata-owner.lock");
    }

    private static void assertCrossProcessLockHeld(Path marker) throws IOException {
        // 跨 JVM 锁冲突应返回 null；同 JVM OverlappingFileLockException 会让测试失败。
        try (FileChannel channel = FileChannel.open(marker, StandardOpenOption.WRITE);
             FileLock lock = channel.tryLock()) {
            assertNull(lock, "owner 子 JVM 必须实际持有 marker 的排他文件锁");
        }
    }

    private static final class ChildOwner implements AutoCloseable {
        private final Process process;
        private final Path ready;
        private final Path output;
        private Path spill;
        private Path channel;
        private Path marker;

        private ChildOwner(Process process, Path ready, Path output) {
            this.process = process;
            this.ready = ready;
            this.output = output;
        }

        private static ChildOwner start(Path root) throws IOException { return start(root, false); }
        private static ChildOwner start(Path root, boolean retained) throws IOException {
            Path ready = root.resolve("owner.ready");
            Path output = root.resolve("owner-process.log");
            String classpath = System.getProperty("surefire.test.class.path");
            if (classpath == null || classpath.trim().isEmpty()) {
                classpath = System.getProperty("java.class.path");
            }
            assertFalse(classpath == null || classpath.trim().isEmpty(), "必须使用完整测试 runtime classpath");
            Process process = new ProcessBuilder(
                    Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", classpath, SpillOwnerProcess.class.getName(),
                    root.toString(), ready.toString(), Boolean.toString(retained))
                    .redirectErrorStream(true)
                    .redirectOutput(output.toFile())
                    .start();
            return new ChildOwner(process, ready, output);
        }

        private void awaitReady() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
            while (!Files.exists(ready)) {
                assertTrue(process.isAlive(), this::diagnostics);
                assertTrue(System.nanoTime() < deadline, this::diagnostics);
                Thread.sleep(20L);
            }
            List<String> paths = Files.readAllLines(ready, StandardCharsets.UTF_8);
            assertEquals(2, paths.size(), "握手必须同时发布已经持锁的目录与写完的 channel");
            spill = Paths.get(paths.get(0));
            channel = Paths.get(paths.get(1));
            marker = SpillTestSupport.ownerLockFile(spill.toString());
            assertEquals(spill, channel.getParent());
            assertTrue(Files.isRegularFile(marker));
            assertTrue(Files.isRegularFile(channel));
        }

        private void closeGracefully() throws Exception {
            process.getOutputStream().write("CLOSE\n".getBytes(StandardCharsets.UTF_8));
            process.getOutputStream().flush();
            assertTrue(process.waitFor(10L, TimeUnit.SECONDS), this::diagnostics);
            assertEquals(0, process.exitValue(), this::diagnostics);
        }

        private String diagnostics() {
            try {
                return "owner 子 JVM 状态异常，alive=" + process.isAlive()
                        + "，握手=" + Files.exists(ready) + "，输出:\n"
                        + new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
            } catch (IOException failure) {
                return "读取 owner 子 JVM 输出失败: " + failure;
            }
        }

        @Override
        public void close() throws Exception {
            // 仅回收本 fixture 创建并持有句柄的子进程，不搜索或终止其他 Java 进程。
            if (process.isAlive()) {
                process.destroyForcibly();
            }
            try {
                assertTrue(process.waitFor(10L, TimeUnit.SECONDS), this::diagnostics);
                assertFalse(process.isAlive(), "测试 finally 结束前子 JVM 必须实际退出");
            } finally {
                process.getOutputStream().close();
                process.getInputStream().close();
                process.getErrorStream().close();
            }
        }
    }

    /** 子 JVM 的唯一入口；持有原生 IOManager 与 marker 锁直到收到明确关闭命令。 */
    public static final class SpillOwnerProcess {
        public static void main(String[] args) throws Exception {
            Path root = Paths.get(args[0]);
            Path ready = Paths.get(args[1]);
            PaimonSpillDirCleaner.IOManagerBuildResult built =
                    PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString(), "process-owner");
            try {
                if (built.spillDirs().size() != 1) {
                    throw new IllegalStateException("单一 root 必须只创建一个原生 Spill 目录");
                }
                // Paimon 1.3.2 原生 UUID 目录与 channel；仅 IOManager.close 才递归删除目录。
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java#L81
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java#L74
                Path spill = Paths.get(built.spillDirs().get(0));
                FileIOChannel.ID id = built.ioManager().createChannel();
                Path channel = id.getPathFile().getCanonicalFile().toPath();
                Files.write(channel, CHANNEL_CONTENT);
                // 模拟内核经 tempDirs() 创建的嵌套索引内容；真实 RocksDB 布局另由 Context 集成测试验证。
                String confinedRoot = PaimonSpillDirCleaner.withConfinedTempDirs(
                        built.ioManager(), built.spillDirs()).tempDirs()[0];
                Path indexDir = Files.createDirectory(Paths.get(confinedRoot).resolve("rocksdb-test-index"));
                Path index = Files.write(indexDir.resolve("data.sst"), CHANNEL_CONTENT);
                Files.setLastModifiedTime(index, OLD_TIME);
                Files.setLastModifiedTime(indexDir, OLD_TIME);
                Files.setLastModifiedTime(channel, OLD_TIME);
                Files.setLastModifiedTime(spill, OLD_TIME);
                if (args.length > 2 && Boolean.parseBoolean(args[2])) {
                    // 故障保留后丢弃原始引用并 GC，唯一受验证的资源根是静态 retained ledger。
                    java.lang.ref.WeakReference<org.apache.paimon.disk.IOManager> reference = retainAndForget(built);
                    built = null;
                    for (int attempt = 0; attempt < 3; attempt++) { System.gc(); Thread.sleep(20); }
                    if (reference.get() == null) { throw new AssertionError("retained IOManager lost after GC"); }
                }
                Path pending = ready.resolveSibling("owner.ready.pending");
                Files.write(pending, Arrays.asList(spill.toString(), channel.toString()), StandardCharsets.UTF_8);
                Files.move(pending, ready, StandardCopyOption.ATOMIC_MOVE);

                BufferedReader commands = new BufferedReader(
                        new InputStreamReader(System.in, StandardCharsets.UTF_8));
                if (!"CLOSE".equals(commands.readLine())) {
                    throw new IllegalStateException("owner 子 JVM 未收到明确的 CLOSE 命令");
                }
            } finally {
                if (built != null) {
                    built.ioManager().close();
                    PaimonSpillDirCleaner.releaseAfterClose(built.spillDirs(), true);
                }
            }
        }
        private static java.lang.ref.WeakReference<org.apache.paimon.disk.IOManager> retainAndForget(
                PaimonSpillDirCleaner.IOManagerBuildResult built) {
            io.tapdata.connector.paimon.service.PaimonStopController stop =
                    new io.tapdata.connector.paimon.service.PaimonStopController("child-retained", 1, 1, 1);
            io.tapdata.connector.paimon.service.PaimonStopResources ledger =
                    new io.tapdata.connector.paimon.service.PaimonStopResources();
            ledger.scope("retained IO", stop).reserve("IOManager").bind(built.ioManager());
            stop.attachResourceRoot(ledger); stop.start();
            stop.retain(new IOException("injected close proof failure"), ledger);
            if (!stop.isFinished() || !stop.isRetained()) { throw new AssertionError("retained terminal missing"); }
            return new java.lang.ref.WeakReference<>(built.ioManager());
        }

    }
}
