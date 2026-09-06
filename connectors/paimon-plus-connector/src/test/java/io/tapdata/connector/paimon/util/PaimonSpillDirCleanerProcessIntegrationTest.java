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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * 两个真实 JVM 在同一本机文件系统上验证 Spill 删除互斥与进程异常退出后的回收。
 * 这里只证明 advisory FileLock 保护目录，不证明跨 Engine writer fencing，也不外推 NFS 锁语义。
 */
class PaimonSpillDirCleanerProcessIntegrationTest {

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

            Map<String, Long> deleted = new LinkedHashMap<>();
            // 父 JVM 从未 registerLiveDirs；跳过子 JVM 的目录必须依赖实际 OS 文件锁。
            assertEquals(0, PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                    new String[] {tempDir.toString()}, 0L, deleted::put));

            assertTrue(deleted.isEmpty());
            assertTrue(child.process.isAlive());
            assertTrue(Files.isDirectory(child.spill));
            assertTrue(Files.isRegularFile(child.marker));
            assertArrayEquals(CHANNEL_CONTENT, Files.readAllBytes(child.channel));
            assertArrayEquals(CHANNEL_CONTENT,
                    Files.readAllBytes(child.spill.resolve("rocksdb-test-index/data.sst")));
            assertTrue(Files.exists(legacyData), "无 marker 的旧版本目录不能按年龄猜测为可删除");
            assertFalse(Files.exists(ownerMarker(legacyData.getParent())));

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

            Map<String, Long> deleted = new LinkedHashMap<>();
            // 这次扫描可代表恢复后的 A，也可代表能访问同一本地 root 的 B；不依赖 Engine 身份。
            assertEquals(1, PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                    new String[] {tempDir.toString()}, 0L, deleted::put));

            assertEquals(1, deleted.size());
            assertEquals(Long.valueOf(2L * CHANNEL_CONTENT.length), deleted.get(child.spill.toString()));
            assertFalse(Files.exists(child.spill));
            assertFalse(Files.exists(child.marker));
            assertTrue(Files.exists(legacyData), "无 marker 残留仍不具备自动删除证明");
            assertFalse(Files.exists(ownerMarker(legacyData.getParent())));
            assertEquals(0, PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                    new String[] {tempDir.toString()}, 0L, null), "重复恢复扫描应幂等");
        }
    }

    private static Path createLegacyDirectoryWithoutMarker(Path root) throws IOException {
        Path directory = Files.createDirectory(root.resolve("paimon-io-legacy-without-marker"));
        Path data = Files.write(directory.resolve("legacy.channel"), new byte[] {5, 6});
        Files.setLastModifiedTime(data, OLD_TIME);
        Files.setLastModifiedTime(directory, OLD_TIME);
        return data;
    }

    private static Path ownerMarker(Path spill) {
        return spill.resolveSibling("." + spill.getFileName()
                + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
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

        private static ChildOwner start(Path root) throws IOException {
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
                    root.toString(), ready.toString())
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
            marker = ownerMarker(spill);
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
                    PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString());
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
                Path pending = ready.resolveSibling("owner.ready.pending");
                Files.write(pending, Arrays.asList(spill.toString(), channel.toString()), StandardCharsets.UTF_8);
                Files.move(pending, ready, StandardCopyOption.ATOMIC_MOVE);

                BufferedReader commands = new BufferedReader(
                        new InputStreamReader(System.in, StandardCharsets.UTF_8));
                if (!"CLOSE".equals(commands.readLine())) {
                    throw new IllegalStateException("owner 子 JVM 未收到明确的 CLOSE 命令");
                }
            } finally {
                built.ioManager().close();
                PaimonSpillDirCleaner.releaseAfterClose(built.spillDirs(), true);
            }
        }
    }
}
