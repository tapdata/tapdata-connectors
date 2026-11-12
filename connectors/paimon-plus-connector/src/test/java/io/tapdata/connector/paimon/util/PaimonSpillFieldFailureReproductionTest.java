package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用 develop e3f68298 的现有实现端到端复现 2026-11 现场 ENOENT（defect characterization）。
 *
 * <ul>
 *   <li>A：本测试 JVM 的应用 ClassLoader，经现有 {@code resolveAndCreateIOManager} 持有真实 Paimon
 *       IOManager 与 owner 锁；</li>
 *   <li>B：同一 JVM 内的隔离 ClassLoader（模拟 PDK 重新加载 jar）或同一 ClassLoader 的注册窗口，
 *       运行现有清理器；</li>
 *   <li>C：另一个 JVM，运行现有清理器。grace=0 只替代“目录空闲超过 10 分钟”。</li>
 * </ul>
 *
 * <p>SPEC V0.3 实现后（Plan T07），本类改写为同样步骤下“C 删除数为 0、A 建 channel 成功”的回归测试。
 */
@DisabledOnOs(OS.WINDOWS)
class PaimonSpillFieldFailureReproductionTest {

    /** 跨 ClassLoader：B 的 LIVE_DIRS 看不到 A 的目录，探测后 A 丢锁，C 删除目录，A 的下一次 spill 失败。 */
    @Test
    void foreignClassLoaderProbeThenOtherJvmCleanupReproducesFieldEnoent(@TempDir Path root) throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult owner =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString());
        try {
            File spill = new File(owner.spillDirs().get(0));
            Path marker = ownerMarker(spill);
            assertFalse(SpillProbeProcess.otherProcessAcquires(marker), "注册后 owner 锁必须有效");

            try (URLClassLoader loaderB = IsolatedConnectorLoader.create()) {
                Class<?> foreignCleaner = loaderB.loadClass(PaimonSpillDirCleaner.class.getName());
                assertNotSame(PaimonSpillDirCleaner.class, foreignCleaner, "B 必须重新定义清理器类");
                Object deletedByB = foreignCleaner
                        .getMethod("cleanupStaleSpillDirs", String[].class, long.class, BiConsumer.class)
                        .invoke(null, new String[] {root.toString()}, PaimonSpillDirCleaner.DEFAULT_STALE_GRACE_MS, null);
                assertEquals(0, deletedByB, "同 JVM 的扫描本身不删除目录");
            }

            assertLockLostThenDeletedByOtherJvm(owner.ioManager(), spill, marker, root);
        } finally {
            closeOwner(owner);
        }
    }

    /**
     * 同一 ClassLoader：现有 registerPaths 先取锁（PaimonSpillDirCleaner:269）后登记 LIVE_DIRS（:284）。
     * 这里把状态精确置于该窗口（持锁、未登记）后运行同一 ClassLoader 的清理器，结果与跨 ClassLoader 相同。
     */
    @Test
    void sameClassLoaderRegistrationWindowAlsoLosesOwnerLock(@TempDir Path root) throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult owner =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString());
        try {
            String path = owner.spillDirs().get(0);
            File spill = new File(path);
            Path marker = ownerMarker(spill);
            Set<String> liveDirs = liveDirs();
            assertTrue(liveDirs.remove(path), "目录必须已登记");
            try {
                assertEquals(0, PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {root.toString()}, PaimonSpillDirCleaner.DEFAULT_STALE_GRACE_MS, null));
            } finally {
                liveDirs.add(path);
            }

            assertLockLostThenDeletedByOtherJvm(owner.ioManager(), spill, marker, root);
        } finally {
            closeOwner(owner);
        }
    }

    /** 对照组：没有同 JVM 探测时，另一个 JVM 即使 grace=0 也拿不到锁，目录与后续 spill 均正常。 */
    @Test
    void withoutSameJvmProbeOtherJvmCleanupKeepsLiveSpillDirectory(@TempDir Path root) throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult owner =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString());
        try {
            File spill = new File(owner.spillDirs().get(0));
            assertEquals("deleted=0", SpillProbeProcess.legacyCleanupInOtherJvm(root));
            assertTrue(spill.isDirectory());
            assertFalse(SpillProbeProcess.otherProcessAcquires(ownerMarker(spill)));
            owner.ioManager().createBufferFileWriter(owner.ioManager().createChannel()).closeAndDelete();
        } finally {
            closeOwner(owner);
        }
    }

    private static void assertLockLostThenDeletedByOtherJvm(IOManager ioManager, File spill, Path marker, Path root)
            throws Exception {
        assertTrue(SpillProbeProcess.otherProcessAcquires(marker), "探测关闭后 owner 的 OS 锁已丢失");
        assertEquals("deleted=1", SpillProbeProcess.legacyCleanupInOtherJvm(root));
        assertFalse(spill.exists(), "另一个 JVM 删除了仍被 A 使用的 spill 目录");

        IOException failure = assertThrows(IOException.class,
                () -> ioManager.createBufferFileWriter(ioManager.createChannel()));
        // channel 路径沿用 IOManager 自己的（未规范化）目录对象，与 owner 登记的规范路径指向同一目录。
        File ioManagerSpill = ((IOManagerImpl) ioManager).getSpillingDirectories()[0];
        assertEquals(spill.getCanonicalFile(), ioManagerSpill.getCanonicalFile());
        PaimonSpillPlatformContractTest.assertFieldSignature(failure, ioManagerSpill);
    }

    private static Path ownerMarker(File spill) {
        return Paths.get(spill.getParent(), "." + spill.getName() + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
    }

    @SuppressWarnings("unchecked")
    private static Set<String> liveDirs() throws ReflectiveOperationException {
        Field field = PaimonSpillDirCleaner.class.getDeclaredField("LIVE_DIRS");
        field.setAccessible(true);
        return (Set<String>) field.get(null);
    }

    private static void closeOwner(PaimonSpillDirCleaner.IOManagerBuildResult owner) throws Exception {
        owner.ioManager().close();
        PaimonSpillDirCleaner.releaseAfterClose(owner.spillDirs(), true);
        for (String path : owner.spillDirs()) {
            Files.deleteIfExists(ownerMarker(new File(path)));
        }
    }
}
