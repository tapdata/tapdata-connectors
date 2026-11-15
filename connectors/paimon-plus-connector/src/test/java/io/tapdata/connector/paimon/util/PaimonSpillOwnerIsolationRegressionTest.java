package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.IOManagerImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AC-03 回归：与 PR #2 的现场复现用例（同 JVM 另一个 ClassLoader 或同一 ClassLoader 扫描 → 另一个 JVM 扫描）
 * 步骤完全相同，结论反转。
 *
 * <ul>
 *   <li>A：本测试 JVM 的应用 ClassLoader，经生产外观持有真实 Paimon IOManager 与租约锁；</li>
 *   <li>B：同一 JVM 的隔离 ClassLoader（模拟 PDK 重新加载 jar，新版本或未重启的旧版本）或同一 ClassLoader，
 *       运行启动清理；</li>
 *   <li>C：另一个 JVM，分别运行 develop 平铺清理器（旧版本引擎）与新回收器（新版本引擎），grace=0。</li>
 * </ul>
 */
@DisabledOnOs(OS.WINDOWS)
class PaimonSpillOwnerIsolationRegressionTest {

    @Test
    void foreignClassLoaderScanThenOtherJvmCleanupKeepsLiveLease(@TempDir Path root) throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult owner =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString(), "default.orders");
        try {
            Path lockFile = SpillTestSupport.ownerLockFile(owner.spillDirs().get(0));
            assertFalse(SpillProbeProcess.otherProcessAcquires(lockFile), "Provision 后 owner 锁必须有效");

            try (URLClassLoader loaderB = IsolatedConnectorLoader.create()) {
                // B 代表同一 JVM 中重新加载的新版本连接器……
                Class<?> foreignReclaimer = loaderB.loadClass(PaimonSpillReclaimer.class.getName());
                assertNotSame(PaimonSpillReclaimer.class, foreignReclaimer, "B 必须重新定义回收器类");
                assertEquals(0, reclaimIn(loaderB, root));
                // ……或升级后未重启、仍留在同一 JVM 中的旧版本连接器（AC-22）。
                Method legacy = loaderB.loadClass(LegacyFlatSpillCleanerFixture.class.getName())
                        .getMethod("cleanupStaleSpillDirs", String[].class, long.class, BiConsumer.class);
                legacy.setAccessible(true);
                assertEquals(0, legacy.invoke(null, new String[] {root.toString()}, 0L, null));
            }

            assertLeaseSurvivesOtherJvmCleanup(owner, lockFile, root);
        } finally {
            release(owner);
        }
    }

    @Test
    void sameClassLoaderScanDuringOwnershipKeepsLiveLease(@TempDir Path root) throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult owner =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString(), "default.orders");
        try {
            Path lockFile = SpillTestSupport.ownerLockFile(owner.spillDirs().get(0));
            assertEquals(0, SpillTestSupport.runStartupCleanup(owner.spillDirs(), null), "生产启动回收入口");
            assertEquals(0, LegacyFlatSpillCleanerFixture.cleanupStaleSpillDirs(new String[] {root.toString()}, 0L, null));

            assertLeaseSurvivesOtherJvmCleanup(owner, lockFile, root);
        } finally {
            release(owner);
        }
    }

    @Test
    void releasedLeaseLeavesNothingForOtherJvms(@TempDir Path root) throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult owner =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString(), "default.orders");
        Path lease = SpillTestSupport.ownerLockFile(owner.spillDirs().get(0)).getParent();

        release(owner);

        assertFalse(Files.exists(lease));
        assertEquals("reclaimed=0", SpillProbeProcess.reclaimInOtherJvm(root));
    }

    private static void assertLeaseSurvivesOtherJvmCleanup(PaimonSpillDirCleaner.IOManagerBuildResult owner,
            Path lockFile, Path root) throws Exception {
        assertFalse(SpillProbeProcess.otherProcessAcquires(lockFile), "同 JVM 扫描之后 owner 的 OS 锁仍然有效");
        assertEquals("deleted=0", SpillProbeProcess.legacyCleanupInOtherJvm(root), "旧版本引擎的平铺扫描看不到租约");
        assertEquals("reclaimed=0", SpillProbeProcess.reclaimInOtherJvm(root), "新版本引擎的回收者拿不到锁");

        File spill = ((IOManagerImpl) owner.ioManager()).getSpillingDirectories()[0];
        assertTrue(spill.isDirectory(), "活跃 spill 目录必须仍然存在");
        owner.ioManager().createBufferFileWriter(owner.ioManager().createChannel()).closeAndDelete();
    }

    private static int reclaimIn(URLClassLoader loader, Path root) throws Exception {
        Class<?> reclaimerType = loader.loadClass(PaimonSpillReclaimer.class.getName());
        Method standard = reclaimerType.getDeclaredMethod("standard");
        standard.setAccessible(true);
        Method reclaim = reclaimerType.getDeclaredMethod("reclaim", Collection.class);
        reclaim.setAccessible(true);
        Object result = reclaim.invoke(standard.invoke(null), Collections.singletonList(root.toString()));
        Field reclaimed = result.getClass().getDeclaredField("reclaimed");
        reclaimed.setAccessible(true);
        return ((List<?>) reclaimed.get(result)).size();
    }

    private static void release(PaimonSpillDirCleaner.IOManagerBuildResult owner) throws Exception {
        owner.ioManager().close();
        assertTrue(PaimonSpillDirCleaner.releaseAfterClose(owner.spillDirs(), true).complete());
    }
}
