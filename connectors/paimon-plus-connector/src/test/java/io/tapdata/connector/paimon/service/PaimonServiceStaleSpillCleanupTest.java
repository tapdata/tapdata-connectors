package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * 启动回收必须覆盖运行时可能使用的全部根：全局 {@code diskTmpDir} 与表节点配置中的每个表级覆盖。
 * 写上下文与 HASH_DYNAMIC 预检按表级值解析 IOManager 根，进程在覆盖根下崩溃留下的租约也必须在此回收。
 */
class PaimonServiceStaleSpillCleanupTest {

    @TempDir
    Path tempDir;

    @Test
    void s3BufferMustUseTheSameNormalizedAbsoluteRoots() throws Exception {
        Path first = tempDir.resolve("s3 first");
        Path second = tempDir.resolve("s3 second");
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("s3");
        config.setDiskTmpDir(" " + first + File.pathSeparator + second + " ");
        Method build = PaimonService.class.getDeclaredMethod("buildHadoopConfiguration");
        build.setAccessible(true);
        org.apache.hadoop.conf.Configuration hadoop = (org.apache.hadoop.conf.Configuration)
                build.invoke(new PaimonService(config, mock(Log.class)));
        org.junit.jupiter.api.Assertions.assertEquals(first + "," + second, hadoop.get("fs.s3a.buffer.dir"));
        assertTrue(Files.isDirectory(first));
        assertTrue(Files.isDirectory(second));
    }

    @Test
    void s3BufferMustValidateAllRootsBeforeCreatingAny() throws Exception {
        Path first = tempDir.resolve("not-created");
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("s3");
        config.setDiskTmpDir(first + ", 'bucket' = '-1'");
        Method build = PaimonService.class.getDeclaredMethod("buildHadoopConfiguration");
        build.setAccessible(true);
        java.lang.reflect.InvocationTargetException failure = org.junit.jupiter.api.Assertions.assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> build.invoke(new PaimonService(config, mock(Log.class))));
        assertTrue(failure.getCause() instanceof IllegalArgumentException);
        assertFalse(Files.exists(first));
    }

    @Test
    void invalidTableRootMustFailBeforeCatalogOrGlobalRootCreation() {
        PaimonConfig config = new PaimonConfig();
        Path warehouse = tempDir.resolve("warehouse-not-created");
        Path root = tempDir.resolve("spill-not-created");
        config.setWarehouse(warehouse.toString());
        config.setDiskTmpDir(root.toString());
        config.setTableConfig(Collections.singletonMap("orders",
                DataMap.create().kv("diskTmpDir", "'bucket' = '-1', 'write-buffer-size' = '128mb'")));
        PaimonService service = new PaimonService(config, mock(Log.class));
        IllegalArgumentException failure = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, service::init);
        assertTrue(failure.getMessage().contains("orders"));
        assertFalse(Files.exists(warehouse));
        assertFalse(Files.exists(root));
    }

    @Test
    void canonicalAliasesMustDeduplicateWithoutFollowingSymlinkRoots() throws Exception {
        PaimonConfig config = new PaimonConfig(); config.setDatabase("default");
        PaimonService service = new PaimonService(config, mock(Log.class));
        Method collect = PaimonService.class.getDeclaredMethod("collectTmpDirRoots", String.class, java.util.Set.class);
        collect.setAccessible(true);
        java.util.Set<String> roots = new java.util.LinkedHashSet<>();
        Path child = Files.createDirectory(tempDir.resolve("child"));
        collect.invoke(service, tempDir.toString(), roots);
        collect.invoke(service, child.resolve("..").toString(), roots);
        org.junit.jupiter.api.Assertions.assertEquals(Collections.singleton(tempDir.toFile().getCanonicalPath()), roots);
        Path link = Files.createSymbolicLink(tempDir.resolve("linked-root"), child);
        collect.invoke(service, link.toString(), roots);
        org.junit.jupiter.api.Assertions.assertEquals(1, roots.size(), "不能通过 canonicalize 绕过 symlink 拒绝策略");
    }

    @Test
    void startupReclaimMustCoverPerTableDiskTmpDirRoots() throws Exception {
        // 另一个已终止 JVM 遗留的租约（锁文件未被持有），只位于表级根下。
        Path lease = Files.createDirectories(tempDir.resolve("tapdata-paimon-v2")
                .resolve("j0123456789abcdef0123456789abcdef").resolve("0123456789ab-" + java.util.UUID.randomUUID()));
        Files.write(lease.resolve("owner.lock"), new byte[0]);
        Files.write(Files.createDirectories(lease.resolve("io").resolve("paimon-io-orphan")).resolve("data.sst"),
                new byte[] {1});
        // 旧平铺布局残留：新版本不打开、不删除（INV-09）。
        Path legacy = Files.createDirectory(tempDir.resolve("paimon-io-stale"));
        File legacyMarker = new File(tempDir.toFile(), ".paimon-io-stale.tapdata-owner.lock");
        assertTrue(legacyMarker.createNewFile());

        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        // 全局根指向别处，只有表级根知道这个租约。
        config.setDiskTmpDir(tempDir.resolve("global-root-unused").toString());
        DataMap table = new DataMap();
        table.put("diskTmpDir", tempDir.toString());
        config.setTableConfig(Collections.singletonMap("orders", table));

        PaimonService service = new PaimonService(config, mock(Log.class));
        Method reclaim = PaimonService.class.getDeclaredMethod("reclaimOrphanedSpillLeases");
        reclaim.setAccessible(true);
        reclaim.invoke(service);

        assertFalse(Files.exists(lease), "表级根下的 ORPHANED 租约必须被回收");
        assertTrue(Files.isDirectory(legacy), "旧平铺目录不得被删除");
        assertTrue(legacyMarker.exists(), "旧平铺锁文件不得被打开或删除");
    }
}
