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
 * Startup stale-spill cleanup must cover every root the runtime can spill into: the global
 * {@code diskTmpDir} plus each per-table override from the table node config. Contexts and the
 * HASH_DYNAMIC preflight resolve their IOManager roots through the per-table value, so a crash
 * under an overridden root must also be reclaimed here.
 */
class PaimonServiceStaleSpillCleanupTest {

    @TempDir
    Path tempDir;

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
    void startupCleanupMustCoverPerTableDiskTmpDirRoots() throws Exception {
        File staleDir = Files.createDirectory(tempDir.resolve("paimon-io-stale")).toFile();
        File staleData = Files.write(staleDir.toPath().resolve("data.sst"), new byte[] {1})
                .toFile();
        File ownerFile = new File(
                tempDir.toFile(), ".paimon-io-stale" + ".tapdata-owner.lock");
        assertTrue(ownerFile.createNewFile());
        long old = System.currentTimeMillis() - 2 * PaimonSpillDirCleaner.DEFAULT_STALE_GRACE_MS;
        assertTrue(staleData.setLastModified(old));
        assertTrue(staleDir.setLastModified(old));

        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        // The global root points elsewhere, so only the per-table root knows about the stale
        // directory.
        config.setDiskTmpDir(tempDir.resolve("global-root-unused").toString());
        DataMap table = new DataMap();
        table.put("diskTmpDir", tempDir.toString());
        config.setTableConfig(Collections.singletonMap("orders", table));

        PaimonService service = new PaimonService(config, mock(Log.class));
        Method cleanup = PaimonService.class.getDeclaredMethod("cleanupStaleSpillDirs");
        cleanup.setAccessible(true);
        cleanup.invoke(service);

        assertFalse(staleDir.exists(), "stale dir under the per-table root must be removed");
        assertFalse(ownerFile.exists(), "owner marker must be removed with the stale dir");
    }
}
