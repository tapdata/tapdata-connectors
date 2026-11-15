package io.tapdata.connector.paimon.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AC-22：新旧版本连接器在同一根目录（同机多引擎或升级未重启）并存。旧版本由 develop e3f68298 平铺扫描的
 * 冻结副本代表。
 */
@DisabledOnOs(OS.WINDOWS)
class PaimonSpillLegacyCoexistenceTest {

    /** 旧版本扫描只列出根目录下的 paimon-io-*，即使新协议的租约已无 owner（锁空闲）也不会进入子根。 */
    @Test
    void legacyScannerNeverEntersTheVersionedSubroot(@TempDir Path root) throws Exception {
        Path orphan = Files.createDirectories(root.resolve("tapdata-paimon-v2")
                .resolve(PaimonJvmIdentity.derive(5L, 5L)).resolve("0123456789ab-" + UUID.randomUUID()));
        Files.write(orphan.resolve("owner.lock"), new byte[0]);
        Path channel = Files.write(Files.createDirectories(orphan.resolve("io").resolve("paimon-io-x"))
                .resolve("a.channel"), new byte[] {1});

        assertEquals(0, LegacyFlatSpillCleanerFixture.cleanupStaleSpillDirs(new String[] {root.toString()}, 0L, null));

        assertTrue(Files.exists(channel), "回退到旧版本时，旧扫描不得删除新协议目录");
    }

    /** 新版本与旧版本同时扫描：旧目录只由旧版本按旧规则处理，新租约只由新版本按锁处理。 */
    @Test
    void bothGenerationsScanTheSameRootWithoutCrossingLayouts(@TempDir Path root) throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult live =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString(), "new.orders");
        Path legacyLive = Files.createDirectory(root.resolve("paimon-io-" + UUID.randomUUID()));
        Path legacyMarker = Files.write(root.resolve("." + legacyLive.getFileName() + ".tapdata-owner.lock"),
                new byte[0]);
        try (SpillProbeProcess.Handle legacyOwner = SpillProbeProcess.start(
                Collections.emptyList(), "hold-lock", legacyMarker.toString())) {
            legacyOwner.expectLine("locked");

            PaimonSpillReclaimer.Result newScan =
                    PaimonSpillReclaimer.standard().reclaim(Collections.singletonList(root.toString()));
            int legacyScan = LegacyFlatSpillCleanerFixture.cleanupStaleSpillDirs(new String[] {root.toString()}, 0L, null);

            assertEquals(0, newScan.reclaimed.size());
            assertEquals(1, newScan.legacyDirectories);
            assertEquals(0, legacyScan, "旧 owner 进程持有旧锁");
            assertTrue(Files.isDirectory(legacyLive));
            assertTrue(SpillTestSupport.ownerLockHeld(live.spillDirs().get(0)), "新租约锁未被旧扫描触碰");
            legacyOwner.send("release");
            legacyOwner.awaitSuccess();
        } finally {
            live.ioManager().close();
            assertTrue(PaimonSpillDirCleaner.releaseAfterClose(live.spillDirs(), true).complete());
        }
        assertEquals(1, LegacyFlatSpillCleanerFixture.cleanupStaleSpillDirs(new String[] {root.toString()}, 0L, null),
                "旧 owner 退出后，旧目录仍只由旧版本扫描回收");
    }
}
