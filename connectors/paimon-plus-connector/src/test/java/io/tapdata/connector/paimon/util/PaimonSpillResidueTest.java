package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.IOManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** AC-25：多表、多根反复启停后无租约残留、无 owner 登记泄漏、无文件描述符泄漏。 */
@DisabledOnOs(OS.WINDOWS)
class PaimonSpillResidueTest {

    private static final int ROUNDS = 20;
    private static final int TABLES = 3;

    @Test
    void repeatedStartStopAcrossTablesAndRootsLeavesNothingBehind(@TempDir Path base) throws Exception {
        Path first = Files.createDirectory(base.resolve("first"));
        Path second = Files.createDirectory(base.resolve("second"));
        String roots = first + "," + second;
        int ownersBefore = SpillTestSupport.ownerRegistrationCount();
        long descriptorsBefore = openDescriptors();

        for (int round = 0; round < ROUNDS; round++) {
            List<PaimonSpillDirCleaner.IOManagerBuildResult> tables = new ArrayList<>();
            for (int table = 0; table < TABLES; table++) {
                PaimonSpillDirCleaner.IOManagerBuildResult built =
                        PaimonSpillDirCleaner.resolveAndCreateIOManager(roots, "default.t" + table);
                assertEquals(2, built.spillDirs().size(), "每个根各一个租约");
                IOManager io = built.ioManager();
                io.createBufferFileWriter(io.createChannel()).close();
                io.createBufferFileWriter(io.createChannel()).close();
                tables.add(built);
            }
            for (PaimonSpillDirCleaner.IOManagerBuildResult built : tables) {
                built.ioManager().close();
                assertTrue(PaimonSpillDirCleaner.releaseAfterClose(built.spillDirs(), true).complete());
            }
        }

        assertEquals(ownersBefore, SpillTestSupport.ownerRegistrationCount(), "owner 登记不得泄漏");
        assertNoEntries(first);
        assertNoEntries(second);
        long descriptorsAfter = openDescriptors();
        if (descriptorsBefore >= 0 && descriptorsAfter >= 0) {
            assertTrue(descriptorsAfter <= descriptorsBefore + 4,
                    "锁 channel 不得泄漏: before=" + descriptorsBefore + " after=" + descriptorsAfter);
        }
    }

    private static void assertNoEntries(Path root) throws Exception {
        try (Stream<Path> entries = Files.list(root)) {
            assertEquals(0L, entries.count(), "正常停止后根目录下不留任何协议目录: " + root);
        }
        assertFalse(Files.exists(root.resolve("tapdata-paimon-v2")));
    }

    /** HotSpot/OpenJDK 在 Unix 上提供 UnixOperatingSystemMXBean；不可用时返回 -1，跳过该项比较。 */
    private static long openDescriptors() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.UnixOperatingSystemMXBean) {
            return ((com.sun.management.UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
        }
        return -1L;
    }
}
