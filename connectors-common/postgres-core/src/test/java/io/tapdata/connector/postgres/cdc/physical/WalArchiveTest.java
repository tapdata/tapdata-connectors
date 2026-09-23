package io.tapdata.connector.postgres.cdc.physical;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WalArchiveTest {

    @Test
    void segmentFileNameUsesPostgresTimelineLogAndSegmentLayout() {
        long lsn = org.postgresql.replication.LogSequenceNumber.valueOf("0/6728DF0").asLong();
        assertEquals("0000001F0000000000000006",
                WalArchive.segmentFileName(lsn, 31, 16L * 1024 * 1024));
    }

    @Test
    void localArchiveResolvesDirectAndTimelineNestedFiles() throws Exception {
        Path root = Files.createTempDirectory("wal-archive-test-");
        Path direct = root.resolve("0000001F0000000000000006");
        Files.write(direct, new byte[] {1, 2, 3});
        WalArchive archive = WalArchive.from(root.toString(), null, 16L * 1024 * 1024);
        assertEquals(direct, archive.obtain(31, 6L * 16L * 1024 * 1024));

        Files.delete(direct);
        Path nestedDir = Files.createDirectories(root.resolve("0000001F"));
        Path nested = nestedDir.resolve("0000001F0000000000000006");
        Files.write(nested, new byte[] {4, 5, 6});
        assertEquals(nested, archive.obtain(31, 6L * 16L * 1024 * 1024));
    }

    @Test
    void rangesSplitAtTimelineHistoryWithoutSplittingARecordRangeBySegment() {
        long start = 0x6000000L;
        long fork = 0x6728DF0L;
        long end = 0x6758150L;
        WalArchive archive = WalArchive.from("/tmp/archive-test", null, 16L * 1024 * 1024);
        List<WalArchive.Range> ranges = archive.ranges(start, end, lsn -> lsn < fork ? 31 : 32);

        assertEquals(2, ranges.size());
        assertEquals(31, ranges.get(0).timeline);
        assertEquals(start, ranges.get(0).startLsn);
        assertEquals(fork, ranges.get(0).endLsn);
        assertEquals(32, ranges.get(1).timeline);
        assertEquals(fork, ranges.get(1).startLsn);
        assertEquals(end, ranges.get(1).endLsn);
    }

    @Test
    void readRangeOnlyReturnsRequestedBytes() throws Exception {
        Path root = Files.createTempDirectory("wal-archive-test-");
        Path file = root.resolve("0000001F0000000000000006");
        byte[] bytes = new byte[64];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        Files.write(file, bytes);
        WalArchive archive = WalArchive.from(root.toString(), null, 64);
        assertArrayEquals(new byte[] {10, 11, 12, 13},
                archive.readRange(file, 0, 10, 14));
    }
}
