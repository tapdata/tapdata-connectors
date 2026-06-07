package io.tapdata.connector.postgres.cdc.physical;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the deterministic helper logic of {@link PhysicalWalLogMiner}:
 * WAL segment-size parsing, commit-timestamp (TimestampTz) extraction and LSN
 * string round-tripping. The streaming/decoding path is covered by the page,
 * record and heap-rmgr decoder tests.
 */
public class PhysicalWalLogMinerTest {

    /* PostgreSQL epoch (2000-01-01) expressed as epoch micros, mirrored in the miner. */
    private static final long PG_EPOCH_MILLIS = 946684800000L;

    @Test
    public void testParseSizeUnits() {
        assertEquals(16L * 1024 * 1024, PhysicalWalLogMiner.parseSize("16MB"));
        assertEquals(1024L * 1024 * 1024, PhysicalWalLogMiner.parseSize("1GB"));
        assertEquals(64L * 1024, PhysicalWalLogMiner.parseSize("64kB"));
        assertEquals(8192L, PhysicalWalLogMiner.parseSize("8192B"));
        assertEquals(8192L, PhysicalWalLogMiner.parseSize("8192"));
    }

    @Test
    public void testParseSizeCaseAndSpaces() {
        assertEquals(16L * 1024 * 1024, PhysicalWalLogMiner.parseSize("  16mb "));
        assertEquals(2L * 1024 * 1024 * 1024, PhysicalWalLogMiner.parseSize("2Gb"));
    }

    @Test
    public void testParseSizeInvalid() {
        assertEquals(0L, PhysicalWalLogMiner.parseSize(null));
        assertEquals(0L, PhysicalWalLogMiner.parseSize(""));
        assertEquals(0L, PhysicalWalLogMiner.parseSize("garbage"));
    }

    @Test
    public void testReadCommitMillisAtEpoch() {
        // TimestampTz == 0 -> exactly 2000-01-01T00:00:00Z
        assertEquals(PG_EPOCH_MILLIS, PhysicalWalLogMiner.readCommitMillis(le64(0L)));
    }

    @Test
    public void testReadCommitMillisOffset() {
        // 5_000_000 micros == 5 seconds after the PG epoch
        assertEquals(PG_EPOCH_MILLIS + 5000L, PhysicalWalLogMiner.readCommitMillis(le64(5_000_000L)));
    }

    @Test
    public void testReadCommitMillisShortDataFallsBack() {
        long before = System.currentTimeMillis();
        long ts = PhysicalWalLogMiner.readCommitMillis(new byte[]{1, 2, 3});
        assertTrue(ts >= before);
        assertEquals(System.currentTimeMillis(), ts, 5000L);
    }

    @Test
    public void testReadSubxactsNoInfoFlag() {
        // Without XLOG_XACT_HAS_INFO there is no xinfo word, hence no subxacts.
        byte[] body = le64(5_000_000L);
        assertEquals(0, PhysicalWalLogMiner.readSubxacts(body, 0).length);
        assertEquals(0, PhysicalWalLogMiner.readSubxacts(null, WalConstants.XLOG_XACT_HAS_INFO).length);
    }

    @Test
    public void testReadSubxactsInfoButNoSubxacts() {
        byte[] body = concat(le64(0L), le32(0)); // xact_time + xinfo=0
        assertEquals(0, PhysicalWalLogMiner.readSubxacts(body, WalConstants.XLOG_XACT_HAS_INFO).length);
    }

    @Test
    public void testReadSubxactsParsesList() {
        byte[] body = concat(le64(0L), le32(WalConstants.XACT_XINFO_HAS_SUBXACTS),
                le32(3), le32(101), le32(102), le32(103));
        long[] subs = PhysicalWalLogMiner.readSubxacts(body, WalConstants.XLOG_XACT_HAS_INFO);
        assertArrayEquals(new long[]{101L, 102L, 103L}, subs);
    }

    @Test
    public void testReadSubxactsSkipsDbInfoBeforeSubxacts() {
        int xinfo = WalConstants.XACT_XINFO_HAS_DBINFO | WalConstants.XACT_XINFO_HAS_SUBXACTS;
        byte[] body = concat(le64(0L), le32(xinfo),
                le32(1234), le32(5678),       // xl_xact_dbinfo: dbId + tsId
                le32(2), le32(55), le32(66)); // nsubxacts + subxids
        long[] subs = PhysicalWalLogMiner.readSubxacts(body, WalConstants.XLOG_XACT_HAS_INFO);
        assertArrayEquals(new long[]{55L, 66L}, subs);
    }

    @Test
    public void testLsnStrRoundTrip() {
        String[] samples = {"0/0", "0/16B6A50", "16/B374D848", "FF/FFFFFFFF"};
        for (String s : samples) {
            long asLong = org.postgresql.replication.LogSequenceNumber.valueOf(s).asLong();
            assertEquals(normalize(s), PhysicalWalLogMiner.lsnStr(asLong));
        }
    }

    /* LogSequenceNumber.asString() emits upper-case hex with no leading zeros in
     * each half except the trailing half is zero-padded to 8 hex digits. */
    private static String normalize(String s) {
        return org.postgresql.replication.LogSequenceNumber.valueOf(s).asString();
    }

    private static byte[] le64(long v) {
        byte[] b = new byte[8];
        for (int i = 0; i < 8; i++) {
            b[i] = (byte) (v >>> (8 * i));
        }
        return b;
    }

    private static byte[] le32(int v) {
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            b[i] = (byte) (v >>> (8 * i));
        }
        return b;
    }

    private static byte[] concat(byte[]... parts) {
        int len = 0;
        for (byte[] p : parts) {
            len += p.length;
        }
        byte[] out = new byte[len];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}
