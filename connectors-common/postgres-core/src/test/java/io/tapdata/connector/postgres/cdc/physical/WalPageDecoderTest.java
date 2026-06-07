package io.tapdata.connector.postgres.cdc.physical;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class WalPageDecoderTest {

    private static final long SEG = DEFAULT_WAL_SEGMENT_SIZE;

    private static void putU16(byte[] b, int o, int v) {
        b[o] = (byte) (v & 0xFF);
        b[o + 1] = (byte) ((v >> 8) & 0xFF);
    }

    private static void putU32(byte[] b, int o, long v) {
        for (int i = 0; i < 4; i++) {
            b[o + i] = (byte) ((v >> (8 * i)) & 0xFF);
        }
    }

    private static void writeLongHeader(byte[] b, int o, int info) {
        putU16(b, o, XLOG_PAGE_MAGIC_16);
        putU16(b, o + 2, info);
    }

    private static byte[] record(int totLen) {
        byte[] r = new byte[totLen];
        putU32(r, 0, totLen);
        for (int i = 4; i < totLen; i++) {
            r[i] = (byte) (i & 0xFF);
        }
        return r;
    }

    @Test
    public void testTwoRecordsSinglePage() {
        byte[] page = new byte[XLOG_BLCKSZ];
        writeLongHeader(page, 0, XLP_LONG_HEADER);
        byte[] r1 = record(30);
        byte[] r2 = record(40);
        System.arraycopy(r1, 0, page, SIZE_OF_XLOG_LONG_PHD, r1.length);          // at 40
        int p2 = (int) maxAlign((long) SIZE_OF_XLOG_LONG_PHD + r1.length);        // 72
        System.arraycopy(r2, 0, page, p2, r2.length);

        WalPageDecoder dec = new WalPageDecoder(SEG, SEG);
        dec.feed(page, 0, page.length);

        WalPageDecoder.RawRecord a = dec.nextRecord();
        assertNotNull(a);
        assertEquals(SEG + SIZE_OF_XLOG_LONG_PHD, a.lsn);
        assertArrayEquals(r1, a.bytes);

        WalPageDecoder.RawRecord b = dec.nextRecord();
        assertNotNull(b);
        assertEquals(SEG + p2, b.lsn);
        assertArrayEquals(r2, b.bytes);

        assertNull(dec.nextRecord());
    }

    @Test
    public void testRecordSpanningPageBoundary() {
        byte[] buf = new byte[2 * XLOG_BLCKSZ];
        writeLongHeader(buf, 0, XLP_LONG_HEADER);
        int onPage0 = XLOG_BLCKSZ - SIZE_OF_XLOG_LONG_PHD;       // 8152 logical bytes available on page0
        int totLen = onPage0 + 48;                              // spill 48 bytes onto page1
        byte[] r = record(totLen);
        // page0 portion
        System.arraycopy(r, 0, buf, SIZE_OF_XLOG_LONG_PHD, onPage0);
        // page1 short header with continuation flag + rem_len
        putU16(buf, XLOG_BLCKSZ, XLOG_PAGE_MAGIC_16);
        putU16(buf, XLOG_BLCKSZ + 2, XLP_FIRST_IS_CONTRECORD);
        putU32(buf, XLOG_BLCKSZ + 16, 48);
        // page1 portion
        System.arraycopy(r, onPage0, buf, XLOG_BLCKSZ + SIZE_OF_XLOG_SHORT_PHD, 48);

        WalPageDecoder dec = new WalPageDecoder(SEG, SEG);
        dec.feed(buf, 0, buf.length);

        WalPageDecoder.RawRecord a = dec.nextRecord();
        assertNotNull(a);
        assertEquals(SEG + SIZE_OF_XLOG_LONG_PHD, a.lsn);
        assertArrayEquals(r, a.bytes);
    }

    @Test
    public void testIncrementalFeed() {
        byte[] page = new byte[XLOG_BLCKSZ];
        writeLongHeader(page, 0, XLP_LONG_HEADER);
        byte[] r1 = record(64);
        System.arraycopy(r1, 0, page, SIZE_OF_XLOG_LONG_PHD, r1.length);

        WalPageDecoder dec = new WalPageDecoder(SEG, SEG);
        dec.feed(page, 0, 50);                 // header + partial record
        assertNull(dec.nextRecord());          // not enough yet
        dec.feed(page, 50, SIZE_OF_XLOG_LONG_PHD + 64 - 50);
        WalPageDecoder.RawRecord a = dec.nextRecord();
        assertNotNull(a);
        assertArrayEquals(r1, a.bytes);
    }

    @Test
    public void testResyncSkipsLeadingContrecord() {
        // Buffer starts at a page boundary whose page opens with a 16-byte
        // continuation fragment of a record we never saw; it must be skipped.
        byte[] page = new byte[XLOG_BLCKSZ];
        putU16(page, 0, XLOG_PAGE_MAGIC_16);
        putU16(page, 2, XLP_FIRST_IS_CONTRECORD);
        putU32(page, 16, 16);                  // rem_len = 16 leftover bytes
        int start = SIZE_OF_XLOG_SHORT_PHD + 16;
        int recStart = (int) maxAlign((long) start);
        byte[] r1 = record(32);
        System.arraycopy(r1, 0, page, recStart, r1.length);

        long startLsn = SEG + XLOG_BLCKSZ;     // page-aligned, NOT segment-aligned -> short header
        WalPageDecoder dec = new WalPageDecoder(startLsn, SEG);
        dec.feed(page, 0, page.length);

        WalPageDecoder.RawRecord a = dec.nextRecord();
        assertNotNull(a);
        assertArrayEquals(r1, a.bytes);
        assertEquals(startLsn + recStart, a.lsn);
    }
}
