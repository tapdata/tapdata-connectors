package io.tapdata.connector.postgres.cdc.physical;

import java.util.Arrays;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;

/**
 * Reassembles complete {@link XLogRecord} byte blocks out of a raw physical-WAL
 * byte stream. Because the physical buffer is a contiguous slice of the WAL,
 * every physical byte maps 1:1 to an absolute LSN, so 8KB page headers (short or
 * long) are located purely from {@code lsn % XLOG_BLCKSZ}. Records that span
 * page boundaries are stitched transparently; trailing zero padding and the
 * leading continuation fragment of the start page are skipped.
 *
 * @author Jarad
 */
public class WalPageDecoder {

    private byte[] buf;
    private int len;
    private long bufferStartLsn;
    private int physPos;
    private final long segSize;
    private boolean needResync = true;

    public WalPageDecoder(long startLsn, long segSize) {
        this.bufferStartLsn = startLsn;
        this.segSize = segSize <= 0 ? DEFAULT_WAL_SEGMENT_SIZE : segSize;
        this.buf = new byte[XLOG_BLCKSZ];
        this.len = 0;
    }

    public void feed(byte[] data, int offset, int length) {
        if (length <= 0) {
            return;
        }
        if (len + length > buf.length) {
            int cap = Math.max(buf.length * 2, len + length);
            buf = Arrays.copyOf(buf, cap);
        }
        System.arraycopy(data, offset, buf, len, length);
        len += length;
    }

    /** Drop already-consumed bytes so the buffer does not grow unbounded. */
    public void compact() {
        if (physPos == 0) {
            return;
        }
        System.arraycopy(buf, physPos, buf, 0, len - physPos);
        len -= physPos;
        bufferStartLsn += physPos;
        physPos = 0;
    }

    public long currentLsn() {
        return bufferStartLsn + physPos;
    }

    /**
     * @return the next complete record, or {@code null} when more bytes must be
     * fed before a full record is available.
     */
    public RawRecord nextRecord() {
        if (needResync && !resync()) {
            return null;
        }
        while (true) {
            int p = alignMax(physPos);
            int[] ph = {p};
            byte[] lenBuf = new byte[4];
            long[] firstLsn = new long[1];
            if (!readLogical(ph, 4, lenBuf, firstLsn)) {
                return null;
            }
            long totLen = u32(lenBuf, 0);
            if (totLen == 0) {
                int np = skipToNextPage(p);
                if (np > len) {
                    return null;
                }
                physPos = np;
                continue;
            }
            // Defensive bounds check: a valid XLogRecord's xl_tot_len is at least
            // SIZE_OF_XLOG_RECORD (24) and at most 1 GB (though real records rarely
            // exceed a few MB). Values outside this range mean we are not at a
            // record header — typically because the decoder started reading from a
            // non-record-aligned LSN (e.g. a look-back anchor in the middle of a
            // record's payload, or a recycled WAL segment whose first page was
            // zero-filled). The diagnostic hex dump lets post-mortem analysis
            // determine whether the misalignment is deterministic or transient.
            if (totLen < SIZE_OF_XLOG_RECORD || totLen > 1024 * 1024 * 1024L) {
                long recordLsn = firstLsn[0];
                int pageOff = (int) (recordLsn % XLOG_BLCKSZ);
                boolean pageAligned = (bufferStartLsn % XLOG_BLCKSZ) == 0;
                throw new IllegalStateException(String.format(
                        "Invalid xl_tot_len=%d (0x%08X) at record-lsn=%s " +
                        "buffer-start=%s page-boundary=%s page-off=%d physPos=%d len=%d. " +
                        "The decoder is likely NOT at a WAL record header. " +
                        "First 64 logical bytes at record-lsn: %s",
                        totLen, totLen,
                        lsnHex(recordLsn),
                        lsnHex(bufferStartLsn),
                        pageAligned, pageOff, physPos, len,
                        hexDumpLogical(recordLsn, 64)));
            }
            byte[] rec = new byte[(int) totLen];
            int[] ph2 = {p};
            long[] recLsn = new long[1];
            if (!readLogical(ph2, (int) totLen, rec, recLsn)) {
                return null;
            }
            physPos = ph2[0];
            long endLsn = bufferStartLsn + ph2[0];
            return new RawRecord(recLsn[0], maxAlign(endLsn), rec);
        }
    }

    /* On a fresh start the buffer begins at a page boundary; if that page opens
     * with the tail of a record continued from the previous page, skip it. */
    private boolean resync() {
        long lsn = bufferStartLsn;
        if (lsn % XLOG_BLCKSZ != 0) {
            needResync = false;
            return true;
        }
        int hsz = (lsn % segSize == 0) ? SIZE_OF_XLOG_LONG_PHD : SIZE_OF_XLOG_SHORT_PHD;
        if (len < hsz) {
            return false;
        }
        int info = u16(buf, 2);
        if ((info & XLP_FIRST_IS_CONTRECORD) != 0) {
            long remLen = u32(buf, 16);
            int target = hsz + (int) remLen;
            if (len < target) {
                return false;
            }
            physPos = target;
        }
        needResync = false;
        return true;
    }

    private int alignMax(int p) {
        long lsn = bufferStartLsn + p;
        return p + (int) (maxAlign(lsn) - lsn);
    }

    private int skipToNextPage(int p) {
        long lsn = bufferStartLsn + p;
        long next = ((lsn / XLOG_BLCKSZ) + 1) * XLOG_BLCKSZ;
        return p + (int) (next - lsn);
    }

    /* Copy n logical bytes into dest, skipping page headers; firstLsn[0] gets the
     * LSN of the first real data byte. Returns false if the buffer is short. */
    private boolean readLogical(int[] ph, int n, byte[] dest, long[] firstLsn) {
        int p = ph[0];
        int got = 0;
        boolean firstSet = false;
        while (got < n) {
            long lsn = bufferStartLsn + p;
            int off = (int) (lsn % XLOG_BLCKSZ);
            if (off == 0) {
                int hsz = (lsn % segSize == 0) ? SIZE_OF_XLOG_LONG_PHD : SIZE_OF_XLOG_SHORT_PHD;
                if (p + hsz > len) {
                    return false;
                }
                p += hsz;
                continue;
            }
            if (!firstSet) {
                firstLsn[0] = lsn;
                firstSet = true;
            }
            int inPage = XLOG_BLCKSZ - off;
            int chunk = Math.min(n - got, inPage);
            if (p + chunk > len) {
                return false;
            }
            System.arraycopy(buf, p, dest, got, chunk);
            got += chunk;
            p += chunk;
        }
        ph[0] = p;
        return true;
    }

    private static int u16(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8);
    }

    private static long u32(byte[] b, int o) {
        return (b[o] & 0xFFL) | ((b[o + 1] & 0xFFL) << 8) | ((b[o + 2] & 0xFFL) << 16) | ((b[o + 3] & 0xFFL) << 24);
    }

    private static String lsnHex(long lsn) {
        long hi = lsn >>> 32;
        long lo = lsn & 0xFFFFFFFFL;
        return String.format("%X/%08X", hi, lo);
    }

    /**
     * Read up to {@code maxBytes} logical bytes (skipping page headers) starting
     * from {@code startLsn} and format them as a space-separated hex string for
     * diagnostic inclusion in error messages. Returns empty string if the
     * requested LSN range is not yet in buffer.
     */
    private String hexDumpLogical(long startLsn, int maxBytes) {
        StringBuilder sb = new StringBuilder();
        long lsn = startLsn;
        int remaining = maxBytes;
        while (remaining > 0) {
            int off = (int) (lsn % XLOG_BLCKSZ);
            if (off == 0) {
                int hsz = (lsn % segSize == 0) ? SIZE_OF_XLOG_LONG_PHD : SIZE_OF_XLOG_SHORT_PHD;
                lsn += hsz;
                continue;
            }
            int physOff = (int) (lsn - bufferStartLsn);
            if (physOff < 0 || physOff >= len) {
                break;
            }
            int chunk = Math.min(remaining, XLOG_BLCKSZ - off);
            chunk = Math.min(chunk, len - physOff);
            for (int i = 0; i < chunk; i++) {
                sb.append(String.format("%02X ", buf[physOff + i] & 0xFF));
            }
            remaining -= chunk;
            lsn += chunk;
        }
        return sb.toString().trim();
    }

    /** A complete raw record: its start LSN, the next-record LSN, and the bytes. */
    public static class RawRecord {
        public final long lsn;
        public final long nextLsn;
        public final byte[] bytes;

        public RawRecord(long lsn, long nextLsn, byte[] bytes) {
            this.lsn = lsn;
            this.nextLsn = nextLsn;
            this.bytes = bytes;
        }
    }
}
