package io.tapdata.connector.postgres.cdc.physical;

import java.nio.charset.StandardCharsets;

/**
 * Little-endian cursor over a byte array, mirroring the access patterns of the
 * PostgreSQL on-disk format (all multi-byte integers are stored in the host's
 * native order, which for the supported platforms is little-endian). Provides
 * unsigned reads and MAXALIGN helpers used while walking heap tuples.
 *
 * @author Jarad
 */
public class WalByteReader {

    private final byte[] buf;
    private int pos;
    private final int start;
    private final int end;

    public WalByteReader(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public WalByteReader(byte[] buf, int offset, int length) {
        this.buf = buf;
        this.start = offset;
        this.pos = offset;
        this.end = offset + length;
    }

    public int position() {
        return pos - start;
    }

    public void position(int p) {
        this.pos = start + p;
    }

    public int remaining() {
        return end - pos;
    }

    public boolean hasRemaining() {
        return pos < end;
    }

    public void skip(int n) {
        ensure(n);
        pos += n;
    }

    public int readUInt8() {
        ensure(1);
        return buf[pos++] & 0xFF;
    }

    public byte readByte() {
        ensure(1);
        return buf[pos++];
    }

    public int readUInt16() {
        ensure(2);
        int v = (buf[pos] & 0xFF) | ((buf[pos + 1] & 0xFF) << 8);
        pos += 2;
        return v;
    }

    public short readInt16() {
        return (short) readUInt16();
    }

    public long readUInt32() {
        ensure(4);
        long v = (buf[pos] & 0xFFL)
                | ((buf[pos + 1] & 0xFFL) << 8)
                | ((buf[pos + 2] & 0xFFL) << 16)
                | ((buf[pos + 3] & 0xFFL) << 24);
        pos += 4;
        return v;
    }

    public int readInt32() {
        return (int) readUInt32();
    }

    public long readInt64() {
        ensure(8);
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (buf[pos + i] & 0xFFL) << (8 * i);
        }
        pos += 8;
        return v;
    }

    public int peekUInt8() {
        ensure(1);
        return buf[pos] & 0xFF;
    }

    public int peekUInt8(int ahead) {
        if (pos + ahead >= end) {
            throw new IndexOutOfBoundsException("WAL peek out of bounds: pos=" + position() + " ahead=" + ahead);
        }
        return buf[pos + ahead] & 0xFF;
    }

    public long peekUInt32() {
        ensure(4);
        return (buf[pos] & 0xFFL)
                | ((buf[pos + 1] & 0xFFL) << 8)
                | ((buf[pos + 2] & 0xFFL) << 16)
                | ((buf[pos + 3] & 0xFFL) << 24);
    }

    public byte[] readBytes(int n) {
        ensure(n);
        byte[] out = new byte[n];
        System.arraycopy(buf, pos, out, 0, n);
        pos += n;
        return out;
    }

    public String readString(int n) {
        return new String(readBytes(n), StandardCharsets.UTF_8);
    }

    /**
     * Advance the cursor to the next MAXALIGN boundary relative to the buffer
     * start, matching how PostgreSQL aligns successive tuple attributes.
     */
    public void alignMaxAlign() {
        int rel = position();
        int aligned = WalConstants.maxAlign(rel);
        pos = start + aligned;
    }

    /**
     * Align to an arbitrary power-of-two alignment (1/2/4/8) relative to the
     * buffer start, used for per-attribute typalign handling.
     */
    public void align(int alignment) {
        if (alignment <= 1) {
            return;
        }
        int rel = position();
        int aligned = (rel + (alignment - 1)) & ~(alignment - 1);
        pos = start + aligned;
    }

    private void ensure(int n) {
        if (pos + n > end) {
            throw new IndexOutOfBoundsException(
                    "WAL read out of bounds: pos=" + position() + " need=" + n + " remaining=" + remaining());
        }
    }
}
