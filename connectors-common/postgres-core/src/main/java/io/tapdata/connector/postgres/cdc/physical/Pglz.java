package io.tapdata.connector.postgres.cdc.physical;

/**
 * Pure-Java port of PostgreSQL's pglz decompressor
 * (src/common/pg_lzcompress.c, {@code pglz_decompress}). Used to inflate
 * compressed varlena attribute values carried inline in heap tuples. The
 * compressed payload is the bytes following the 4-byte varlena header and the
 * 4-byte {@code va_tcinfo} word; {@code rawSize} is taken from that word.
 *
 * @author Jarad
 */
public final class Pglz {

    private Pglz() {
    }

    public static byte[] decompress(byte[] src, int rawSize) {
        if (src == null || rawSize <= 0) {
            return null;
        }
        byte[] dest = new byte[rawSize];
        int sp = 0;
        int dp = 0;
        int srcend = src.length;
        while (sp < srcend && dp < rawSize) {
            int ctrl = src[sp++] & 0xFF;
            for (int c = 0; c < 8 && sp < srcend && dp < rawSize; c++) {
                if ((ctrl & 1) != 0) {
                    int len = (src[sp] & 0x0F) + 3;
                    int off = ((src[sp] & 0xF0) << 4) | (src[sp + 1] & 0xFF);
                    sp += 2;
                    if (len == 18) {
                        len += src[sp++] & 0xFF;
                    }
                    if (off == 0 || dp - off < 0) {
                        return null;            // malformed back-reference
                    }
                    if (len > rawSize - dp) {
                        len = rawSize - dp;
                    }
                    while (len-- > 0) {
                        dest[dp] = dest[dp - off];
                        dp++;
                    }
                } else {
                    dest[dp++] = src[sp++];
                }
                ctrl >>= 1;
            }
        }
        if (dp != rawSize) {
            return null;                        // truncated / unexpected end
        }
        return dest;
    }
}
