package io.tapdata.connector.postgres.cdc.physical;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;

/**
 * Deforms a heap tuple as carried in WAL into a column-name to Java-value map,
 * faithfully reproducing {@code heap_deform_tuple} plus the WAL framing applied
 * by {@code heap_xlog_insert}/{@code update}. The input is the bytes of an
 * {@code xl_heap_header} (t_infomask2, t_infomask, t_hoff) immediately followed
 * by the null bitmap, optional alignment padding and the attribute data.
 *
 * <p>Alignment of attributes is computed relative to the column-data start; this
 * is correct because {@code t_hoff} is always MAXALIGN'd, so the byte that the
 * data starts at sits on an 8-byte boundary of the reconstructed tuple.</p>
 *
 * @author Jarad
 */
public final class HeapTupleDecoder {

    private HeapTupleDecoder() {
    }

    @FunctionalInterface
    public interface ToastedValueFetcher {
        byte[] fetch(long toastRelId, long valueId);
    }

    /* on-disk TOAST pointer: VARHDRSZ_EXTERNAL(2) + sizeof(varatt_external)(16). */
    private static final int VARTAG_ONDISK = 18;
    private static final int EXTERNAL_ONDISK_SIZE = 2 + 16;
    private static final int VARHDRSZ = 4;

    public static Map<String, Object> decode(byte[] tuple, List<ColumnInfo> columns) {
        return decode(tuple, columns, null);
    }

    public static Map<String, Object> decode(byte[] tuple, List<ColumnInfo> columns, ToastedValueFetcher toastFetcher) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (tuple == null || tuple.length < SIZE_OF_HEAP_HEADER) {
            return out;
        }
        WalByteReader h = new WalByteReader(tuple);
        int infomask2 = h.readUInt16();
        int infomask = h.readUInt16();
        int tHoff = h.readUInt8();
        int natts = infomask2 & HEAP_NATTS_MASK;
        boolean hasNull = (infomask & HEAP_HASNULL) != 0;

        int colDataStart = SIZE_OF_HEAP_HEADER + (tHoff - SIZE_OF_HEAP_TUPLE_HEADER);
        if (colDataStart < SIZE_OF_HEAP_HEADER || colDataStart > tuple.length) {
            return out;
        }
        byte[] nullBitmap = null;
        if (hasNull) {
            int bitmapLen = (natts + 7) / 8;
            if (SIZE_OF_HEAP_HEADER + bitmapLen > tuple.length) {
                return out;
            }
            nullBitmap = new byte[bitmapLen];
            System.arraycopy(tuple, SIZE_OF_HEAP_HEADER, nullBitmap, 0, bitmapLen);
        }

        WalByteReader r = new WalByteReader(tuple, colDataStart, tuple.length - colDataStart);

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo col = columns.get(i);
            if (i >= natts) {
                if (!col.dropped) {
                    out.put(col.name, null);
                }
                continue;
            }
            boolean isNull = hasNull && !bitSet(nullBitmap, i);
            if (isNull) {
                if (!col.dropped) {
                    out.put(col.name, null);
                }
                continue;
            }
            Object value;
            try {
                value = readAttr(r, col, toastFetcher);
            } catch (RuntimeException ex) {
                // Malformed/garbage tuple bytes (typically a stale cache page or
                // an unrecoverable delta-only update): abort the rest of the row
                // with nulls rather than killing the whole miner.
                for (int j = i; j < columns.size(); j++) {
                    ColumnInfo c = columns.get(j);
                    if (!c.dropped) {
                        out.put(c.name, null);
                    }
                }
                return out;
            }
            if (!col.dropped) {
                out.put(col.name, value);
            }
        }
        return out;
    }

    private static boolean bitSet(byte[] bitmap, int i) {
        return (bitmap[i >> 3] & (1 << (i & 7))) != 0;
    }

    private static Object readAttr(WalByteReader r, ColumnInfo col, ToastedValueFetcher toastFetcher) {
        int attlen = col.typLen;
        if (attlen > 0) {
            r.align(col.typAlign);
            return PgTypeDecoder.decode(col.typeOid, r.readBytes(attlen));
        }
        if (attlen == -1) {
            return readVarlena(r, col, toastFetcher);
        }
        if (attlen == -2) {
            // cstring: null-terminated, no alignment beyond byte
            StringBuilder sb = new StringBuilder();
            int b;
            while ((b = r.readUInt8()) != 0) {
                sb.append((char) b);
            }
            return sb.toString();
        }
        throw new IllegalStateException("unsupported attlen " + attlen + " for column " + col.name);
    }

    private static Object readVarlena(WalByteReader r, ColumnInfo col, ToastedValueFetcher toastFetcher) {
        int first = r.peekUInt8();
        if ((first & 0x01) == 0x01) {
            if (first == 0x01) {
                // 1B external TOAST pointer; value lives in the TOAST relation
                int tag = r.peekUInt8(1);
                if (tag != VARTAG_ONDISK) {
                    r.skip(2);
                    return null;
                }
                r.skip(2);
                int rawSize = r.readInt32();
                long extInfo = r.readUInt32();
                long valueId = r.readUInt32();
                long toastRelId = r.readUInt32();
                byte[] toasted;
                try {
                    toasted = toastFetcher == null ? null : toastFetcher.fetch(toastRelId, valueId);
                } catch (RuntimeException ex) {
                    toasted = null;
                }
                if (toasted == null) {
                    return null;
                }
                int extSize = (int) (extInfo & 0x3FFFFFFFL);
                boolean compressed = extSize < rawSize - VARHDRSZ;
                if (compressed) {
                    int method = (int) ((extInfo >> 30) & 0x03);
                    if (!isPglzCompressionMethod(method)) {
                        return null;
                    }
                    byte[] plain = Pglz.decompress(toasted, rawSize - VARHDRSZ);
                    return plain == null ? null : PgTypeDecoder.decode(col.typeOid, plain);
                }
                return PgTypeDecoder.decode(col.typeOid, toasted);
            }
            int total = (first >> 1) & 0x7F;        // includes the 1-byte header
            r.skip(1);
            return PgTypeDecoder.decode(col.typeOid, r.readBytes(total - 1));
        }
        // 4-byte header: align first
        r.align(col.typAlign);
        long header = r.peekUInt32();
        int total = (int) ((header >> 2) & 0x3FFFFFFF);     // includes 4-byte header
        if (total < 4 || total - 4 > r.remaining() - 4) {
            throw new IllegalStateException("implausible varlena length " + total
                    + " for column " + col.name + " (remaining=" + r.remaining() + ")");
        }
        boolean compressed = (header & 0x03) == 0x02;
        if (compressed) {
            r.skip(4);
            long tcinfo = r.readUInt32();
            int rawSize = (int) (tcinfo & 0x3FFFFFFF);
            int method = (int) ((tcinfo >> 30) & 0x03);
            byte[] comp = r.readBytes(total - 8);
            byte[] plain = isPglzCompressionMethod(method) ? Pglz.decompress(comp, rawSize) : null;
            return plain == null ? null : PgTypeDecoder.decode(col.typeOid, plain);
        }
        r.skip(4);
        return PgTypeDecoder.decode(col.typeOid, r.readBytes(total - 4));
    }

    private static boolean isPglzCompressionMethod(int method) {
        return method == 0 || method == 1;
    }
}
