package io.tapdata.connector.postgres.cdc.physical;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class HeapTupleDecoderTest {

    private static void u16(ByteArrayOutputStream o, int v) {
        o.write(v & 0xFF);
        o.write((v >> 8) & 0xFF);
    }

    private static void u32(ByteArrayOutputStream o, long v) {
        for (int i = 0; i < 4; i++) {
            o.write((int) ((v >> (8 * i)) & 0xFF));
        }
    }

    private static void u64(ByteArrayOutputStream o, long v) {
        for (int i = 0; i < 8; i++) {
            o.write((int) ((v >> (8 * i)) & 0xFF));
        }
    }

    @Test
    public void testDeformFixedAndVarlena() {
        List<ColumnInfo> cols = Arrays.asList(
                new ColumnInfo("a", 1, PgTypeDecoder.INT4, 4, 'i', false),
                new ColumnInfo("b", 2, PgTypeDecoder.INT8, 8, 'd', false),
                new ColumnInfo("c", 3, PgTypeDecoder.TEXT, -1, 'i', false));

        int tHoff = maxAlign(SIZE_OF_HEAP_TUPLE_HEADER);     // 24, no null bitmap
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, 3);                          // t_infomask2 = natts
        u16(o, 0);                          // t_infomask = no nulls
        o.write(tHoff);                     // t_hoff
        // padding between offset 23 and t_hoff (24) -> 1 byte
        for (int i = 0; i < tHoff - SIZE_OF_HEAP_TUPLE_HEADER; i++) {
            o.write(0);
        }
        u32(o, 42);                         // a int4
        u32(o, 0);                          // align padding to 8 for int8
        u64(o, 123456789L);                 // b int8
        // c text "hi" short varlena: header=(payload+1)<<1|1
        o.write(((2 + 1) << 1) | 1);
        o.write('h');
        o.write('i');

        Map<String, Object> m = HeapTupleDecoder.decode(o.toByteArray(), cols);
        assertEquals(42, m.get("a"));
        assertEquals(123456789L, m.get("b"));
        assertEquals("hi", m.get("c"));
    }

    @Test
    public void testNullBitmapAndDropped() {
        List<ColumnInfo> cols = Arrays.asList(
                new ColumnInfo("a", 1, PgTypeDecoder.INT4, 4, 'i', false),
                new ColumnInfo("dead", 2, PgTypeDecoder.INT4, 4, 'i', true),
                new ColumnInfo("c", 3, PgTypeDecoder.INT4, 4, 'i', false));

        int natts = 3;
        int bitmapLen = (natts + 7) / 8;    // 1
        int tHoff = maxAlign(SIZE_OF_HEAP_TUPLE_HEADER + bitmapLen);   // 24
        // a present, dead null, c present -> bits 1,0,1 -> 0b101 = 0x05
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, natts);
        u16(o, HEAP_HASNULL);
        o.write(tHoff);
        o.write(0x05);                      // null bitmap
        for (int i = 0; i < tHoff - SIZE_OF_HEAP_TUPLE_HEADER - bitmapLen; i++) {
            o.write(0);                     // padding to t_hoff
        }
        u32(o, 7);                          // a
        u32(o, 9);                          // c (dead is null, no bytes)

        Map<String, Object> m = HeapTupleDecoder.decode(o.toByteArray(), cols);
        assertEquals(7, m.get("a"));
        assertEquals(9, m.get("c"));
        assertFalse(m.containsKey("dead"));
        assertEquals(2, m.size());
    }

    @Test
    public void testExternalToastPointerFetch() {
        List<ColumnInfo> cols = Arrays.asList(
                new ColumnInfo("payload", 1, PgTypeDecoder.TEXT, -1, 'i', false));

        int tHoff = maxAlign(SIZE_OF_HEAP_TUPLE_HEADER);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, 1);
        u16(o, 0);
        o.write(tHoff);
        for (int i = 0; i < tHoff - SIZE_OF_HEAP_TUPLE_HEADER; i++) {
            o.write(0);
        }
        o.write(0x01);                      // external varlena header
        o.write(18);                        // VARTAG_ONDISK
        u32(o, 11);                         // raw size
        u32(o, 7);                          // extinfo: stored payload size
        u32(o, 77);                         // valueId
        u32(o, 991);                        // toastRelId

        Map<String, Object> m = HeapTupleDecoder.decode(o.toByteArray(), cols, (toastRelId, valueId) -> {
            assertEquals(991L, toastRelId);
            assertEquals(77L, valueId);
            return "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        });

        assertEquals("payload", m.get("payload"));
    }

    @Test
    public void testExternalToastPointerFetchCompressed() {
        assertExternalToastPointerFetchCompressed(0);
    }

    @Test
    public void testExternalToastPointerFetchCompressedPg14Pglz() {
        assertExternalToastPointerFetchCompressed(1);
    }

    private static void assertExternalToastPointerFetchCompressed(int method) {
        List<ColumnInfo> cols = Arrays.asList(
                new ColumnInfo("payload", 1, PgTypeDecoder.TEXT, -1, 'i', false));

        int tHoff = maxAlign(SIZE_OF_HEAP_TUPLE_HEADER);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, 1);
        u16(o, 0);
        o.write(tHoff);
        for (int i = 0; i < tHoff - SIZE_OF_HEAP_TUPLE_HEADER; i++) {
            o.write(0);
        }
        o.write(0x01);
        o.write(18);
        u32(o, 10);                         // raw size = header + 6-byte payload
        u32(o, 5 | ((long) method << 30));   // ext size + compression method
        u32(o, 77);                         // valueId
        u32(o, 991);                        // toastRelId

        byte[] toasted = new byte[] {
                0x04,                       // ctrl: literal, literal, copy
                'a', 'b',
                0x01, 0x02                  // back-ref -> "ababab"
        };
        Map<String, Object> m = HeapTupleDecoder.decode(o.toByteArray(), cols, (toastRelId, valueId) -> toasted);

        assertEquals("ababab", m.get("payload"));
    }

    @Test
    public void testExternalToastPointerFetchUnsupportedCompressionReturnsNull() {
        List<ColumnInfo> cols = Arrays.asList(
                new ColumnInfo("payload", 1, PgTypeDecoder.TEXT, -1, 'i', false));

        int tHoff = maxAlign(SIZE_OF_HEAP_TUPLE_HEADER);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, 1);
        u16(o, 0);
        o.write(tHoff);
        for (int i = 0; i < tHoff - SIZE_OF_HEAP_TUPLE_HEADER; i++) {
            o.write(0);
        }
        o.write(0x01);
        o.write(18);
        u32(o, 10);
        u32(o, 5 | (2L << 30));             // lz4/unsupported compression method
        u32(o, 77);
        u32(o, 991);

        Map<String, Object> m = HeapTupleDecoder.decode(o.toByteArray(), cols, (toastRelId, valueId) -> new byte[] {
                0x04, 'a', 'b', 0x01, 0x02
        });

        assertNull(m.get("payload"));
    }

    @Test
    public void testInlineCompressedVarlenaPg14Pglz() {
        List<ColumnInfo> cols = Arrays.asList(
                new ColumnInfo("payload", 1, PgTypeDecoder.TEXT, -1, 'i', false));

        byte[] compressed = new byte[] {
                0x04, 'a', 'b', 0x01, 0x02
        };
        int tHoff = maxAlign(SIZE_OF_HEAP_TUPLE_HEADER);
        int total = 8 + compressed.length;
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, 1);
        u16(o, 0);
        o.write(tHoff);
        for (int i = 0; i < tHoff - SIZE_OF_HEAP_TUPLE_HEADER; i++) {
            o.write(0);
        }
        u32(o, ((long) total << 2) | 0x02);  // 4B compressed varlena header
        u32(o, 6 | (1L << 30));              // raw payload size + pglz method
        o.write(compressed, 0, compressed.length);

        Map<String, Object> m = HeapTupleDecoder.decode(o.toByteArray(), cols);

        assertEquals("ababab", m.get("payload"));
    }
}
