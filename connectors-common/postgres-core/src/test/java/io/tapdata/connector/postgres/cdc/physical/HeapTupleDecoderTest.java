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

    private static byte[] hex(String s) {
        s = s.replaceAll("\\s+", "");
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    @Test
    public void testDeformFixedAndVarlena() {
        List<ColumnInfo> cols = Arrays.asList(
                new ColumnInfo("a", 1, PgTypeDecoder.INT4, 4, 'i', false),
                new ColumnInfo("b", 2, PgTypeDecoder.INT8, 8, 'd', false),
                new ColumnInfo("c", 3, PgTypeDecoder.TEXT, -1, 'i', false));

        int tHoff = maxAlign(SIZE_OF_HEAP_TUPLE_HEADER);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, 3);
        u16(o, 0);
        o.write(tHoff);
        for (int i = 0; i < tHoff - SIZE_OF_HEAP_TUPLE_HEADER; i++) {
            o.write(0);
        }
        u32(o, 42);
        u32(o, 0);
        u64(o, 123456789L);
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
        int bitmapLen = (natts + 7) / 8;
        int tHoff = maxAlign(SIZE_OF_HEAP_TUPLE_HEADER + bitmapLen);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, natts);
        u16(o, HEAP_HASNULL);
        o.write(tHoff);
        o.write(0x05);
        for (int i = 0; i < tHoff - SIZE_OF_HEAP_TUPLE_HEADER - bitmapLen; i++) {
            o.write(0);
        }
        u32(o, 7);
        u32(o, 9);

        Map<String, Object> m = HeapTupleDecoder.decode(o.toByteArray(), cols);
        assertEquals(7, m.get("a"));
        assertEquals(9, m.get("c"));
        assertFalse(m.containsKey("dead"));
        assertEquals(2, m.size());
    }

    @Test
    public void testNullBitmapUsesPhysicalAttnumWhenCatalogHasGap() {
        List<ColumnInfo> cols = Arrays.asList(
                new ColumnInfo("a", 1, PgTypeDecoder.INT4, 4, 'i', false),
                new ColumnInfo("d", 4, PgTypeDecoder.INT4, 4, 'i', false));

        int natts = 4;
        int bitmapLen = (natts + 7) / 8;
        int tHoff = maxAlign(SIZE_OF_HEAP_TUPLE_HEADER + bitmapLen);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, natts);
        u16(o, HEAP_HASNULL);
        o.write(tHoff);
        o.write(0x09);
        for (int i = 0; i < tHoff - SIZE_OF_HEAP_TUPLE_HEADER - bitmapLen; i++) {
            o.write(0);
        }
        u32(o, 7);
        u32(o, 99);

        Map<String, Object> m = HeapTupleDecoder.decode(o.toByteArray(), cols);
        assertEquals(7, m.get("a"));
        assertEquals(99, m.get("d"));
    }

    @Test
    public void testPg15JsonbShortVarlenaFromWalTuple() {
        List<ColumnInfo> cols = Arrays.asList(
                new ColumnInfo("a1", 1, PgTypeDecoder.INT4, 4, 'i', false),
                new ColumnInfo("a2", 2, PgTypeDecoder.JSONB, -1, 'i', false));

        Map<String, Object> objectRow = HeapTupleDecoder.decode(
                hex("020002081800040000000b00000020"), cols);
        assertEquals(4, objectRow.get("a1"));
        assertEquals("{}", objectRow.get("a2"));

        Map<String, Object> arrayRow = HeapTupleDecoder.decode(
                hex("020002081800050000000b00000040"), cols);
        assertEquals(5, arrayRow.get("a1"));
        assertEquals("[]", arrayRow.get("a2"));

        Map<String, Object> numericRow = HeapTupleDecoder.decode(
                hex("0200020818000700000057020000200300008005000000020000000c0000106466646b646b646b736600002800000001800c00330c"), cols);
        assertEquals(7, numericRow.get("a1"));
        assertEquals("{\"dfd\":\"sf\",\"kdkdk\":123123}", numericRow.get("a2"));

        Map<String, Object> nestedArrayRow = HeapTupleDecoder.decode(
                hex("0200020818000c000000a902000020030000800500000002000000350000506466646b646b646b73660000040000400a0000900a00001008000010030000002800000001800c00330c00002000000000807b002000000000807b00313233"), cols);
        assertEquals(12, nestedArrayRow.get("a1"));
        assertEquals("{\"dfd\":\"sf\",\"kdkdk\":[123123,123,123,\"123\"]}", nestedArrayRow.get("a2"));
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
        o.write(0x01);
        o.write(18);
        u32(o, 11);
        u32(o, 7);
        u32(o, 77);
        u32(o, 991);

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
        u32(o, 10);
        u32(o, 5 | ((long) method << 30));
        u32(o, 77);
        u32(o, 991);

        byte[] toasted = new byte[] {
                0x04,
                'a', 'b',
                0x01, 0x02
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
        u32(o, 5 | (2L << 30));
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
        u32(o, ((long) total << 2) | 0x02);
        u32(o, 6 | (1L << 30));
        o.write(compressed, 0, compressed.length);

        Map<String, Object> m = HeapTupleDecoder.decode(o.toByteArray(), cols);

        assertEquals("ababab", m.get("payload"));
    }
}
