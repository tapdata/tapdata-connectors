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
}
