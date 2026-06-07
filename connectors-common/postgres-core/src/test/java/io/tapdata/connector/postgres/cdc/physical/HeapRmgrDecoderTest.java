package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.connector.postgres.cdc.NormalRedo;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class HeapRmgrDecoderTest {

    private static final RelationInfo REL = new RelationInfo("public", "t", Arrays.asList(
            new ColumnInfo("a", 1, PgTypeDecoder.INT4, 4, 'i', false),
            new ColumnInfo("b", 2, PgTypeDecoder.TEXT, -1, 'i', false)),
            Collections.singletonList("a"));

    private static void u16(ByteArrayOutputStream o, int v) {
        o.write(v & 0xFF);
        o.write((v >> 8) & 0xFF);
    }

    private static void u32(ByteArrayOutputStream o, long v) {
        for (int i = 0; i < 4; i++) {
            o.write((int) ((v >> (8 * i)) & 0xFF));
        }
    }

    /** the tuple data area (reconstructed offset 23 onward): pad + int4 + short text. */
    private static byte[] dataArea(int a, String text) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(0);                                 // pad to t_hoff(24)
        u32(o, a);
        byte[] t = text.getBytes(StandardCharsets.US_ASCII);
        o.write(((t.length + 1) << 1) | 1);         // short varlena header
        o.write(t, 0, t.length);
        return o.toByteArray();
    }

    private static byte[] tuple(int a, String text) {
        byte[] da = dataArea(a, text);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        u16(o, 2);                                  // t_infomask2 = natts
        u16(o, 0);                                  // t_infomask
        o.write(maxAlign(SIZE_OF_HEAP_TUPLE_HEADER));   // t_hoff = 24
        o.write(da, 0, da.length);
        return o.toByteArray();
    }

    private static XLogRecord.BlockRef block0(byte[] data) {
        XLogRecord.BlockRef b = new XLogRecord.BlockRef();
        b.id = 0;
        b.relNumber = 16384;
        b.hasData = true;
        b.data = data;
        return b;
    }

    @Test
    public void testInsert() {
        XLogRecord rec = new XLogRecord();
        rec.rmid = RM_HEAP_ID;
        rec.info = XLOG_HEAP_INSERT;
        rec.xid = 5;
        ByteArrayOutputStream md = new ByteArrayOutputStream();
        u16(md, 1);                                 // offnum
        md.write(XLH_INSERT_CONTAINS_NEW_TUPLE);    // flags
        rec.mainData = md.toByteArray();
        rec.blocks.add(block0(tuple(42, "hi")));

        List<NormalRedo> events = HeapRmgrDecoder.decode(rec, REL);
        assertEquals(1, events.size());
        assertEquals("INSERT", events.get(0).getOperation());
        assertEquals(42, events.get(0).getRedoRecord().get("a"));
        assertEquals("hi", events.get(0).getRedoRecord().get("b"));
    }

    @Test
    public void testUpdateWithFullOldTuple() {
        XLogRecord rec = new XLogRecord();
        rec.rmid = RM_HEAP_ID;
        rec.info = XLOG_HEAP_UPDATE;
        rec.xid = 9;
        ByteArrayOutputStream md = new ByteArrayOutputStream();
        u32(md, 0);                                 // old_xmax
        u16(md, 1);                                 // old_offnum
        md.write(0);                                // old_infobits
        md.write(XLH_UPDATE_CONTAINS_OLD_TUPLE);    // flags
        u32(md, 0);                                 // new_xmax
        u16(md, 2);                                 // new_offnum
        byte[] oldTuple = tuple(1, "aa");
        md.write(oldTuple, 0, oldTuple.length);
        rec.mainData = md.toByteArray();
        rec.blocks.add(block0(tuple(2, "bb")));     // no prefix/suffix flags

        List<NormalRedo> events = HeapRmgrDecoder.decode(rec, REL);
        assertEquals(1, events.size());
        NormalRedo r = events.get(0);
        assertEquals("UPDATE", r.getOperation());
        assertEquals(1, r.getUndoRecord().get("a"));
        assertEquals("aa", r.getUndoRecord().get("b"));
        assertEquals(2, r.getRedoRecord().get("a"));
        assertEquals("bb", r.getRedoRecord().get("b"));
    }

    @Test
    public void testMultiInsert() {
        XLogRecord rec = new XLogRecord();
        rec.rmid = RM_HEAP2_ID;
        rec.info = XLOG_HEAP2_MULTI_INSERT;
        rec.xid = 11;
        ByteArrayOutputStream md = new ByteArrayOutputStream();
        md.write(0);                                // flags
        md.write(0);                                // padding
        u16(md, 2);                                 // ntuples
        rec.mainData = md.toByteArray();

        ByteArrayOutputStream bd = new ByteArrayOutputStream();
        for (Object[] t : new Object[][]{{7, "x"}, {8, "yy"}}) {
            byte[] da = dataArea((int) t[0], (String) t[1]);
            u16(bd, da.length);                     // datalen
            u16(bd, 2);                             // t_infomask2
            u16(bd, 0);                             // t_infomask
            bd.write(maxAlign(SIZE_OF_HEAP_TUPLE_HEADER));  // t_hoff
            bd.write(da, 0, da.length);
            if ((bd.size() & 1) == 1) {
                bd.write(0);                        // keep next tuple SHORTALIGN'd
            }
        }
        rec.blocks.add(block0(bd.toByteArray()));

        List<NormalRedo> events = HeapRmgrDecoder.decode(rec, REL);
        assertEquals(2, events.size());
        assertEquals(7, events.get(0).getRedoRecord().get("a"));
        assertEquals("x", events.get(0).getRedoRecord().get("b"));
        assertEquals(8, events.get(1).getRedoRecord().get("a"));
        assertEquals("yy", events.get(1).getRedoRecord().get("b"));
    }
}
