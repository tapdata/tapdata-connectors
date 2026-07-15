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
            Collections.singletonList("a"), false);

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

    /* on-disk HeapTupleHeaderData (23 bytes) + data area, the shape stored on a
     * real heap page and reconstructed from a full-page image. */
    private static byte[] onDiskTuple(int a, String text) {
        byte[] da = dataArea(a, text);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (int i = 0; i < 18; i++) {
            o.write(0);                             // t_choice(12) + t_ctid(6)
        }
        u16(o, 2);                                  // t_infomask2 = natts
        u16(o, 0);                                  // t_infomask
        o.write(maxAlign(SIZE_OF_HEAP_TUPLE_HEADER));   // t_hoff = 24
        o.write(da, 0, da.length);
        return o.toByteArray();
    }

    private static void p16(byte[] page, int off, int v) {
        page[off] = (byte) v;
        page[off + 1] = (byte) (v >> 8);
    }

    private static void p32(byte[] page, int off, int v) {
        for (int i = 0; i < 4; i++) {
            page[off + i] = (byte) (v >>> (8 * i));
        }
    }

    /* Build an 8KB heap page holding the given on-disk tuples at 1-based offsets,
     * mirroring the bufpage.h layout PageImageExtractor reads back. */
    private static byte[] heapPage(int[] offnums, byte[][] tuples) {
        byte[] page = new byte[XLOG_BLCKSZ];
        int maxOff = 0;
        for (int o : offnums) {
            maxOff = Math.max(maxOff, o);
        }
        int upper = XLOG_BLCKSZ;
        for (int i = 0; i < offnums.length; i++) {
            byte[] t = tuples[i];
            upper -= maxAlign(t.length);
            System.arraycopy(t, 0, page, upper, t.length);
            int word = (upper & 0x7FFF) | (1 << 15) | ((t.length & 0x7FFF) << 17);
            p32(page, 24 + (offnums[i] - 1) * 4, word);   // ItemId for offnum
        }
        p16(page, 12, 24 + maxOff * 4);             // pd_lower
        p16(page, 14, upper);                       // pd_upper
        return page;
    }

    private static XLogRecord.BlockRef imageBlock0(byte[] page) {
        XLogRecord.BlockRef b = new XLogRecord.BlockRef();
        b.id = 0;
        b.relNumber = 16384;
        b.hasImage = true;
        b.image = page;
        b.bimgInfo = 0;
        return b;
    }

    /**
     * wal_level=replica regression: an UPDATE that took a full-page image of the
     * new page carries no inline new tuple (XLH_UPDATE_CONTAINS_NEW_TUPLE is set
     * only under wal_level=logical). Both before- and after-images must still be
     * recovered from the post-modification image rather than coming back null.
     */
    @Test
    public void testUpdateRecoversBothImagesFromFpiUnderReplica() {
        byte[] page = heapPage(new int[]{1, 2},
                new byte[][]{onDiskTuple(1, "aa"), onDiskTuple(2, "bb")});

        XLogRecord rec = new XLogRecord();
        rec.rmid = RM_HEAP_ID;
        rec.info = XLOG_HEAP_UPDATE;
        rec.xid = 20;
        ByteArrayOutputStream md = new ByteArrayOutputStream();
        u32(md, 0);                                 // old_xmax
        u16(md, 1);                                 // old_offnum
        md.write(0);                                // old_infobits
        md.write(0);                                // flags: no OLD, no prefix/suffix, no NEW
        u32(md, 0);                                 // new_xmax
        u16(md, 2);                                 // new_offnum
        rec.mainData = md.toByteArray();
        rec.blocks.add(imageBlock0(page));          // FPI only, no block data

        HeapRmgrDecoder.Ctx ctx = new HeapRmgrDecoder.Ctx(new PageStateCache(), false, null);
        List<NormalRedo> events = HeapRmgrDecoder.decode(rec, REL, ctx);
        assertEquals(1, events.size());
        NormalRedo r = events.get(0);
        assertEquals("UPDATE", r.getOperation());
        assertNotNull(r.getUndoRecord());
        assertNotNull(r.getRedoRecord());
        assertEquals(1, r.getUndoRecord().get("a"));
        assertEquals("aa", r.getUndoRecord().get("b"));
        assertEquals(2, r.getRedoRecord().get("a"));
        assertEquals("bb", r.getRedoRecord().get("b"));
    }

    @Test
    public void testLogicalUpdateDoesNotUseFpiAsOldImageFallback() {
        byte[] page = heapPage(new int[]{1, 2},
                new byte[][]{onDiskTuple(1, "stale"), onDiskTuple(2, "new")});

        XLogRecord rec = new XLogRecord();
        rec.rmid = RM_HEAP_ID;
        rec.info = XLOG_HEAP_UPDATE;
        rec.xid = 21;
        ByteArrayOutputStream md = new ByteArrayOutputStream();
        u32(md, 0);                                 // old_xmax
        u16(md, 1);                                 // old_offnum
        md.write(0);                                // old_infobits
        md.write(0);                                // flags: no OLD tuple/key
        u32(md, 0);                                 // new_xmax
        u16(md, 2);                                 // new_offnum
        rec.mainData = md.toByteArray();
        rec.blocks.add(imageBlock0(page));          // FPI is post-image, not a logical old-image source

        HeapRmgrDecoder.Ctx ctx = new HeapRmgrDecoder.Ctx(null, true, null);
        List<NormalRedo> events = HeapRmgrDecoder.decode(rec, REL, ctx);
        assertEquals(1, events.size());
        NormalRedo r = events.get(0);
        assertEquals("UPDATE", r.getOperation());
        assertNull(r.getUndoRecord());
        assertNotNull(r.getRedoRecord());
        assertEquals(2, r.getRedoRecord().get("a"));
        assertEquals("new", r.getRedoRecord().get("b"));
    }

    /**
     * Overlay persistence regression: a hot page that took an FPI on one UPDATE
     * must keep serving before-images to later FPI-less UPDATEs on the same
     * page. This is the property a HOT-prune (XLOG_HEAP2_PRUNE) must NOT disturb
     * — with the logical overlay it is a no-op, so the cached tuple at the live
     * offset survives and the second UPDATE recovers its old image from cache
     * instead of coming back null (the {@code bmsql_district} failure mode).
     */
    @Test
    public void testUpdateRecoversOldImageFromCacheAcrossMaintenance() {
        PageStateCache cache = new PageStateCache();
        HeapRmgrDecoder.Ctx ctx = new HeapRmgrDecoder.Ctx(cache, false, null);

        // rec1: FPI-bearing UPDATE seeds the overlay; the live tuple is at offnum 2.
        byte[] page = heapPage(new int[]{1, 2},
                new byte[][]{onDiskTuple(1, "aa"), onDiskTuple(2, "bb")});
        XLogRecord rec1 = new XLogRecord();
        rec1.rmid = RM_HEAP_ID;
        rec1.info = XLOG_HEAP_UPDATE;
        rec1.xid = 30;
        ByteArrayOutputStream md1 = new ByteArrayOutputStream();
        u32(md1, 0);                                // old_xmax
        u16(md1, 1);                                // old_offnum
        md1.write(0);                               // old_infobits
        md1.write(0);                               // flags: FPI only
        u32(md1, 0);                                // new_xmax
        u16(md1, 2);                                // new_offnum
        rec1.mainData = md1.toByteArray();
        rec1.blocks.add(imageBlock0(page));
        HeapRmgrDecoder.decode(rec1, REL, ctx);

        // (A HOT prune lands here in production; invalidateForMaintenance is a
        //  no-op, so the overlay seeded above is exactly what survives.)

        // rec2: FPI-less UPDATE of the live tuple at offnum 2 -> 3, full new
        // tuple inline, no old tuple. Before-image must come from the overlay.
        XLogRecord rec2 = new XLogRecord();
        rec2.rmid = RM_HEAP_ID;
        rec2.info = XLOG_HEAP_UPDATE;
        rec2.xid = 31;
        ByteArrayOutputStream md2 = new ByteArrayOutputStream();
        u32(md2, 0);                                // old_xmax
        u16(md2, 2);                                // old_offnum (the live tuple)
        md2.write(0);                               // old_infobits
        md2.write(0);                               // flags: no OLD, no prefix/suffix
        u32(md2, 0);                                // new_xmax
        u16(md2, 3);                                // new_offnum
        rec2.mainData = md2.toByteArray();
        rec2.blocks.add(block0(tuple(3, "cc")));    // full new tuple, no FPI

        List<NormalRedo> events = HeapRmgrDecoder.decode(rec2, REL, ctx);
        assertEquals(1, events.size());
        NormalRedo r = events.get(0);
        assertEquals("UPDATE", r.getOperation());
        assertNotNull(r.getUndoRecord());
        assertEquals(2, r.getUndoRecord().get("a"));
        assertEquals("bb", r.getUndoRecord().get("b"));
        assertEquals(3, r.getRedoRecord().get("a"));
        assertEquals("cc", r.getRedoRecord().get("b"));
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
    public void testDeleteKeepsOverlayForPossibleSubtransactionRollback() {
        PageStateCache cache = new PageStateCache();
        HeapRmgrDecoder.Ctx ctx = new HeapRmgrDecoder.Ctx(cache, false, null);

        byte[] page = heapPage(new int[]{1}, new byte[][]{onDiskTuple(4, "dd")});
        XLogRecord seed = new XLogRecord();
        seed.rmid = RM_HEAP_ID;
        seed.info = XLOG_HEAP_INSERT;
        seed.xid = 40;
        ByteArrayOutputStream seedMd = new ByteArrayOutputStream();
        u16(seedMd, 1);
        seed.mainData = seedMd.toByteArray();
        seed.blocks.add(imageBlock0(page));
        HeapRmgrDecoder.decode(seed, REL, ctx);

        XLogRecord rolledBackDelete = deleteWithoutOldTuple(41, 1);
        List<NormalRedo> first = HeapRmgrDecoder.decode(rolledBackDelete, REL, ctx);
        assertEquals(1, first.size());
        assertEquals(4, first.get(0).getUndoRecord().get("a"));

        XLogRecord committedDelete = deleteWithoutOldTuple(42, 1);
        List<NormalRedo> second = HeapRmgrDecoder.decode(committedDelete, REL, ctx);
        assertEquals(1, second.size());
        assertNotNull(second.get(0).getUndoRecord());
        assertEquals(4, second.get(0).getUndoRecord().get("a"));
        assertEquals("dd", second.get(0).getUndoRecord().get("b"));
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

    private static XLogRecord deleteWithoutOldTuple(long xid, int offnum) {
        XLogRecord rec = new XLogRecord();
        rec.rmid = RM_HEAP_ID;
        rec.info = XLOG_HEAP_DELETE;
        rec.xid = xid;
        ByteArrayOutputStream md = new ByteArrayOutputStream();
        u32(md, 0);
        u16(md, offnum);
        md.write(0);
        md.write(0);
        rec.mainData = md.toByteArray();
        XLogRecord.BlockRef b = new XLogRecord.BlockRef();
        b.id = 0;
        b.relNumber = 16384;
        rec.blocks.add(b);
        return rec;
    }
}
