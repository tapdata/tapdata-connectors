package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.connector.postgres.cdc.NormalRedo;
import io.tapdata.connector.postgres.cdc.NormalRedo.OperationEnum;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;

/**
 * Turns a parsed {@link XLogRecord} from the heap / heap2 resource managers into
 * one or more {@link NormalRedo} DML events, reproducing the WAL framing of
 * {@code heap_xlog_insert/update/delete} and {@code heap_xlog_multi_insert}.
 *
 * <p>Under {@code wal_level=replica} without {@code REPLICA IDENTITY FULL} the
 * old tuple is not in WAL and UPDATE's new tuple is often prefix/suffix
 * compressed against it; both are recovered by using FPIs as anchors and a
 * {@link PageStateCache} that re-applies subsequent records to keep the page
 * image current. When the source already provides full tuples (RI FULL or
 * {@code wal_level=logical}), pass a null cache or set the bypass flag.</p>
 *
 * @author Jarad
 */
public final class HeapRmgrDecoder {

    private HeapRmgrDecoder() {
    }

    /** Decoding context carrying optional cache and per-record debug callback. */
    public static final class Ctx {
        public final PageStateCache cache;          // null disables tracking
        public final boolean walLevelLogical;       // global bypass when true
        public final BiConsumer<String, Object[]> debug;   // (fmt, args) → log line; may be null

        public Ctx(PageStateCache cache, boolean walLevelLogical, BiConsumer<String, Object[]> debug) {
            this.cache = cache;
            this.walLevelLogical = walLevelLogical;
            this.debug = debug;
        }

        boolean trackingEnabled(RelationInfo rel) {
            return cache != null && !walLevelLogical && !rel.replicaIdentityFull;
        }

        void log(String fmt, Object... args) {
            if (debug != null) {
                debug.accept(fmt, args);
            }
        }
    }

    private static final Ctx NO_CTX = new Ctx(null, false, null);

    /** Backward-compatible entry point used by tests; cache disabled. */
    public static List<NormalRedo> decode(XLogRecord rec, RelationInfo rel) {
        return decode(rec, rel, NO_CTX);
    }

    public static List<NormalRedo> decode(XLogRecord rec, RelationInfo rel, Ctx ctx) {
        if (rel == null) {
            return Collections.emptyList();
        }
        if (rec.rmid == RM_HEAP_ID) {
            switch (rec.heapOp()) {
                case XLOG_HEAP_INSERT:
                    return one(decodeInsert(rec, rel, ctx));
                case XLOG_HEAP_DELETE:
                    return one(decodeDelete(rec, rel, ctx));
                case XLOG_HEAP_UPDATE:
                case XLOG_HEAP_HOT_UPDATE:
                    return one(decodeUpdate(rec, rel, ctx));
                default:
                    return Collections.emptyList();
            }
        }
        if (rec.rmid == RM_HEAP2_ID && rec.heapOp() == XLOG_HEAP2_MULTI_INSERT) {
            return decodeMultiInsert(rec, rel, ctx);
        }
        return Collections.emptyList();
    }

    private static NormalRedo decodeInsert(XLogRecord rec, RelationInfo rel, Ctx ctx) {
        XLogRecord.BlockRef b0 = rec.block(0);
        WalByteReader md = new WalByteReader(rec.mainData);
        int offnum = md.readUInt16();
        boolean initPage = (rec.info & XLOG_HEAP_INIT_PAGE) != 0;

        CachedPage cp = primePage(ctx, rel, b0, initPage);
        byte[] tuple = null;
        if (b0 != null && b0.hasData && b0.data.length > 0) {
            tuple = b0.data;
        } else if (cp != null) {
            // No inline tuple (omitted by PG when an FPI is taken): the FPI-seeded
            // overlay already holds the inserted tuple at offnum.
            tuple = cp.get(offnum);
        } else if (b0 != null && b0.hasImage) {
            tuple = PageImageExtractor.extractWalTuple(b0, offnum);
        }
        ctx.log("[WAL-DEBUG] INSERT rel={} blk={} offnum={} initPage={} hasImage={} hasData={} dataLen={} imgLen={} mainData={} tuple={}",
                new Object[]{relTag(rel), blockNumber(b0), offnum, initPage,
                        b0 != null && b0.hasImage, b0 != null && b0.hasData,
                        b0 == null ? 0 : b0.data.length, b0 == null ? 0 : b0.image.length,
                        hex(rec.mainData), hex(tuple)});
        if (tuple == null) {
            return null;
        }
        // Idempotent on an FPI-seeded overlay (the same tuple is already there),
        // so no "already applied" guard is needed.
        if (cp != null) {
            cp.put(offnum, tuple);
        }
        NormalRedo r = base(rec, rel, OperationEnum.INSERT);
        r.setRedoRecord(HeapTupleDecoder.decode(tuple, rel.columns));
        return r;
    }

    private static NormalRedo decodeDelete(XLogRecord rec, RelationInfo rel, Ctx ctx) {
        WalByteReader md = new WalByteReader(rec.mainData);
        md.skip(4);                                 // xmax (not needed by the overlay)
        int offnum = md.readUInt16();
        md.skip(1);                                 // infobits
        int flags = md.readUInt8();
        byte[] oldTuple = null;
        if ((flags & XLH_DELETE_CONTAINS_OLD) != 0 && md.remaining() > 0) {
            oldTuple = md.readBytes(md.remaining());
        }
        XLogRecord.BlockRef b0 = rec.block(0);
        CachedPage cp = primePage(ctx, rel, b0, false);
        if (oldTuple == null && cp != null) {
            // The deleted tuple keeps its column bytes (only t_xmax is stamped)
            // and stays LP_NORMAL until vacuum, so the before-image is still
            // readable from the overlay at offnum.
            oldTuple = cp.get(offnum);
        } else if (oldTuple == null && b0 != null && b0.hasImage) {
            oldTuple = PageImageExtractor.extractWalTuple(b0, offnum);
        }
        ctx.log("[WAL-DEBUG] DELETE rel={} blk={} offnum={} flags=0x{} hasImage={} cacheHit={} mainData={} oldTuple={}",
                new Object[]{relTag(rel), blockNumber(b0), offnum, Integer.toHexString(flags),
                        b0 != null && b0.hasImage, cp != null && (b0 == null || !b0.hasImage),
                        hex(rec.mainData), hex(oldTuple)});
        // Keep the tuple bytes in the overlay. DELETE WAL is decoded before the
        // commit/abort gate, and a later ROLLBACK TO SAVEPOINT can make the row
        // visible again. Slot reuse is still safe because INSERT/UPDATE-new
        // overwrites the offset before any later record should read it.
        NormalRedo r = base(rec, rel, OperationEnum.DELETE);
        if (oldTuple != null) {
            r.setUndoRecord(HeapTupleDecoder.decode(oldTuple, rel.columns));
        }
        return r;
    }

    private static NormalRedo decodeUpdate(XLogRecord rec, RelationInfo rel, Ctx ctx) {
        WalByteReader md = new WalByteReader(rec.mainData);
        md.skip(4);                                 // old_xmax (not needed by the overlay)
        int oldOffnum = md.readUInt16();
        md.skip(1);                                 // old_infobits
        int flags = md.readUInt8();
        md.skip(4);                                 // new_xmax
        int newOffnum = md.readUInt16();
        byte[] oldTuple = null;
        if ((flags & XLH_UPDATE_CONTAINS_OLD) != 0 && md.remaining() > 0) {
            oldTuple = md.readBytes(md.remaining());
        }
        XLogRecord.BlockRef b0 = rec.block(0);      // newbuf
        XLogRecord.BlockRef b1 = rec.block(1);      // oldbuf (only when cross-page)

        CachedPage newCp = primePage(ctx, rel, b0, (rec.info & XLOG_HEAP_INIT_PAGE) != 0);
        CachedPage oldCp = b1 != null ? primePage(ctx, rel, b1, false) : newCp;
        if (oldTuple == null && oldCp != null) {
            oldTuple = oldCp.get(oldOffnum);
        } else if (oldTuple == null) {
            XLogRecord.BlockRef oldRef = (b1 != null && b1.hasImage) ? b1 : b0;
            oldTuple = PageImageExtractor.extractWalTuple(oldRef, oldOffnum);
        }
        // The new tuple is normally a prefix/suffix delta in b0.data. Under
        // wal_level=replica PostgreSQL omits it whenever an FPI of the new page
        // was taken (XLH_UPDATE_CONTAINS_NEW_TUPLE is only set for logical), so
        // when the delta is absent fall back to the FPI: the image is copied
        // after the page is modified, hence new_offnum already holds the new
        // tuple in the freshly seeded overlay.
        byte[] newTuple = reconstructNew(b0, flags, oldTuple);
        if (newTuple == null) {
            if (newCp != null) {
                newTuple = newCp.get(newOffnum);
            } else if (b0 != null && b0.hasImage) {
                newTuple = PageImageExtractor.extractWalTuple(b0, newOffnum);
            }
        }
        ctx.log("[WAL-DEBUG] UPDATE rel={} oldBlk={} newBlk={} oldOff={} newOff={} flags=0x{} hasImage0={} hasImage1={} oldCp={} newCp={} mainData={} oldTuple={} newTuple={}",
                new Object[]{relTag(rel), blockNumber(b1 != null ? b1 : b0), blockNumber(b0),
                        oldOffnum, newOffnum, Integer.toHexString(flags),
                        b0 != null && b0.hasImage, b1 != null && b1.hasImage,
                        cpTag(oldCp), cpTag(newCp),
                        hex(rec.mainData), hex(oldTuple), hex(newTuple)});
        // Keep oldOffnum in the overlay rather than evicting it: the old row
        // version retains its column bytes and stays LP_NORMAL until a
        // prune/vacuum, so a second UPDATE that legitimately re-reads the same
        // old offset still resolves. That occurs when the first updating txn
        // aborts — its heap_update WAL is written regardless, the tuple stays
        // live at oldOffnum, and the next committed update again deltas from it;
        // evicting here stranded that update with a null before/after image.
        // Overlay mutations run at decode time (ahead of the commit gate), so an
        // eviction cannot be rolled back for the aborted record. A later prune
        // FPI reseeds the overlay and clears the now-dead version; slot reuse is
        // safe because INSERT/UPDATE-new puts the new tuple before any record
        // reads that offset. Installing the new version is idempotent.
        if (newCp != null && newTuple != null) {
            newCp.put(newOffnum, newTuple);
        }
        NormalRedo r = base(rec, rel, OperationEnum.UPDATE);
        if (oldTuple != null) {
            r.setUndoRecord(HeapTupleDecoder.decode(oldTuple, rel.columns));
        }
        if (newTuple != null) {
            r.setRedoRecord(HeapTupleDecoder.decode(newTuple, rel.columns));
        }
        return r;
    }

    private static List<NormalRedo> decodeMultiInsert(XLogRecord rec, RelationInfo rel, Ctx ctx) {
        WalByteReader md = new WalByteReader(rec.mainData);
        md.skip(1);                                 // flags
        md.skip(1);                                 // padding
        int ntuples = md.readUInt16();
        int[] offnums = new int[ntuples];
        for (int i = 0; i < ntuples && md.remaining() >= 2; i++) {
            offnums[i] = md.readUInt16();
        }
        XLogRecord.BlockRef b0 = rec.block(0);
        List<NormalRedo> out = new ArrayList<>();
        CachedPage cp = primePage(ctx, rel, b0, (rec.info & XLOG_HEAP_INIT_PAGE) != 0);
        byte[][] tuples = new byte[ntuples][];
        if (b0 != null && b0.hasData && b0.data.length > 0) {
            WalByteReader r = new WalByteReader(b0.data);
            for (int i = 0; i < ntuples; i++) {
                r.align(2);                         // SHORTALIGN before each tuple header
                int datalen = r.readUInt16();
                int infomask2 = r.readUInt16();
                int infomask = r.readUInt16();
                int tHoff = r.readUInt8();
                byte[] tdata = r.readBytes(datalen);
                tuples[i] = withHeader(infomask2, infomask, tHoff, tdata);
            }
        } else if (cp != null) {
            // Inline tuple data omitted by PG when an FPI was taken: the
            // FPI-seeded overlay already holds every inserted tuple.
            for (int i = 0; i < ntuples; i++) {
                tuples[i] = cp.get(offnums[i]);
            }
        } else {
            return out;
        }
        for (int i = 0; i < ntuples; i++) {
            if (tuples[i] == null) {
                continue;
            }
            NormalRedo nr = base(rec, rel, OperationEnum.INSERT);
            nr.setRedoRecord(HeapTupleDecoder.decode(tuples[i], rel.columns));
            out.add(nr);
        }
        ctx.log("[WAL-DEBUG] MULTI_INSERT rel={} blk={} ntuples={} offnums={} dataLen={}",
                new Object[]{relTag(rel), blockNumber(b0), ntuples, Arrays.toString(offnums),
                        b0 == null ? 0 : b0.data.length});
        if (cp != null) {
            for (int i = 0; i < ntuples; i++) {
                if (tuples[i] != null) {
                    cp.put(offnums[i], tuples[i]);
                }
            }
        }
        return out;
    }

    private static byte[] reconstructNew(XLogRecord.BlockRef b0, int flags, byte[] oldTuple) {
        if (b0 == null || !b0.hasData || b0.data == null) {
            return null;
        }
        try {
            WalByteReader r = new WalByteReader(b0.data);
            int prefixlen = (flags & XLH_UPDATE_PREFIX_FROM_OLD) != 0 ? r.readUInt16() : 0;
            int suffixlen = (flags & XLH_UPDATE_SUFFIX_FROM_OLD) != 0 ? r.readUInt16() : 0;
            int infomask2 = r.readUInt16();
            int infomask = r.readUInt16();
            int tHoff = r.readUInt8();
            byte[] rest = r.readBytes(r.remaining());
            if (prefixlen == 0 && suffixlen == 0) {
                return withHeader(infomask2, infomask, tHoff, rest);
            }
            if (oldTuple == null) {
                return null;                        // cannot rebuild without full old tuple
            }
            byte[] oldCol = columnData(oldTuple);
            int bpad = tHoff - SIZE_OF_HEAP_TUPLE_HEADER;
            if (bpad < 0 || bpad > rest.length
                    || prefixlen > oldCol.length
                    || suffixlen > oldCol.length - prefixlen) {
                return null;
            }
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            if (prefixlen > 0) {
                data.write(rest, 0, bpad);
                data.write(oldCol, 0, prefixlen);
                data.write(rest, bpad, rest.length - bpad);
            } else {
                data.write(rest, 0, rest.length);
            }
            if (suffixlen > 0) {
                data.write(oldCol, oldCol.length - suffixlen, suffixlen);
            }
            return withHeader(infomask2, infomask, tHoff, data.toByteArray());
        } catch (RuntimeException ignore) {
            return null;
        }
    }

    private static byte[] columnData(byte[] tuple) {
        if (tuple == null || tuple.length <= SIZE_OF_HEAP_HEADER) {
            return new byte[0];
        }
        int hoff = tuple[4] & 0xFF;                 // t_hoff at byte 4 of xl_heap_header
        int start = SIZE_OF_HEAP_HEADER + (hoff - SIZE_OF_HEAP_TUPLE_HEADER);
        if (start < SIZE_OF_HEAP_HEADER || start > tuple.length) {
            return new byte[0];
        }
        return Arrays.copyOfRange(tuple, start, tuple.length);
    }

    private static byte[] withHeader(int infomask2, int infomask, int tHoff, byte[] dataArea) {
        byte[] out = new byte[SIZE_OF_HEAP_HEADER + dataArea.length];
        out[0] = (byte) infomask2;
        out[1] = (byte) (infomask2 >> 8);
        out[2] = (byte) infomask;
        out[3] = (byte) (infomask >> 8);
        out[4] = (byte) tHoff;
        System.arraycopy(dataArea, 0, out, SIZE_OF_HEAP_HEADER, dataArea.length);
        return out;
    }

    private static NormalRedo base(XLogRecord rec, RelationInfo rel, OperationEnum op) {
        NormalRedo r = new NormalRedo();
        r.setOperation(op.name());
        r.setNameSpace(rel.schema);
        r.setTableName(rel.table);
        r.setTransactionId(String.valueOf(rec.xid));
        r.setCdcSequenceId(rec.lsn);
        r.setSourceXid(rec.xid);
        return r;
    }

    private static List<NormalRedo> one(NormalRedo r) {
        return r == null ? Collections.emptyList() : Collections.singletonList(r);
    }

    /* ---------- cache helpers ---------- */

    /**
     * Prime the {@link CachedPage} overlay for (b.relNumber, b.blockNumber):
     * <ul>
     *   <li>FPI present -> reconstruct the page and reseed the overlay with every
     *       live tuple; this is the authoritative post-modification state, and
     *       because writes are keyed by offset, the caller can re-store this
     *       record's own tuple idempotently;</li>
     *   <li>WILL_INIT (a record that re-initialises the page in place) -> clear
     *       the overlay before the record is applied;</li>
     *   <li>otherwise return whatever overlay the cache currently holds (may be
     *       null when the page has not been seen since miner start, in which case
     *       before-image recovery falls back to the FPI extractor).</li>
     * </ul>
     */
    private static CachedPage primePage(Ctx ctx, RelationInfo rel, XLogRecord.BlockRef b, boolean initFlag) {
        if (b == null || !ctx.trackingEnabled(rel)) {
            return null;
        }
        if (b.hasImage) {
            byte[] page = PageImageExtractor.reconstructPage(b);
            if (page != null) {
                CachedPage cp = ctx.cache.getOrCreate(b.relNumber, b.blockNumber);
                if (cp.seedFromImage(page)) {
                    ctx.log("[WAL-DEBUG] SEED rel={} blk={} offsets={} (FPI)",
                            new Object[]{relTag(rel), b.blockNumber, cp.size()});
                } else {
                    ctx.log("[WAL-DEBUG] SEED-SKIP rel={} blk={} (empty/will-init FPI; kept {} tuples)",
                            new Object[]{relTag(rel), b.blockNumber, cp.size()});
                }
                return cp;
            }
            // Image present but not reconstructable (compressed / unsupported): the
            // page silently stays un-seeded, which strands every later FPI-less
            // UPDATE/DELETE on it with a null before-image. Surface it explicitly.
            ctx.log("[WAL-DEBUG] SEED-FAIL rel={} blk={} bimgInfo=0x{} (image present but not reconstructable; compressed?)",
                    new Object[]{relTag(rel), b.blockNumber, Integer.toHexString(b.bimgInfo)});
        }
        boolean willInit = initFlag || (b.forkFlags & BKPBLOCK_WILL_INIT) != 0;
        if (willInit) {
            CachedPage cp = ctx.cache.getOrCreate(b.relNumber, b.blockNumber);
            cp.clear();
            return cp;
        }
        return ctx.cache.get(b.relNumber, b.blockNumber);
    }

    /* ---------- debug helpers ---------- */

    private static String relTag(RelationInfo rel) {
        return rel == null ? "?" : rel.schema + "." + rel.table;
    }

    private static long blockNumber(XLogRecord.BlockRef b) {
        return b == null ? -1L : b.blockNumber;
    }

    /* Overlay state at decode time: "null" when the page has never been seen
     * (no FPI ever seeded it), otherwise the live offset count. Distinguishes a
     * missing seed from a seeded-but-stale-offset miss. */
    private static String cpTag(CachedPage cp) {
        return cp == null ? "null" : ("size=" + cp.size());
    }

    private static String hex(byte[] data) {
        if (data == null) {
            return "null";
        }
        // Dump the full byte array (no truncation): the [WAL-DEBUG] trace is used
        // to compare oldTuple/newTuple byte-for-byte against the source, so a
        // clipped image hides exactly the bytes being diagnosed.
        StringBuilder sb = new StringBuilder(2 + data.length * 2);
        sb.append('[').append(data.length).append("] ");
        for (byte b : data) {
            int v = b & 0xFF;
            if (v < 0x10) sb.append('0');
            sb.append(Integer.toHexString(v));
        }
        return sb.toString();
    }
}
