package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.connector.postgres.cdc.NormalRedo;
import io.tapdata.connector.postgres.cdc.NormalRedo.OperationEnum;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;

/**
 * Turns a parsed {@link XLogRecord} from the heap / heap2 resource managers into
 * one or more {@link NormalRedo} DML events, reproducing the WAL framing of
 * {@code heap_xlog_insert/update/delete} and {@code heap_xlog_multi_insert}.
 *
 * <p>UPDATE new-tuples may be prefix/suffix compressed against the old tuple on
 * the same page; full reconstruction is possible only when the full old tuple is
 * also logged (REPLICA IDENTITY FULL). When it is not, the after-image is left
 * null (graceful degradation) while the before-image key is still emitted.</p>
 *
 * @author Jarad
 */
public final class HeapRmgrDecoder {

    private HeapRmgrDecoder() {
    }

    public static List<NormalRedo> decode(XLogRecord rec, RelationInfo rel) {
        if (rel == null) {
            return Collections.emptyList();
        }
        if (rec.rmid == RM_HEAP_ID) {
            switch (rec.heapOp()) {
                case XLOG_HEAP_INSERT:
                    return one(decodeInsert(rec, rel));
                case XLOG_HEAP_DELETE:
                    return one(decodeDelete(rec, rel));
                case XLOG_HEAP_UPDATE:
                case XLOG_HEAP_HOT_UPDATE:
                    return one(decodeUpdate(rec, rel));
                default:
                    return Collections.emptyList();
            }
        }
        if (rec.rmid == RM_HEAP2_ID && rec.heapOp() == XLOG_HEAP2_MULTI_INSERT) {
            return decodeMultiInsert(rec, rel);
        }
        return Collections.emptyList();
    }

    private static NormalRedo decodeInsert(XLogRecord rec, RelationInfo rel) {
        WalByteReader md = new WalByteReader(rec.mainData);
        md.skip(2);                                 // offnum
        int flags = md.readUInt8();
        XLogRecord.BlockRef b0 = rec.block(0);
        if (b0 == null || !b0.hasData || (flags & XLH_INSERT_CONTAINS_NEW_TUPLE) == 0) {
            return null;
        }
        NormalRedo r = base(rec, rel, OperationEnum.INSERT);
        r.setRedoRecord(HeapTupleDecoder.decode(b0.data, rel.columns));
        return r;
    }

    private static NormalRedo decodeDelete(XLogRecord rec, RelationInfo rel) {
        WalByteReader md = new WalByteReader(rec.mainData);
        md.skip(7);                                 // xmax(4)+offnum(2)+infobits(1)
        int flags = md.readUInt8();
        NormalRedo r = base(rec, rel, OperationEnum.DELETE);
        if ((flags & XLH_DELETE_CONTAINS_OLD) != 0 && md.remaining() > 0) {
            r.setUndoRecord(HeapTupleDecoder.decode(md.readBytes(md.remaining()), rel.columns));
        }
        return r;
    }

    private static NormalRedo decodeUpdate(XLogRecord rec, RelationInfo rel) {
        WalByteReader md = new WalByteReader(rec.mainData);
        md.skip(7);                                 // old_xmax(4)+old_offnum(2)+old_infobits(1)
        int flags = md.readUInt8();
        md.skip(6);                                 // new_xmax(4)+new_offnum(2)
        byte[] oldTuple = null;
        if ((flags & XLH_UPDATE_CONTAINS_OLD) != 0 && md.remaining() > 0) {
            oldTuple = md.readBytes(md.remaining());
        }
        NormalRedo r = base(rec, rel, OperationEnum.UPDATE);
        if (oldTuple != null) {
            r.setUndoRecord(HeapTupleDecoder.decode(oldTuple, rel.columns));
        }
        byte[] newTuple = reconstructNew(rec.block(0), flags, oldTuple);
        if (newTuple != null) {
            r.setRedoRecord(HeapTupleDecoder.decode(newTuple, rel.columns));
        }
        return r;
    }

    private static List<NormalRedo> decodeMultiInsert(XLogRecord rec, RelationInfo rel) {
        WalByteReader md = new WalByteReader(rec.mainData);
        md.skip(1);                                 // flags
        md.skip(1);                                 // padding
        int ntuples = md.readUInt16();
        XLogRecord.BlockRef b0 = rec.block(0);
        List<NormalRedo> out = new ArrayList<>();
        if (b0 == null || !b0.hasData) {
            return out;
        }
        WalByteReader r = new WalByteReader(b0.data);
        for (int i = 0; i < ntuples; i++) {
            r.align(2);                             // SHORTALIGN before each tuple header
            int datalen = r.readUInt16();
            int infomask2 = r.readUInt16();
            int infomask = r.readUInt16();
            int tHoff = r.readUInt8();
            byte[] tdata = r.readBytes(datalen);
            NormalRedo nr = base(rec, rel, OperationEnum.INSERT);
            nr.setRedoRecord(HeapTupleDecoder.decode(withHeader(infomask2, infomask, tHoff, tdata), rel.columns));
            out.add(nr);
        }
        return out;
    }

    private static byte[] reconstructNew(XLogRecord.BlockRef b0, int flags, byte[] oldTuple) {
        if (b0 == null || !b0.hasData) {
            return null;
        }
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
            return null;                            // cannot rebuild without full old tuple
        }
        byte[] oldCol = columnData(oldTuple);
        int bpad = tHoff - SIZE_OF_HEAP_TUPLE_HEADER;
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
    }

    private static byte[] columnData(byte[] tuple) {
        int hoff = tuple[4] & 0xFF;                 // t_hoff at byte 4 of xl_heap_header
        int start = SIZE_OF_HEAP_HEADER + (hoff - SIZE_OF_HEAP_TUPLE_HEADER);
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
        return r;
    }

    private static List<NormalRedo> one(NormalRedo r) {
        return r == null ? Collections.emptyList() : Collections.singletonList(r);
    }
}
