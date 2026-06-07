package io.tapdata.connector.postgres.cdc.physical;

import java.util.ArrayList;
import java.util.List;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;

/**
 * A decoded XLOG record: the fixed header fields plus the parsed block
 * references (each with its optional full-page image and rmgr block data) and
 * the trailing main data. The parsing faithfully reproduces PostgreSQL's
 * {@code DecodeXLogRecord} (src/backend/access/transam/xlogreader.c).
 *
 * @author Jarad
 */
public class XLogRecord {

    public long lsn;
    public long nextLsn;
    public long totLen;
    public long xid;
    public long topXid;
    public long prev;
    public int info;
    public int rmid;
    public final List<BlockRef> blocks = new ArrayList<>();
    public byte[] mainData = new byte[0];

    public int heapOp() {
        return info & XLOG_HEAP_OPMASK;
    }

    public BlockRef block(int id) {
        for (BlockRef b : blocks) {
            if (b.id == id) {
                return b;
            }
        }
        return null;
    }

    /** A single registered block reference within a record. */
    public static class BlockRef {
        public int id;
        public int forkFlags;
        public int dataLength;
        public boolean hasImage;
        public boolean hasData;
        public long spcOid;
        public long dbOid;
        public long relNumber;
        public long blockNumber;
        public byte[] data = new byte[0];
        public byte[] image = new byte[0];
    }

    public static XLogRecord parse(WalPageDecoder.RawRecord raw) {
        XLogRecord rec = new XLogRecord();
        rec.lsn = raw.lsn;
        rec.nextLsn = raw.nextLsn;
        WalByteReader r = new WalByteReader(raw.bytes);
        rec.totLen = r.readUInt32();
        rec.xid = r.readUInt32();
        rec.prev = r.readInt64();
        rec.info = r.readUInt8();
        rec.rmid = r.readUInt8();
        r.skip(2);                 // padding
        r.readUInt32();            // crc (not validated)

        long remaining = rec.totLen - SIZE_OF_XLOG_RECORD;
        long datatotal = 0;
        int mainDataLen = 0;
        long lastSpc = 0, lastDb = 0, lastRel = 0;
        boolean haveRel = false;

        while (remaining > datatotal) {
            int blockId = r.readUInt8();
            remaining -= 1;
            if (blockId == XLR_BLOCK_ID_DATA_SHORT) {
                mainDataLen = r.readUInt8();
                remaining -= 1;
                datatotal += mainDataLen;
            } else if (blockId == XLR_BLOCK_ID_DATA_LONG) {
                mainDataLen = (int) r.readUInt32();
                remaining -= 4;
                datatotal += mainDataLen;
            } else if (blockId == XLR_BLOCK_ID_ORIGIN) {
                r.skip(2);
                remaining -= 2;
            } else if (blockId == XLR_BLOCK_ID_TOPLEVEL_XID) {
                rec.topXid = r.readUInt32();   // top-level xid of a subtransaction record
                remaining -= 4;
            } else if (blockId <= XLR_MAX_BLOCK_ID) {
                BlockRef blk = new BlockRef();
                blk.id = blockId;
                blk.forkFlags = r.readUInt8();
                blk.dataLength = r.readUInt16();
                remaining -= 3;
                blk.hasImage = (blk.forkFlags & BKPBLOCK_HAS_IMAGE) != 0;
                blk.hasData = (blk.forkFlags & BKPBLOCK_HAS_DATA) != 0;
                datatotal += blk.dataLength;
                int imageBytes = 0;
                if (blk.hasImage) {
                    imageBytes = r.readUInt16();   // bimg length
                    r.readUInt16();                // hole_offset
                    int bimgInfo = r.readUInt8();
                    remaining -= 5;
                    boolean compressed = (bimgInfo & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD)) != 0;
                    if ((bimgInfo & BKPIMAGE_HAS_HOLE) != 0 && compressed) {
                        r.readUInt16();            // hole_length
                        remaining -= 2;
                    }
                    datatotal += imageBytes;
                }
                if ((blk.forkFlags & BKPBLOCK_SAME_REL) == 0) {
                    blk.spcOid = r.readUInt32();
                    blk.dbOid = r.readUInt32();
                    blk.relNumber = r.readUInt32();
                    remaining -= SIZE_OF_REL_FILE_LOCATOR;
                    lastSpc = blk.spcOid;
                    lastDb = blk.dbOid;
                    lastRel = blk.relNumber;
                    haveRel = true;
                } else if (haveRel) {
                    blk.spcOid = lastSpc;
                    blk.dbOid = lastDb;
                    blk.relNumber = lastRel;
                }
                blk.blockNumber = r.readUInt32();
                remaining -= SIZE_OF_BLOCK_NUMBER;
                blk.image = new byte[imageBytes];
                rec.blocks.add(blk);
            } else {
                throw new IllegalStateException("invalid block_id " + blockId + " in record at lsn " + rec.lsn);
            }
        }

        rec.blocks.sort((a, b) -> Integer.compare(a.id, b.id));
        for (BlockRef blk : rec.blocks) {
            if (blk.image.length > 0) {
                blk.image = r.readBytes(blk.image.length);
            }
            if (blk.hasData && blk.dataLength > 0) {
                blk.data = r.readBytes(blk.dataLength);
            }
        }
        rec.mainData = r.readBytes(mainDataLen);
        return rec;
    }
}
