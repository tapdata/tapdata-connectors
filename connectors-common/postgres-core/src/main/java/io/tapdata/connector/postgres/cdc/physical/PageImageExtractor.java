package io.tapdata.connector.postgres.cdc.physical;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;

/**
 * Reconstructs a full 8KB heap page from a WAL backup-block image and extracts
 * the on-disk heap tuple at a given offset. Mirrors PostgreSQL's
 * {@code RestoreBlockImage} (xlogreader.c) and the heap page layout in
 * {@code bufpage.h} / {@code htup_details.h}. Used as the wal_level=replica
 * fallback when records carry an FPI but no rmgr data (INSERT/UPDATE/DELETE
 * after a checkpoint), and as the only path to obtain old/new tuples for
 * UPDATE/DELETE when REPLICA IDENTITY FULL is not set.
 *
 * <p>Compressed images (pglz/lz4/zstd from wal_compression) are not supported;
 * callers receive {@code null} and should degrade gracefully.</p>
 *
 * @author Jarad
 */
public final class PageImageExtractor {

    private PageImageExtractor() {
    }

    /* PageHeaderData (bufpage.h): pd_lsn(8)+pd_checksum(2)+pd_flags(2)+
     * pd_lower(2)+pd_upper(2)+pd_special(2)+pd_pagesize_version(2)+pd_prune_xid(4) */
    private static final int SIZE_OF_PAGE_HEADER = 24;
    private static final int SIZE_OF_ITEM_ID = 4;

    /* ItemId lp_flags values */
    private static final int LP_NORMAL = 1;

    /* HeapTupleHeaderData field offsets (htup_details.h):
     * t_choice(12)+t_ctid(6)+t_infomask2(2)+t_infomask(2)+t_hoff(1) */
    private static final int OFF_T_INFOMASK2 = 18;
    private static final int OFF_T_INFOMASK = 20;
    private static final int OFF_T_HOFF = 22;

    /**
     * Rebuild the full XLOG_BLCKSZ-sized page from a backup block image,
     * filling the hole between pd_lower and pd_upper with zeros when the
     * image was stored with the hole removed. Returns {@code null} when the
     * image is compressed (unsupported here) or when the block carries no
     * image at all.
     */
    public static byte[] reconstructPage(XLogRecord.BlockRef block) {
        if (block == null || !block.hasImage || block.image.length == 0) {
            return null;
        }
        boolean compressed = (block.bimgInfo & (BKPIMAGE_COMPRESS_PGLZ
                | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD)) != 0;
        if (compressed) {
            return null;
        }
        byte[] page = new byte[XLOG_BLCKSZ];
        if ((block.bimgInfo & BKPIMAGE_HAS_HOLE) != 0) {
            int holeOff = block.imageHoleOffset;
            int holeLen = block.imageHoleLength;
            int tail = block.image.length - holeOff;
            System.arraycopy(block.image, 0, page, 0, holeOff);
            // hole bytes already zero from new byte[]
            System.arraycopy(block.image, holeOff, page, holeOff + holeLen, tail);
        } else {
            int n = Math.min(block.image.length, XLOG_BLCKSZ);
            System.arraycopy(block.image, 0, page, 0, n);
        }
        return page;
    }

    /**
     * Highest 1-based line-pointer slot present on the page, derived from
     * pd_lower. Slots in {@code 1..maxOffset} may still be unused/dead; callers
     * probe each with {@link #extractOnDiskTuple} and skip the nulls.
     */
    public static int maxOffset(byte[] page) {
        if (page == null || page.length < SIZE_OF_PAGE_HEADER) {
            return 0;
        }
        int pdLower = readUInt16LE(page, 12);   // PageHeaderData.pd_lower
        return Math.max(0, (pdLower - SIZE_OF_PAGE_HEADER) / SIZE_OF_ITEM_ID);
    }

    /**
     * Extract the raw on-disk heap tuple bytes at the given 1-based offset
     * number from a reconstructed page. Returns {@code null} when the line
     * pointer is not LP_NORMAL (e.g. redirected, dead, unused) or when the
     * offset is out of range.
     */
    public static byte[] extractOnDiskTuple(byte[] page, int offnum) {
        if (page == null || page.length < SIZE_OF_PAGE_HEADER || offnum < 1) {
            return null;
        }
        int pdLower = readUInt16LE(page, 12);   // PageHeaderData.pd_lower
        int linpStart = SIZE_OF_PAGE_HEADER;
        int maxOff = (pdLower - linpStart) / SIZE_OF_ITEM_ID;
        if (offnum > maxOff) {
            return null;
        }
        int itemIdOff = linpStart + (offnum - 1) * SIZE_OF_ITEM_ID;
        int word = readUInt32LE(page, itemIdOff);
        int lpOff = word & 0x7FFF;
        int lpFlags = (word >>> 15) & 0x3;
        int lpLen = (word >>> 17) & 0x7FFF;
        if (lpFlags != LP_NORMAL || lpLen == 0 || lpOff + lpLen > XLOG_BLCKSZ) {
            return null;
        }
        byte[] out = new byte[lpLen];
        System.arraycopy(page, lpOff, out, 0, lpLen);
        return out;
    }

    /**
     * Convert an on-disk heap tuple (starting with HeapTupleHeaderData) into
     * the WAL tuple wire format expected by {@link HeapTupleDecoder}: a
     * 5-byte xl_heap_header (t_infomask2, t_infomask, t_hoff) followed by the
     * null bitmap, alignment padding and the column data area.
     */
    public static byte[] toWalTupleFormat(byte[] onDisk) {
        if (onDisk == null || onDisk.length < SIZE_OF_HEAP_TUPLE_HEADER) {
            return null;
        }
        int infomask2 = readUInt16LE(onDisk, OFF_T_INFOMASK2);
        int infomask = readUInt16LE(onDisk, OFF_T_INFOMASK);
        int tHoff = onDisk[OFF_T_HOFF] & 0xFF;
        int tail = onDisk.length - SIZE_OF_HEAP_TUPLE_HEADER;
        byte[] out = new byte[SIZE_OF_HEAP_HEADER + tail];
        out[0] = (byte) infomask2;
        out[1] = (byte) (infomask2 >>> 8);
        out[2] = (byte) infomask;
        out[3] = (byte) (infomask >>> 8);
        out[4] = (byte) tHoff;
        System.arraycopy(onDisk, SIZE_OF_HEAP_TUPLE_HEADER, out, SIZE_OF_HEAP_HEADER, tail);
        return out;
    }

    /**
     * Convenience helper combining {@link #reconstructPage},
     * {@link #extractOnDiskTuple} and {@link #toWalTupleFormat}. Returns
     * {@code null} when any step fails (compressed image, bad offnum, etc.).
     */
    public static byte[] extractWalTuple(XLogRecord.BlockRef block, int offnum) {
        byte[] page = reconstructPage(block);
        if (page == null) {
            return null;
        }
        byte[] onDisk = extractOnDiskTuple(page, offnum);
        if (onDisk == null) {
            return null;
        }
        return toWalTupleFormat(onDisk);
    }

    private static int readUInt16LE(byte[] b, int off) {
        return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8);
    }

    private static int readUInt32LE(byte[] b, int off) {
        return (b[off] & 0xFF)
                | ((b[off + 1] & 0xFF) << 8)
                | ((b[off + 2] & 0xFF) << 16)
                | ((b[off + 3] & 0xFF) << 24);
    }
}
