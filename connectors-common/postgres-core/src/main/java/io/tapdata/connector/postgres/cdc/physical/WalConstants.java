package io.tapdata.connector.postgres.cdc.physical;

/**
 * Binary layout constants for decoding raw PostgreSQL WAL streamed from a
 * physical replication slot. Values are verified against PostgreSQL 16
 * (src/include/access/xlogrecord.h and heapam_xlog.h). They are stable for
 * PG 13-16 with respect to the fields this decoder relies on.
 *
 * @author Jarad
 */
public final class WalConstants {

    private WalConstants() {
    }

    /* ---- page / segment geometry ---- */
    public static final int XLOG_BLCKSZ = 8192;              // default WAL page size
    public static final long DEFAULT_WAL_SEGMENT_SIZE = 16L * 1024 * 1024;
    public static final int MAXALIGN = 8;

    /* ---- XLogPageHeaderData (short) ---- */
    // uint16 magic + uint16 info + uint32 tli + uint64 pageaddr + uint32 rem_len -> 20, MAXALIGN -> 24
    public static final int SIZE_OF_XLOG_SHORT_PHD = 24;
    // short header + uint64 sysid + uint32 seg_size + uint32 blcksz -> 40 (already MAXALIGN)
    public static final int SIZE_OF_XLOG_LONG_PHD = 40;
    public static final int XLOG_PAGE_MAGIC_16 = 0xD113;    // PG16 page magic

    /* xlp_info flag bits */
    public static final int XLP_FIRST_IS_CONTRECORD = 0x0001;
    public static final int XLP_LONG_HEADER = 0x0002;
    public static final int XLP_BKP_REMOVABLE = 0x0004;
    public static final int XLP_FIRST_IS_OVERWRITE_CONTRECORD = 0x0008;

    /* ---- XLogRecord header ---- */
    // uint32 tot_len + uint32 xid + uint64 prev + uint8 info + uint8 rmid + 2 pad + uint32 crc = 24
    public static final int SIZE_OF_XLOG_RECORD = 24;

    /* xl_info masks */
    public static final int XLR_INFO_MASK = 0x0F;
    public static final int XLR_RMGR_INFO_MASK = 0xF0;

    /* ---- block reference (XLogRecordBlockHeader) ---- */
    public static final int BKPBLOCK_FORK_MASK = 0x0F;
    public static final int BKPBLOCK_FLAG_MASK = 0xF0;
    public static final int BKPBLOCK_HAS_IMAGE = 0x10;
    public static final int BKPBLOCK_HAS_DATA = 0x20;
    public static final int BKPBLOCK_WILL_INIT = 0x40;
    public static final int BKPBLOCK_SAME_REL = 0x80;

    /* image header (XLogRecordBlockImageHeader) bimg_info flags */
    public static final int BKPIMAGE_HAS_HOLE = 0x01;
    public static final int BKPIMAGE_APPLY = 0x02;
    public static final int BKPIMAGE_COMPRESS_PGLZ = 0x04;
    public static final int BKPIMAGE_COMPRESS_LZ4 = 0x08;
    public static final int BKPIMAGE_COMPRESS_ZSTD = 0x10;

    /* reserved block ids */
    public static final int XLR_MAX_BLOCK_ID = 32;
    public static final int XLR_BLOCK_ID_DATA_SHORT = 255;
    public static final int XLR_BLOCK_ID_DATA_LONG = 254;
    public static final int XLR_BLOCK_ID_ORIGIN = 253;
    public static final int XLR_BLOCK_ID_TOPLEVEL_XID = 252;

    /* RelFileLocator = spcOid(4) + dbOid(4) + relNumber(4) */
    public static final int SIZE_OF_REL_FILE_LOCATOR = 12;
    public static final int SIZE_OF_BLOCK_NUMBER = 4;

    /* ---- resource managers we care about ---- */
    public static final int RM_XLOG_ID = 0;
    public static final int RM_TRANSACTION_ID = 1;
    public static final int RM_HEAP2_ID = 9;
    public static final int RM_HEAP_ID = 10;

    /* ---- heap opcodes (xl_info & XLOG_HEAP_OPMASK) ---- */
    public static final int XLOG_HEAP_OPMASK = 0x70;
    public static final int XLOG_HEAP_INIT_PAGE = 0x80;
    public static final int XLOG_HEAP_INSERT = 0x00;
    public static final int XLOG_HEAP_DELETE = 0x10;
    public static final int XLOG_HEAP_UPDATE = 0x20;
    public static final int XLOG_HEAP_TRUNCATE = 0x30;
    public static final int XLOG_HEAP_HOT_UPDATE = 0x40;
    public static final int XLOG_HEAP_CONFIRM = 0x50;
    public static final int XLOG_HEAP_LOCK = 0x60;
    public static final int XLOG_HEAP_INPLACE = 0x70;

    /* heap2 opcodes */
    public static final int XLOG_HEAP2_PRUNE = 0x10;
    public static final int XLOG_HEAP2_VACUUM = 0x20;
    public static final int XLOG_HEAP2_FREEZE_PAGE = 0x30;
    public static final int XLOG_HEAP2_VISIBLE = 0x40;
    public static final int XLOG_HEAP2_MULTI_INSERT = 0x50;

    /* xlog rmgr opcodes (subset) */
    public static final int XLOG_CHECKPOINT_SHUTDOWN = 0x00;
    public static final int XLOG_CHECKPOINT_ONLINE = 0x10;
    public static final int XLOG_SWITCH = 0x40;
    public static final int XLOG_FPI = 0xA0;
    public static final int XLOG_FPI_FOR_HINT = 0xB0;

    /* ---- transaction rmgr opcodes ---- */
    public static final int XLOG_XACT_COMMIT = 0x00;
    public static final int XLOG_XACT_ABORT = 0x20;
    public static final int XLOG_XACT_COMMIT_PREPARED = 0x30;
    public static final int XLOG_XACT_ABORT_PREPARED = 0x40;
    public static final int XLOG_XACT_OPMASK = 0x70;

    /* xl_info low-nibble flag: an xl_xact_xinfo word follows the commit/abort body */
    public static final int XLOG_XACT_HAS_INFO = 0x01;
    /* xl_xact_xinfo bits (order of the optional sections that follow) */
    public static final int XACT_XINFO_HAS_DBINFO = (1 << 0);
    public static final int XACT_XINFO_HAS_SUBXACTS = (1 << 1);

    /* ---- heap record struct sizes ---- */
    public static final int SIZE_OF_HEAP_HEADER = 5;        // t_infomask2(2)+t_infomask(2)+t_hoff(1)
    public static final int SIZE_OF_HEAP_INSERT = 3;        // offnum(2)+flags(1)
    public static final int SIZE_OF_HEAP_DELETE = 8;        // xmax(4)+offnum(2)+infobits(1)+flags(1)
    public static final int SIZE_OF_HEAP_UPDATE = 14;       // old_xmax(4)+old_offnum(2)+old_infobits(1)+flags(1)+new_xmax(4)+new_offnum(2)
    public static final int SIZE_OF_HEAP_MULTI_INSERT = 4;  // flags(1)+pad(1)+ntuples(2)
    public static final int SIZE_OF_MULTI_INSERT_TUPLE = 7; // datalen(2)+t_infomask2(2)+t_infomask(2)+t_hoff(1)

    /* HeapTupleHeaderData: offsetof(t_bits) */
    public static final int SIZE_OF_HEAP_TUPLE_HEADER = 23;

    /* heap tuple infomask bits */
    public static final int HEAP_HASNULL = 0x0001;
    public static final int HEAP_HASVARWIDTH = 0x0002;
    public static final int HEAP_HASEXTERNAL = 0x0004;
    public static final int HEAP_NATTS_MASK = 0x07FF;       // in t_infomask2

    /* ---- insert flags ---- */
    public static final int XLH_INSERT_CONTAINS_NEW_TUPLE = (1 << 3);
    public static final int XLH_INSERT_ON_TOAST_RELATION = (1 << 4);
    public static final int XLH_INSERT_LAST_IN_MULTI = (1 << 1);

    /* ---- update flags ---- */
    public static final int XLH_UPDATE_CONTAINS_OLD_TUPLE = (1 << 2);
    public static final int XLH_UPDATE_CONTAINS_OLD_KEY = (1 << 3);
    public static final int XLH_UPDATE_CONTAINS_NEW_TUPLE = (1 << 4);
    public static final int XLH_UPDATE_PREFIX_FROM_OLD = (1 << 5);
    public static final int XLH_UPDATE_SUFFIX_FROM_OLD = (1 << 6);
    public static final int XLH_UPDATE_CONTAINS_OLD =
            XLH_UPDATE_CONTAINS_OLD_TUPLE | XLH_UPDATE_CONTAINS_OLD_KEY;

    /* ---- delete flags ---- */
    public static final int XLH_DELETE_CONTAINS_OLD_TUPLE = (1 << 1);
    public static final int XLH_DELETE_CONTAINS_OLD_KEY = (1 << 2);
    public static final int XLH_DELETE_CONTAINS_OLD =
            XLH_DELETE_CONTAINS_OLD_TUPLE | XLH_DELETE_CONTAINS_OLD_KEY;

    public static int maxAlign(int len) {
        return (len + (MAXALIGN - 1)) & ~(MAXALIGN - 1);
    }

    public static long maxAlign(long len) {
        return (len + (MAXALIGN - 1)) & ~((long) MAXALIGN - 1);
    }
}
