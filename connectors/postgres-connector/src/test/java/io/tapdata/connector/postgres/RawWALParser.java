package io.tapdata.connector.postgres;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.postgresql.replication.fluent.physical.ChainedPhysicalStreamBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * PostgreSQL Physical WAL Parser for CDC data listening
 * Correctly parses WAL records based on PostgreSQL 9.5+ format
 * Reference: https://www.interdb.jp/pg/pgsql09/04.html
 */
public class RawWALParser {

    private static final Logger LOGGER = Logger.getLogger(RawWALParser.class.getName());

    // Resource Manager IDs (from PostgreSQL source code)
    private static final int RM_XLOG_ID = 0;
    private static final int RM_XACT_ID = 1;
    private static final int RM_SMGR_ID = 2;
    private static final int RM_CLOG_ID = 3;
    private static final int RM_DBASE_ID = 4;
    private static final int RM_TBLSPC_ID = 5;
    private static final int RM_MULTIXACT_ID = 6;
    private static final int RM_RELMAP_ID = 7;
    private static final int RM_STANDBY_ID = 8;
    private static final int RM_HEAP2_ID = 9;
    private static final int RM_HEAP_ID = 10;
    private static final int RM_BTREE_ID = 11;

    // HEAP operation info flags
    private static final int XLOG_HEAP_INSERT = 0x00;
    private static final int XLOG_HEAP_DELETE = 0x10;
    private static final int XLOG_HEAP_UPDATE = 0x20;
    private static final int XLOG_HEAP_HOT_UPDATE = 0x40;

    // XACT operation info flags
    private static final int XLOG_XACT_COMMIT = 0x00;
    private static final int XLOG_XACT_ABORT = 0x20;

    // Block reference IDs
    private static final int XLR_BLOCK_ID_DATA_SHORT = 255;
    private static final int XLR_BLOCK_ID_DATA_LONG = 254;

    // Block flags
    private static final int BKPBLOCK_FORK_MASK = 0x0F;
    private static final int BKPBLOCK_FLAG_MASK = 0xF0;
    private static final int BKPBLOCK_HAS_IMAGE = 0x10;
    private static final int BKPBLOCK_HAS_DATA = 0x20;
    private static final int BKPBLOCK_WILL_INIT = 0x40;
    private static final int BKPBLOCK_SAME_REL = 0x80;

    private volatile boolean running = true;
    private PGReplicationStream stream;

    public static void main(String[] args) {
        RawWALParser parser = new RawWALParser();

        // 添加优雅关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down WAL parser...");
            parser.stop();
        }));

        parser.start();
    }

    public void start() {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";

        // 使用物理复制连接（不需要复制槽）
        Properties replicationProps = new Properties();
        replicationProps.setProperty("user", "postgres");
        replicationProps.setProperty("password", "gj0628");
        replicationProps.setProperty("replication", "database"); // 启用复制协议
        replicationProps.setProperty("assumeMinServerVersion", "9.4");
        replicationProps.setProperty("preferQueryMode", "simple");

        try (Connection replicationConnection = DriverManager.getConnection(jdbcUrl, replicationProps)) {
            PGConnection pgConnection = replicationConnection.unwrap(PGConnection.class);

            // 创建物理复制流
            stream = createPhysicalReplicationStream(pgConnection);

            // 持续处理物理WAL记录
            processPhysicalWALRecords(stream);

        } catch (SQLException e) {
            LOGGER.severe("Failed to start physical WAL parser: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup();
        }
    }

    public void stop() {
        running = false;
        if (stream != null) {
            try {
                stream.close();
            } catch (SQLException e) {
                LOGGER.warning("Error closing replication stream: " + e.getMessage());
            }
        }
    }

    /**
     * 创建物理复制流
     */
    private PGReplicationStream createPhysicalReplicationStream(PGConnection pgConnection) throws SQLException {
        LOGGER.info("Creating physical replication stream...");

        // 获取当前WAL位置作为起始点
        LogSequenceNumber startLsn = getCurrentWALPosition(pgConnection);
        LOGGER.info("Starting physical replication from LSN: " + startLsn);

        return pgConnection.getReplicationAPI()
                .replicationStream()
                .physical()
                .withStartPosition(startLsn)
                .withStatusInterval(10, TimeUnit.SECONDS)
                .start();
    }

    /**
     * 获取当前WAL位置
     */
    private LogSequenceNumber getCurrentWALPosition(PGConnection pgConnection) throws SQLException {
        try (Statement stmt = ((Connection)pgConnection).createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT pg_current_wal_lsn()")) {
                if (rs.next()) {
                    String lsnStr = rs.getString(1);
                    return LogSequenceNumber.valueOf(lsnStr);
                }
            }
        }
        throw new SQLException("Could not get current WAL position");
    }

    /**
     * 处理物理WAL记录
     */
    private void processPhysicalWALRecords(PGReplicationStream stream) throws SQLException {
        LOGGER.info("Starting to process physical WAL records...");

        while (running) {
            try {
                ByteBuffer buffer = stream.readPending();

                if (buffer == null) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.info("Physical WAL processing interrupted");
                        return;
                    }
                    continue;
                }

                // 解析物理WAL记录
                parsePhysicalWALRecord(buffer);

                // 确认处理进度
                LogSequenceNumber lsn = stream.getLastReceiveLSN();
                stream.setAppliedLSN(lsn);
                stream.setFlushedLSN(lsn);

            } catch (SQLException e) {
                LOGGER.severe("Error processing physical WAL record: " + e.getMessage());
                throw e;
            }
        }

        LOGGER.info("Physical WAL processing stopped");
    }

    /**
     * 解析物理WAL记录
     * 基于PostgreSQL 9.5+的正确WAL格式
     */
    private void parsePhysicalWALRecord(ByteBuffer buffer) {
        try {
            // 设置字节序为小端序（PostgreSQL使用小端序）
            buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

            while (buffer.remaining() > 0) {
                if (!parseXLogRecord(buffer)) {
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing physical WAL record: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 解析XLogRecord结构
     * 基于PostgreSQL 9.5+格式
     */
    private boolean parseXLogRecord(ByteBuffer buffer) {
        try {
            if (buffer.remaining() < 24) { // XLogRecord最小长度
                return false;
            }

            int recordStart = buffer.position();

            // 解析XLogRecord头部 (PostgreSQL 9.5+格式)
            int xl_tot_len = buffer.getInt();           // 记录总长度
            int xl_xid = buffer.getInt();               // 事务ID
            long xl_prev = buffer.getLong();            // 前一个记录的LSN
            int xl_info = buffer.get() & 0xFF;          // 信息标志
            int xl_rmid = buffer.get() & 0xFF;          // 资源管理器ID
            buffer.getShort(); // 跳过2字节填充
            int xl_crc = buffer.getInt();               // CRC校验

            // 验证记录长度
            if (xl_tot_len < 24 || xl_tot_len > 65536) { // 合理的最大长度
                LOGGER.warning("Invalid WAL record length: " + xl_tot_len);
                return false;
            }

            if (buffer.remaining() < xl_tot_len - 24) {
                LOGGER.warning("Insufficient data for WAL record");
                return false;
            }

            LOGGER.info(String.format("XLogRecord - XID: %d, Length: %d, RM: %d, Info: 0x%02X, Prev: 0x%X",
                    xl_xid, xl_tot_len, xl_rmid, xl_info, xl_prev));

            // 解析记录数据部分
            int dataLength = xl_tot_len - 24;
            if (dataLength > 0) {
                parseXLogRecordData(buffer, xl_rmid, xl_info, xl_xid, dataLength);
            }

            // 移动到下一个记录
            buffer.position(recordStart + xl_tot_len);
            return true;

        } catch (Exception e) {
            LOGGER.warning("Error parsing XLogRecord: " + e.getMessage());
            return false;
        }
    }

    /**
     * 解析XLogRecord数据部分
     * 基于PostgreSQL 9.5+的新格式
     */
    private void parseXLogRecordData(ByteBuffer buffer, int rmid, int info, int xid, int dataLength) {
        try {
            int dataStart = buffer.position();

            // 解析块头部和数据头部
            parseRecordHeaders(buffer, rmid, info, xid, dataLength);

            // 确保移动到数据结束位置
            buffer.position(dataStart + dataLength);

        } catch (Exception e) {
            LOGGER.warning("Error parsing XLogRecord data: " + e.getMessage());
            // 跳过剩余数据
            buffer.position(buffer.position() + Math.min(dataLength, buffer.remaining()));
        }
    }

    /**
     * 解析记录头部（XLogRecordBlockHeader和XLogRecordDataHeader）
     */
    private void parseRecordHeaders(ByteBuffer buffer, int rmid, int info, int xid, int totalDataLength) {
        try {
            int dataEnd = buffer.position() + totalDataLength;

            while (buffer.position() < dataEnd && buffer.remaining() > 0) {
                if (buffer.remaining() < 1) break;

                int id = buffer.get() & 0xFF;

                if (id <= 31) {
                    // XLogRecordBlockHeader
                    parseBlockHeader(buffer, id, rmid, info, xid);
                } else if (id == XLR_BLOCK_ID_DATA_SHORT) {
                    // XLogRecordDataHeaderShort
                    parseDataHeaderShort(buffer, rmid, info, xid);
                } else if (id == XLR_BLOCK_ID_DATA_LONG) {
                    // XLogRecordDataHeaderLong
                    parseDataHeaderLong(buffer, rmid, info, xid);
                } else {
                    LOGGER.warning("Unknown record header ID: " + id);
                    break;
                }
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing record headers: " + e.getMessage());
        }
    }

    /**
     * 解析XLogRecordBlockHeader
     */
    private void parseBlockHeader(ByteBuffer buffer, int blockId, int rmid, int info, int xid) {
        try {
            if (buffer.remaining() < 3) return;

            int fork_flags = buffer.get() & 0xFF;
            int data_length = buffer.getShort() & 0xFFFF;

            int fork = fork_flags & BKPBLOCK_FORK_MASK;
            boolean hasImage = (fork_flags & BKPBLOCK_HAS_IMAGE) != 0;
            boolean hasData = (fork_flags & BKPBLOCK_HAS_DATA) != 0;
            boolean willInit = (fork_flags & BKPBLOCK_WILL_INIT) != 0;
            boolean sameRel = (fork_flags & BKPBLOCK_SAME_REL) != 0;

            LOGGER.info(String.format("  Block %d - Fork: %d, DataLen: %d, HasImage: %s, HasData: %s",
                    blockId, fork, data_length, hasImage, hasData));

            // 解析RelFileNode（如果不是SAME_REL）
            if (!sameRel && buffer.remaining() >= 12) {
                int spcNode = buffer.getInt();
                int dbNode = buffer.getInt();
                int relNode = buffer.getInt();
                LOGGER.info(String.format("    RelFileNode: %d.%d.%d", spcNode, dbNode, relNode));
            }

            // 解析BlockNumber
            if (buffer.remaining() >= 4) {
                int blockNumber = buffer.getInt();
                LOGGER.info(String.format("    Block Number: %d", blockNumber));
            }

            // 解析XLogRecordBlockImageHeader（如果有full-page image）
            if (hasImage) {
                parseBlockImageHeader(buffer);
            }

            // 解析块数据
            if (hasData && data_length > 0) {
                parseBlockData(buffer, rmid, info, xid, data_length);
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing block header: " + e.getMessage());
        }
    }

    /**
     * 解析XLogRecordBlockImageHeader
     */
    private void parseBlockImageHeader(ByteBuffer buffer) {
        try {
            if (buffer.remaining() < 5) return;

            int length = buffer.getShort() & 0xFFFF;
            int hole_offset = buffer.getShort() & 0xFFFF;
            int bimg_info = buffer.get() & 0xFF;

            boolean hasHole = (bimg_info & 0x01) != 0;
            boolean compressed = (bimg_info & 0x1C) != 0;

            LOGGER.info(String.format("    Image - Length: %d, HoleOffset: %d, HasHole: %s, Compressed: %s",
                    length, hole_offset, hasHole, compressed));

            // 如果有压缩头部
            if (hasHole && compressed && buffer.remaining() >= 2) {
                int hole_length = buffer.getShort() & 0xFFFF;
                LOGGER.info(String.format("    Hole Length: %d", hole_length));
            }

            // 跳过实际的页面镜像数据
            if (length > 0 && buffer.remaining() >= length) {
                buffer.position(buffer.position() + length);
                LOGGER.info("    Skipped page image data");
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing block image header: " + e.getMessage());
        }
    }

    /**
     * 解析块数据
     */
    private void parseBlockData(ByteBuffer buffer, int rmid, int info, int xid, int dataLength) {
        try {
            LOGGER.info(String.format("    Block Data - RM: %d, Info: 0x%02X, Length: %d", rmid, info, dataLength));

            if (rmid == RM_HEAP_ID) {
                parseHeapBlockData(buffer, info, xid, dataLength);
            } else if (rmid == RM_XACT_ID) {
                parseXactBlockData(buffer, info, xid, dataLength);
            } else {
                // 跳过未知的块数据
                skipData(buffer, dataLength);
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing block data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析堆表块数据
     */
    private void parseHeapBlockData(ByteBuffer buffer, int info, int xid, int dataLength) {
        try {
            switch (info) {
                case XLOG_HEAP_INSERT:
                    LOGGER.info(String.format("      HEAP INSERT - XID: %d", xid));
                    parseHeapInsertData(buffer, dataLength);
                    break;
                case XLOG_HEAP_DELETE:
                    LOGGER.info(String.format("      HEAP DELETE - XID: %d", xid));
                    parseHeapDeleteData(buffer, dataLength);
                    break;
                case XLOG_HEAP_UPDATE:
                    LOGGER.info(String.format("      HEAP UPDATE - XID: %d", xid));
                    parseHeapUpdateData(buffer, dataLength);
                    break;
                case XLOG_HEAP_HOT_UPDATE:
                    LOGGER.info(String.format("      HEAP HOT_UPDATE - XID: %d", xid));
                    parseHeapUpdateData(buffer, dataLength);
                    break;
                default:
                    LOGGER.fine("      Unknown heap operation: 0x" + Integer.toHexString(info));
                    skipData(buffer, dataLength);
                    break;
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing heap block data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析XLogRecordDataHeaderShort
     */
    private void parseDataHeaderShort(ByteBuffer buffer, int rmid, int info, int xid) {
        try {
            if (buffer.remaining() < 1) return;

            int data_length = buffer.get() & 0xFF;
            LOGGER.info(String.format("  Main Data (Short) - Length: %d", data_length));

            if (data_length > 0) {
                parseMainData(buffer, rmid, info, xid, data_length);
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing data header short: " + e.getMessage());
        }
    }

    /**
     * 解析XLogRecordDataHeaderLong
     */
    private void parseDataHeaderLong(ByteBuffer buffer, int rmid, int info, int xid) {
        try {
            if (buffer.remaining() < 4) return;

            int data_length = buffer.getInt();
            LOGGER.info(String.format("  Main Data (Long) - Length: %d", data_length));

            if (data_length > 0) {
                parseMainData(buffer, rmid, info, xid, data_length);
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing data header long: " + e.getMessage());
        }
    }

    /**
     * 解析主数据
     */
    private void parseMainData(ByteBuffer buffer, int rmid, int info, int xid, int dataLength) {
        try {
            LOGGER.info(String.format("  Main Data - RM: %d, Info: 0x%02X, Length: %d", rmid, info, dataLength));

            if (rmid == RM_HEAP_ID) {
                parseHeapMainData(buffer, info, xid, dataLength);
            } else if (rmid == RM_XACT_ID) {
                parseXactMainData(buffer, info, xid, dataLength);
            } else {
                // 跳过未知的主数据
                skipData(buffer, dataLength);
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing main data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析堆表主数据
     */
    private void parseHeapMainData(ByteBuffer buffer, int info, int xid, int dataLength) {
        try {
            switch (info) {
                case XLOG_HEAP_INSERT:
                    LOGGER.info(String.format("    HEAP INSERT Main Data - XID: %d", xid));
                    parseHeapInsertMainData(buffer, dataLength);
                    break;
                case XLOG_HEAP_DELETE:
                    LOGGER.info(String.format("    HEAP DELETE Main Data - XID: %d", xid));
                    parseHeapDeleteMainData(buffer, dataLength);
                    break;
                case XLOG_HEAP_UPDATE:
                case XLOG_HEAP_HOT_UPDATE:
                    LOGGER.info(String.format("    HEAP UPDATE Main Data - XID: %d", xid));
                    parseHeapUpdateMainData(buffer, dataLength);
                    break;
                default:
                    LOGGER.fine("    Unknown heap main data: 0x" + Integer.toHexString(info));
                    skipData(buffer, dataLength);
                    break;
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing heap main data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析堆表INSERT块数据
     */
    private void parseHeapInsertData(ByteBuffer buffer, int dataLength) {
        try {
            LOGGER.info(String.format("        INSERT Block Data - Length: %d", dataLength));

            if (dataLength > 0) {
                // 解析实际的元组数据
                parseHeapTupleData(buffer, dataLength, "INSERT");
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing heap insert data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析HeapTuple数据
     * 基于PostgreSQL官方文档的HeapTupleHeaderData格式
     */
    private void parseHeapTupleData(ByteBuffer buffer, int dataLength, String operation) {
        try {
            if (dataLength < 23) { // HeapTupleHeader最小长度
                LOGGER.warning("Data too short for HeapTupleHeader: " + dataLength);
                skipData(buffer, dataLength);
                return;
            }

            int startPos = buffer.position();

            // 解析HeapTupleHeaderData (23字节)
            int t_xmin = buffer.getInt();           // insert XID stamp
            int t_xmax = buffer.getInt();           // delete XID stamp
            int t_cid = buffer.getInt();            // insert/delete CID stamp

            // t_ctid (ItemPointerData, 6字节)
            int blockNumber = buffer.getInt();      // 块号
            short offsetNumber = buffer.getShort(); // 偏移号

            int t_infomask2 = buffer.getShort() & 0xFFFF;  // 属性数量和标志位
            int t_infomask = buffer.getShort() & 0xFFFF;   // 各种标志位
            int t_hoff = buffer.get() & 0xFF;              // 用户数据偏移量

            LOGGER.info(String.format("        %s HeapTuple - XMIN: %d, XMAX: %d, CID: %d",
                    operation, t_xmin, t_xmax, t_cid));
            LOGGER.info(String.format("          CTID: (%d,%d), InfoMask: 0x%04X, InfoMask2: 0x%04X, HdrOff: %d",
                    blockNumber, offsetNumber, t_infomask, t_infomask2, t_hoff));

            // 解析属性数量
            int natts = t_infomask2 & 0x07FF; // 低11位是属性数量
            LOGGER.info(String.format("          Attributes: %d", natts));

            // 检查是否有NULL位图
            boolean hasNulls = (t_infomask & 0x0001) != 0; // HEAP_HASNULL
            LOGGER.info(String.format("          Has NULLs: %s", hasNulls));

            // 解析NULL位图（如果存在）
            byte[] nullBitmap = null;
            if (hasNulls && natts > 0) {
                int nullBitmapBytes = (natts + 7) / 8; // 向上取整
                if (buffer.remaining() >= nullBitmapBytes) {
                    nullBitmap = new byte[nullBitmapBytes];
                    buffer.get(nullBitmap);
                    LOGGER.info(String.format("          NULL Bitmap: %s", bytesToHex(nullBitmap)));
                }
            }

            // 跳过到用户数据开始位置
            int currentOffset = buffer.position() - startPos;
            if (t_hoff > currentOffset) {
                int skipBytes = t_hoff - currentOffset;
                if (buffer.remaining() >= skipBytes) {
                    buffer.position(buffer.position() + skipBytes);
                    LOGGER.info(String.format("          Skipped %d padding bytes", skipBytes));
                }
            }

            // 解析用户数据
            int userDataLength = dataLength - (buffer.position() - startPos);
            if (userDataLength > 0) {
                LOGGER.info(String.format("          User Data Length: %d bytes", userDataLength));
                parseUserData(buffer, userDataLength, natts, nullBitmap);
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing heap tuple data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析用户数据（列数据）- 完整解析所有元数据
     */
    private void parseUserData(ByteBuffer buffer, int dataLength, int natts, byte[] nullBitmap) {
        try {
            LOGGER.info(String.format("          Parsing %d attributes in %d bytes", natts, dataLength));

            int startPos = buffer.position();
            int endPos = startPos + dataLength;

            // 输出完整的原始数据
            byte[] rawData = new byte[dataLength];
            buffer.get(rawData);
            buffer.position(startPos); // 重置位置
            LOGGER.info(String.format("          Raw Data: %s", bytesToHex(rawData)));

            for (int i = 0; i < natts && buffer.position() < endPos; i++) {
                int attrStartPos = buffer.position();

                // 检查是否为NULL
                boolean isNull = false;
                if (nullBitmap != null) {
                    int byteIndex = i / 8;
                    int bitIndex = i % 8;
                    if (byteIndex < nullBitmap.length) {
                        isNull = (nullBitmap[byteIndex] & (1 << bitIndex)) == 0;
                    }
                }

                if (isNull) {
                    LOGGER.info(String.format("            Attr %d: NULL", i));
                    continue;
                }

                if (buffer.remaining() == 0) {
                    LOGGER.info(String.format("            Attr %d: <no data remaining>", i));
                    break;
                }

                // 详细解析每个属性的数据
                parseAttributeData(buffer, i, endPos);
            }

            // 输出剩余未解析的数据
            int remaining = endPos - buffer.position();
            if (remaining > 0) {
                byte[] remainingData = new byte[remaining];
                buffer.get(remainingData);
                LOGGER.info(String.format("          Remaining %d bytes: %s", remaining, bytesToHex(remainingData)));
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing user data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 解析单个属性的数据
     */
    private void parseAttributeData(ByteBuffer buffer, int attrIndex, int endPos) {
        try {
            int startPos = buffer.position();

            if (buffer.remaining() < 1) {
                LOGGER.info(String.format("            Attr %d: <no data>", attrIndex));
                return;
            }

            // 读取第一个字节来判断数据类型
            int firstByte = buffer.get() & 0xFF;
            buffer.position(startPos); // 重置位置

            LOGGER.info(String.format("            Attr %d: [Pos: %d, FirstByte: 0x%02X]",
                    attrIndex, startPos, firstByte));

            // 根据第一个字节的标志位判断数据格式
            if ((firstByte & 0x01) == 0) {
                // 短变长格式 (1字节长度头)
                parseShortVarlena(buffer, attrIndex);
            } else if ((firstByte & 0x03) == 0x02) {
                // 长变长格式 (4字节长度头)
                parseLongVarlena(buffer, attrIndex);
            } else if ((firstByte & 0x03) == 0x01) {
                // TOAST指针
                parseToastPointer(buffer, attrIndex);
            } else {
                // 可能是固定长度类型或其他格式
                parseFixedLengthData(buffer, attrIndex, endPos);
            }

        } catch (Exception e) {
            LOGGER.warning(String.format("Error parsing attribute %d: %s", attrIndex, e.getMessage()));
        }
    }

    /**
     * 解析短变长数据 (1字节长度头)
     */
    private void parseShortVarlena(ByteBuffer buffer, int attrIndex) {
        try {
            if (buffer.remaining() < 1) return;

            int lengthByte = buffer.get() & 0xFF;
            int dataLength = lengthByte >> 1; // 去掉标志位

            LOGGER.info(String.format("              Short Varlena - Length: %d", dataLength));

            if (dataLength > 1 && buffer.remaining() >= dataLength - 1) {
                byte[] data = new byte[dataLength - 1];
                buffer.get(data);

                // 尝试解析为字符串
                String strValue = new String(data, StandardCharsets.UTF_8);
                LOGGER.info(String.format("              Value: \"%s\"", strValue));
                LOGGER.info(String.format("              Hex: %s", bytesToHex(data)));
            } else if (dataLength == 1) {
                LOGGER.info("              Empty string");
            } else {
                LOGGER.info(String.format("              Invalid length or insufficient data: %d", dataLength));
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing short varlena: " + e.getMessage());
        }
    }

    /**
     * 解析长变长数据 (4字节长度头)
     */
    private void parseLongVarlena(ByteBuffer buffer, int attrIndex) {
        try {
            if (buffer.remaining() < 4) return;

            int lengthWord = buffer.getInt();
            int dataLength = lengthWord >> 2; // 去掉标志位

            LOGGER.info(String.format("              Long Varlena - Length: %d", dataLength));

            if (dataLength > 4 && buffer.remaining() >= dataLength - 4) {
                byte[] data = new byte[dataLength - 4];
                buffer.get(data);

                // 尝试解析为字符串
                String strValue = new String(data, StandardCharsets.UTF_8);
                LOGGER.info(String.format("              Value: \"%s\"", strValue));
                LOGGER.info(String.format("              Hex: %s", bytesToHex(data)));
            } else if (dataLength == 4) {
                LOGGER.info("              Empty long varlena");
            } else {
                LOGGER.info(String.format("              Invalid length or insufficient data: %d", dataLength));
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing long varlena: " + e.getMessage());
        }
    }

    /**
     * 解析TOAST指针
     */
    private void parseToastPointer(ByteBuffer buffer, int attrIndex) {
        try {
            LOGGER.info("              TOAST Pointer detected");

            if (buffer.remaining() >= 18) { // TOAST指针通常是18字节
                buffer.get(); // 跳过第一个字节（已读过）

                // 读取TOAST指针的各个字段
                int va_rawsize = buffer.getInt();
                int va_extsize = buffer.getInt();
                int va_valueid = buffer.getInt();
                int va_toastrelid = buffer.getInt();

                LOGGER.info(String.format("              RawSize: %d, ExtSize: %d, ValueID: %d, ToastRelID: %d",
                        va_rawsize, va_extsize, va_valueid, va_toastrelid));
            } else {
                // 读取剩余所有字节
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);
                LOGGER.info(String.format("              TOAST data: %s", bytesToHex(data)));
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing TOAST pointer: " + e.getMessage());
        }
    }

    /**
     * 解析固定长度数据
     */
    private void parseFixedLengthData(ByteBuffer buffer, int attrIndex, int endPos) {
        try {
            int remaining = buffer.remaining();
            int maxBytes = Math.min(remaining, endPos - buffer.position());

            LOGGER.info(String.format("              Fixed/Unknown type - %d bytes available", maxBytes));

            if (maxBytes >= 8) {
                // 尝试解析为8字节数据类型
                long longValue = buffer.getLong();
                LOGGER.info(String.format("              As Long: %d (0x%016X)", longValue, longValue));

                // 重置位置，尝试其他解析
                buffer.position(buffer.position() - 8);
            }

            if (maxBytes >= 4) {
                // 尝试解析为4字节数据类型
                int intValue = buffer.getInt();
                LOGGER.info(String.format("              As Int: %d (0x%08X)", intValue, intValue));

                // 重置位置，尝试其他解析
                buffer.position(buffer.position() - 4);
            }

            if (maxBytes >= 2) {
                // 尝试解析为2字节数据类型
                short shortValue = buffer.getShort();
                LOGGER.info(String.format("              As Short: %d (0x%04X)", shortValue, shortValue));

                // 重置位置，尝试其他解析
                buffer.position(buffer.position() - 2);
            }

            if (maxBytes >= 1) {
                // 尝试解析为1字节数据类型
                byte byteValue = buffer.get();
                LOGGER.info(String.format("              As Byte: %d (0x%02X)", byteValue, byteValue));

                // 重置位置
                buffer.position(buffer.position() - 1);
            }

            // 读取所有字节并显示十六进制
            byte[] allBytes = new byte[Math.min(maxBytes, 16)]; // 最多显示16字节
            buffer.get(allBytes);
            LOGGER.info(String.format("              Raw Hex: %s", bytesToHex(allBytes)));

            // 如果还有更多数据，继续读取
            if (maxBytes > 16) {
                byte[] moreBytes = new byte[maxBytes - 16];
                buffer.get(moreBytes);
                LOGGER.info(String.format("              More Hex: %s", bytesToHex(moreBytes)));
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing fixed length data: " + e.getMessage());
        }
    }

    /**
     * 将字节数组转换为十六进制字符串
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }

    /**
     * 解析堆表DELETE块数据
     */
    private void parseHeapDeleteData(ByteBuffer buffer, int dataLength) {
        try {
            LOGGER.info(String.format("        DELETE Block Data - Length: %d", dataLength));

            if (dataLength > 0) {
                // DELETE操作可能包含被删除元组的信息
                parseHeapTupleData(buffer, dataLength, "DELETE");
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing heap delete data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析堆表UPDATE块数据
     */
    private void parseHeapUpdateData(ByteBuffer buffer, int dataLength) {
        try {
            LOGGER.info(String.format("        UPDATE Block Data - Length: %d", dataLength));

            if (dataLength > 0) {
                // UPDATE操作包含新的元组数据
                parseHeapTupleData(buffer, dataLength, "UPDATE");
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing heap update data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析堆表INSERT主数据 - 完整解析xl_heap_insert结构
     */
    private void parseHeapInsertMainData(ByteBuffer buffer, int dataLength) {
        try {
            LOGGER.info(String.format("      INSERT Main Data - Length: %d", dataLength));

            if (dataLength == 0) {
                LOGGER.info("      Empty INSERT main data");
                return;
            }

            // 输出原始数据
            byte[] rawData = new byte[dataLength];
            buffer.get(rawData);
            buffer.position(buffer.position() - dataLength); // 重置位置
            LOGGER.info(String.format("      Raw Data: %s", bytesToHex(rawData)));

            // 解析xl_heap_insert结构
            if (dataLength >= 2) {
                int offnum = buffer.getShort() & 0xFFFF;  // 偏移号
                LOGGER.info(String.format("      INSERT - Offset Number: %d", offnum));
            }

            if (dataLength >= 3) {
                int flags = buffer.get() & 0xFF;
                LOGGER.info(String.format("      INSERT - Flags: 0x%02X", flags));

                // 解析标志位
                if ((flags & 0x01) != 0) LOGGER.info("        Flag: HEAP_INSERT_SKIP_WAL");
                if ((flags & 0x02) != 0) LOGGER.info("        Flag: HEAP_INSERT_IS_FROZEN");
                if ((flags & 0x04) != 0) LOGGER.info("        Flag: HEAP_INSERT_CONTAINS_NEW_TUPLE");
            }

            // 解析剩余数据
            int remaining = dataLength - Math.min(dataLength, 3);
            if (remaining > 0) {
                byte[] remainingData = new byte[remaining];
                buffer.get(remainingData);
                LOGGER.info(String.format("      Additional Data (%d bytes): %s", remaining, bytesToHex(remainingData)));
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing heap insert main data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 解析堆表DELETE主数据 - 完整解析xl_heap_delete结构
     */
    private void parseHeapDeleteMainData(ByteBuffer buffer, int dataLength) {
        try {
            LOGGER.info(String.format("      DELETE Main Data - Length: %d", dataLength));

            if (dataLength == 0) {
                LOGGER.info("      Empty DELETE main data");
                return;
            }

            // 输出原始数据
            byte[] rawData = new byte[dataLength];
            buffer.get(rawData);
            buffer.position(buffer.position() - dataLength); // 重置位置
            LOGGER.info(String.format("      Raw Data: %s", bytesToHex(rawData)));

            // 解析xl_heap_delete结构
            if (dataLength >= 2) {
                int offnum = buffer.getShort() & 0xFFFF;  // 偏移号
                LOGGER.info(String.format("      DELETE - Offset Number: %d", offnum));
            }

            if (dataLength >= 3) {
                int flags = buffer.get() & 0xFF;
                LOGGER.info(String.format("      DELETE - Flags: 0x%02X", flags));

                // 解析标志位
                if ((flags & 0x01) != 0) LOGGER.info("        Flag: HEAP_DELETE_IS_SUPER");
                if ((flags & 0x02) != 0) LOGGER.info("        Flag: HEAP_DELETE_IS_PARTITION_MOVE");
            }

            // 解析剩余数据
            int remaining = dataLength - Math.min(dataLength, 3);
            if (remaining > 0) {
                byte[] remainingData = new byte[remaining];
                buffer.get(remainingData);
                LOGGER.info(String.format("      Additional Data (%d bytes): %s", remaining, bytesToHex(remainingData)));
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing heap delete main data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 解析堆表UPDATE主数据 - 完整解析xl_heap_update结构
     */
    private void parseHeapUpdateMainData(ByteBuffer buffer, int dataLength) {
        try {
            LOGGER.info(String.format("      UPDATE Main Data - Length: %d", dataLength));

            if (dataLength == 0) {
                LOGGER.info("      Empty UPDATE main data");
                return;
            }

            // 输出原始数据
            byte[] rawData = new byte[dataLength];
            buffer.get(rawData);
            buffer.position(buffer.position() - dataLength); // 重置位置
            LOGGER.info(String.format("      Raw Data: %s", bytesToHex(rawData)));

            // 解析xl_heap_update结构
            if (dataLength >= 2) {
                int old_offnum = buffer.getShort() & 0xFFFF;  // 旧偏移号
                LOGGER.info(String.format("      UPDATE - Old Offset Number: %d", old_offnum));
            }

            if (dataLength >= 4) {
                int new_offnum = buffer.getShort() & 0xFFFF;  // 新偏移号
                LOGGER.info(String.format("      UPDATE - New Offset Number: %d", new_offnum));
            }

            if (dataLength >= 5) {
                int flags = buffer.get() & 0xFF;
                LOGGER.info(String.format("      UPDATE - Flags: 0x%02X", flags));

                // 解析标志位
                if ((flags & 0x01) != 0) LOGGER.info("        Flag: HEAP_UPDATE_PREFIX_FROM_OLD");
                if ((flags & 0x02) != 0) LOGGER.info("        Flag: HEAP_UPDATE_SUFFIX_FROM_OLD");
                if ((flags & 0x04) != 0) LOGGER.info("        Flag: HEAP_UPDATE_CONTAINS_OLD_TUPLE");
                if ((flags & 0x08) != 0) LOGGER.info("        Flag: HEAP_UPDATE_CONTAINS_OLD_KEY");
                if ((flags & 0x10) != 0) LOGGER.info("        Flag: HEAP_UPDATE_CONTAINS_NEW_TUPLE");
            }

            // 解析剩余数据
            int remaining = dataLength - Math.min(dataLength, 5);
            if (remaining > 0) {
                byte[] remainingData = new byte[remaining];
                buffer.get(remainingData);
                LOGGER.info(String.format("      Additional Data (%d bytes): %s", remaining, bytesToHex(remainingData)));
            }

        } catch (Exception e) {
            LOGGER.warning("Error parsing heap update main data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 解析事务块数据 - 完整解析
     */
    private void parseXactBlockData(ByteBuffer buffer, int info, int xid, int dataLength) {
        try {
            switch (info) {
                case XLOG_XACT_COMMIT:
                    LOGGER.info(String.format("      XACT COMMIT Block Data - XID: %d, Length: %d", xid, dataLength));
                    break;
                case XLOG_XACT_ABORT:
                    LOGGER.info(String.format("      XACT ABORT Block Data - XID: %d, Length: %d", xid, dataLength));
                    break;
                default:
                    LOGGER.info(String.format("      Unknown xact block data: 0x%02X, XID: %d, Length: %d",
                            info, xid, dataLength));
                    break;
            }

            if (dataLength > 0) {
                // 输出原始数据
                byte[] rawData = new byte[dataLength];
                buffer.get(rawData);
                LOGGER.info(String.format("      Block Raw Data: %s", bytesToHex(rawData)));
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing xact block data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 解析事务主数据
     */
    private void parseXactMainData(ByteBuffer buffer, int info, int xid, int dataLength) {
        try {
            switch (info) {
                case XLOG_XACT_COMMIT:
                    LOGGER.info(String.format("    XACT COMMIT Main Data - XID: %d", xid));
                    parseXactCommitMainData(buffer, dataLength);
                    break;
                case XLOG_XACT_ABORT:
                    LOGGER.info(String.format("    XACT ABORT Main Data - XID: %d", xid));
                    parseXactAbortMainData(buffer, dataLength);
                    break;
                default:
                    LOGGER.fine("    Unknown xact main data: 0x" + Integer.toHexString(info));
                    skipData(buffer, dataLength);
                    break;
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing xact main data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析事务提交主数据
     */
    private void parseXactCommitMainData(ByteBuffer buffer, int dataLength) {
        try {
            if (dataLength >= 8) {
                long timestamp = buffer.getLong();
                LOGGER.info(String.format("      COMMIT - Timestamp: %d", timestamp));

                int remaining = dataLength - 8;
                if (remaining > 0) {
                    skipData(buffer, remaining);
                }
            } else {
                skipData(buffer, dataLength);
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing xact commit main data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 解析事务回滚主数据
     */
    private void parseXactAbortMainData(ByteBuffer buffer, int dataLength) {
        try {
            if (dataLength >= 8) {
                long timestamp = buffer.getLong();
                LOGGER.info(String.format("      ABORT - Timestamp: %d", timestamp));

                int remaining = dataLength - 8;
                if (remaining > 0) {
                    skipData(buffer, remaining);
                }
            } else {
                skipData(buffer, dataLength);
            }
        } catch (Exception e) {
            LOGGER.warning("Error parsing xact abort main data: " + e.getMessage());
            skipData(buffer, dataLength);
        }
    }

    /**
     * 跳过指定长度的数据
     */
    private void skipData(ByteBuffer buffer, int length) {
        if (length > 0 && buffer.remaining() >= length) {
            int actualSkip = Math.min(length, buffer.remaining());
            buffer.position(buffer.position() + actualSkip);
            if (actualSkip < length) {
                LOGGER.warning("Could only skip " + actualSkip + " bytes out of " + length + " requested");
            }
        }
    }

    /**
     * 清理资源
     */
    private void cleanup() {
        LOGGER.info("Cleaning up physical replication resources...");
        // 物理复制不需要复制槽，所以清理工作较少
    }

}