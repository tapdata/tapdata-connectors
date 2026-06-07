package io.tapdata.connector.postgres.cdc.physical;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class XLogRecordParserTest {

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
    public void testParseInsertShapedRecord() {
        int d = 10;     // block-0 data length
        int m = 5;      // main data length
        byte[] blockData = new byte[d];
        for (int i = 0; i < d; i++) {
            blockData[i] = (byte) (0xA0 + i);
        }
        byte[] mainData = new byte[m];
        for (int i = 0; i < m; i++) {
            mainData[i] = (byte) (0x10 + i);
        }

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        // block 0 reference header
        body.write(0);                                  // block id
        body.write(BKPBLOCK_HAS_DATA);                  // fork_flags
        u16(body, d);                                   // data_length
        u32(body, 1663);                                // spcOid
        u32(body, 5);                                   // dbOid
        u32(body, 16384);                               // relNumber
        u32(body, 42);                                  // blockNumber
        // main data short header
        body.write(XLR_BLOCK_ID_DATA_SHORT);
        body.write(m);
        // data section: block data then main data
        body.write(blockData, 0, d);
        body.write(mainData, 0, m);
        byte[] bodyBytes = body.toByteArray();

        ByteArrayOutputStream rec = new ByteArrayOutputStream();
        u32(rec, SIZE_OF_XLOG_RECORD + bodyBytes.length); // tot_len
        u32(rec, 12345);                                  // xid
        u64(rec, 0);                                      // prev
        rec.write(XLOG_HEAP_INSERT);                      // info
        rec.write(RM_HEAP_ID);                            // rmid
        u16(rec, 0);                                      // padding
        u32(rec, 0);                                      // crc
        rec.write(bodyBytes, 0, bodyBytes.length);

        WalPageDecoder.RawRecord raw = new WalPageDecoder.RawRecord(0x1000, 0x1100, rec.toByteArray());
        XLogRecord parsed = XLogRecord.parse(raw);

        assertEquals(RM_HEAP_ID, parsed.rmid);
        assertEquals(XLOG_HEAP_INSERT, parsed.heapOp());
        assertEquals(12345, parsed.xid);
        assertEquals(1, parsed.blocks.size());
        XLogRecord.BlockRef b = parsed.block(0);
        assertNotNull(b);
        assertEquals(1663, b.spcOid);
        assertEquals(5, b.dbOid);
        assertEquals(16384, b.relNumber);
        assertEquals(42, b.blockNumber);
        assertTrue(b.hasData);
        assertArrayEquals(blockData, b.data);
        assertArrayEquals(mainData, parsed.mainData);
    }

    @Test
    public void testSameRelInheritsLocator() {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        // block 0 with rel locator
        body.write(0);
        body.write(0);                  // no data, no image
        u16(body, 0);
        u32(body, 100);
        u32(body, 200);
        u32(body, 300);
        u32(body, 7);                   // blockNumber
        // block 1 SAME_REL
        body.write(1);
        body.write(BKPBLOCK_SAME_REL);
        u16(body, 0);
        u32(body, 9);                   // blockNumber (no locator)
        byte[] bodyBytes = body.toByteArray();

        ByteArrayOutputStream rec = new ByteArrayOutputStream();
        u32(rec, SIZE_OF_XLOG_RECORD + bodyBytes.length);
        u32(rec, 1);
        u64(rec, 0);
        rec.write(XLOG_HEAP_UPDATE);
        rec.write(RM_HEAP_ID);
        u16(rec, 0);
        u32(rec, 0);
        rec.write(bodyBytes, 0, bodyBytes.length);

        XLogRecord parsed = XLogRecord.parse(new WalPageDecoder.RawRecord(0, 0, rec.toByteArray()));
        XLogRecord.BlockRef b1 = parsed.block(1);
        assertNotNull(b1);
        assertEquals(100, b1.spcOid);
        assertEquals(200, b1.dbOid);
        assertEquals(300, b1.relNumber);
        assertEquals(9, b1.blockNumber);
    }
}
