package io.tapdata.connector.postgres.cdc.physical;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class WalByteReaderTest {

    @Test
    public void testLittleEndianReads() {
        byte[] b = new byte[]{
                0x12,                         // uint8
                0x34, 0x12,                   // uint16 -> 0x1234
                0x78, 0x56, 0x34, 0x12,       // uint32 -> 0x12345678
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x00, 0x00, 0x00, 0x00 // uint32 max then 0
        };
        WalByteReader r = new WalByteReader(b);
        assertEquals(0x12, r.readUInt8());
        assertEquals(0x1234, r.readUInt16());
        assertEquals(0x12345678L, r.readUInt32());
        assertEquals(0xFFFFFFFFL, r.readUInt32());
        assertEquals(0L, r.readUInt32());
    }

    @Test
    public void testInt64() {
        byte[] b = new byte[]{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        assertEquals(1L, new WalByteReader(b).readInt64());
        byte[] big = new byte[]{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00};
        assertEquals(0x100000000L, new WalByteReader(big).readInt64());
    }

    @Test
    public void testAlignment() {
        byte[] b = new byte[32];
        WalByteReader r = new WalByteReader(b);
        r.skip(1);
        r.align(4);
        assertEquals(4, r.position());
        r.skip(1);
        r.align(8);
        assertEquals(8, r.position());
        r.skip(1);
        r.alignMaxAlign();
        assertEquals(16, r.position());
        r.align(1);
        assertEquals(16, r.position());
    }

    @Test
    public void testOffsetWindow() {
        byte[] b = new byte[]{0, 0, 0x34, 0x12, 0};
        WalByteReader r = new WalByteReader(b, 2, 2);
        assertEquals(0x1234, r.readUInt16());
        assertEquals(0, r.remaining());
        assertThrows(IndexOutOfBoundsException.class, r::readUInt8);
    }

    @Test
    public void testMaxAlignHelper() {
        assertEquals(0, WalConstants.maxAlign(0));
        assertEquals(8, WalConstants.maxAlign(1));
        assertEquals(8, WalConstants.maxAlign(8));
        assertEquals(16, WalConstants.maxAlign(9));
    }
}
