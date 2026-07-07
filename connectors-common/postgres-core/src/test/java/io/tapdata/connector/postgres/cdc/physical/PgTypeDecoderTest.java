package io.tapdata.connector.postgres.cdc.physical;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

public class PgTypeDecoderTest {

    private static byte[] le16(int v) {
        return new byte[]{(byte) (v & 0xFF), (byte) ((v >> 8) & 0xFF)};
    }

    private static byte[] le32(long v) {
        return new byte[]{(byte) v, (byte) (v >> 8), (byte) (v >> 16), (byte) (v >> 24)};
    }

    @Test
    public void testIntegerFamily() {
        assertEquals(true, PgTypeDecoder.decode(PgTypeDecoder.BOOL, new byte[]{1}));
        assertEquals(-1, PgTypeDecoder.decode(PgTypeDecoder.INT2, le16(0xFFFF)));
        assertEquals(258, PgTypeDecoder.decode(PgTypeDecoder.INT4, le32(258)));
    }

    @Test
    public void testFloatAndText() {
        byte[] f = le32(Float.floatToIntBits(1.5f));
        assertEquals(1.5f, PgTypeDecoder.decode(PgTypeDecoder.FLOAT4, f));
        assertEquals("héllo", PgTypeDecoder.decode(PgTypeDecoder.TEXT, "héllo".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testNamePadStripped() {
        // "name" is a fixed NAMEDATALEN buffer: the value is NUL-terminated and
        // padded with NULs, which must not leak into the decoded string.
        byte[] buf = new byte[64];
        byte[] name = "id".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(name, 0, buf, 0, name.length);
        assertEquals("id", PgTypeDecoder.decode(PgTypeDecoder.NAME, buf));
    }

    @Test
    public void testDate() {
        // 2000-01-02 -> 1 day after PG epoch
        assertEquals(LocalDate.of(2000, 1, 2), PgTypeDecoder.decode(PgTypeDecoder.DATE, le32(1)));
    }

    @Test
    public void testNumericLongForm() {
        // on-disk NumericLong for 1234.5: header = sign(pos) | dscale(1) = 0x0001,
        // weight = 0, digits = {1234, 5000} (base-10000): 1234 + 5000*10^-4.
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        write16(o, 0x0001); // n_sign_dscale: positive, dscale = 1
        write16(o, 0);      // n_weight
        write16(o, 1234);
        write16(o, 5000);
        BigDecimal d = PgTypeDecoder.decodeNumeric(o.toByteArray());
        assertEquals(0, new BigDecimal("1234.5").compareTo(d));
        assertEquals(1, d.scale());
    }

    @Test
    public void testNumericLongFormNegative() {
        // -1234.5 : header = NEG(0x4000) | dscale(1)
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        write16(o, 0x4000 | 0x0001);
        write16(o, 0);      // weight
        write16(o, 1234);
        write16(o, 5000);
        BigDecimal d = PgTypeDecoder.decodeNumeric(o.toByteArray());
        assertEquals(0, new BigDecimal("-1234.5").compareTo(d));
    }

    @Test
    public void testNumericShortForm() {
        // on-disk NumericShort for 5: header = NUMERIC_SHORT(0x8000), one digit {5}.
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        write16(o, 0x8000); // short, positive, dscale 0, weight 0
        write16(o, 5);
        BigDecimal d = PgTypeDecoder.decodeNumeric(o.toByteArray());
        assertEquals(0, new BigDecimal("5").compareTo(d));
    }

    @Test
    public void testNumericShortFormNegativeWeight() {
        // 0.5 : short header with dscale=1 and weight=-1; single digit {5000}
        // value = 5000 * 10000^-1 = 0.5
        int header = 0x8000
                | (1 << 7)            // dscale = 1
                | 0x0040              // weight sign bit (negative)
                | (0x3F & -1);        // weight = -1 (6-bit)
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        write16(o, header);
        write16(o, 5000);
        BigDecimal d = PgTypeDecoder.decodeNumeric(o.toByteArray());
        assertEquals(0, new BigDecimal("0.5").compareTo(d));
    }

    @Test
    public void testNumericZero() {
        // zero: long header dscale 0, weight 0, no digits
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        write16(o, 0x0000);
        write16(o, 0);
        BigDecimal d = PgTypeDecoder.decodeNumeric(o.toByteArray());
        assertEquals(0, BigDecimal.ZERO.compareTo(d));
    }

    @Test
    public void testJsonbObject() {
        // Build jsonb {"sdkf":"sfs"} on-disk bytes (after varlena strip):
        // version(1) + header(4 LE) + key_entry(4 LE) + val_entry(4 LE) + data
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        o.write(1); // version
        // header: JB_FOBJECT(4) | nJEntries=2
        write32(o, (4 << 28) | 2);
        // key JEntry: IS_STRING | len=4
        write32(o, (2 << 24) | 4);
        // val JEntry: IS_STRING | len=3
        write32(o, (2 << 24) | 3);
        // data: "sdkf" + "sfs"
        o.write("sdkfsfs".getBytes(StandardCharsets.UTF_8), 0, 7);

        Object result = PgTypeDecoder.decode(PgTypeDecoder.JSONB, o.toByteArray());
        assertNotNull(result, "jsonb decode should not return null");
        assertEquals("{\"sdkf\":\"sfs\"}", result);
    }

    @Test
    public void testJsonbArray() {
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        o.write(1); // version
        // header: JB_FARRAY(8) | nJEntries=3
        write32(o, (8 << 28) | 3);
        // 3 JEntries: IS_STRING | len=1 for "a","b","c"
        write32(o, (2 << 24) | 1);
        write32(o, (2 << 24) | 1);
        write32(o, (2 << 24) | 1);
        o.write("abc".getBytes(StandardCharsets.UTF_8), 0, 3);

        Object result = PgTypeDecoder.decode(PgTypeDecoder.JSONB, o.toByteArray());
        assertNotNull(result);
        assertEquals("[\"a\",\"b\",\"c\"]", result);
    }

    @Test
    public void testJsonbNull() {
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        o.write(1); // version
        // header: JB_FOBJECT(4) | nJEntries=2
        write32(o, (4 << 28) | 2);
        // key: IS_STRING | len=1 ("x")
        write32(o, (2 << 24) | 1);
        // val: IS_NULL
        write32(o, 8 << 24);
        o.write("x".getBytes(StandardCharsets.UTF_8), 0, 1);

        Object result = PgTypeDecoder.decode(PgTypeDecoder.JSONB, o.toByteArray());
        assertNotNull(result);
        assertEquals("{\"x\":null}", result);
    }

    @Test
    public void testPoint() {
        // point(12.0, 12.0) → 16 bytes: two float64 LE
        byte[] buf = new byte[16];
        long xBits = Double.doubleToLongBits(12.0);
        long yBits = Double.doubleToLongBits(12.0);
        for (int i = 0; i < 8; i++) {
            buf[i] = (byte) (xBits >>> (8 * i));
            buf[8 + i] = (byte) (yBits >>> (8 * i));
        }
        Object result = PgTypeDecoder.decode(PgTypeDecoder.POINT, buf);
        assertEquals("(12.0,12.0)", result);
    }

    private static void write32(java.io.ByteArrayOutputStream o, int v) {
        o.write(v & 0xFF);
        o.write((v >> 8) & 0xFF);
        o.write((v >> 16) & 0xFF);
        o.write((v >> 24) & 0xFF);
    }

    private static void write16(java.io.ByteArrayOutputStream o, int v) {
        o.write(v & 0xFF);
        o.write((v >> 8) & 0xFF);
    }

    @Test
    void testDecodePath() {
        // PATH on-disk: npts(4) + closed(4) + dummy(4) + points(3*16) = 60 bytes
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        write32(o, 3); // npts
        write32(o, 0); // closed=0 (open)
        write32(o, 0); // dummy
        for (int i = 0; i < 3; i++) {
            double[] pts = {0.0, 0.0, 10.0, 10.0, 20.0, 0.0};
            long xBits = Double.doubleToLongBits(pts[i * 2]);
            long yBits = Double.doubleToLongBits(pts[i * 2 + 1]);
            for (int j = 0; j < 8; j++) {
                o.write((byte) (xBits >>> (8 * j)));
            }
            for (int j = 0; j < 8; j++) {
                o.write((byte) (yBits >>> (8 * j)));
            }
        }
        Object result = PgTypeDecoder.decode(PgTypeDecoder.PATH, o.toByteArray());
        assertEquals("[(0.0,0.0),(10.0,10.0),(20.0,0.0)]", result);
    }

    @Test
    void testDecodePolygon() {
        // POLYGON on-disk: npts(4) + boundbox(32) + points(3*16) = 84 bytes
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        write32(o, 3); // npts
        // boundbox: high=(20,10), low=(0,0)
        for (double v : new double[]{20.0, 10.0, 0.0, 0.0}) {
            long bits = Double.doubleToLongBits(v);
            for (int j = 0; j < 8; j++) o.write((byte) (bits >>> (8 * j)));
        }
        // points
        for (double[] pt : new double[][]{{0, 0}, {10, 10}, {20, 0}}) {
            for (double v : pt) {
                long bits = Double.doubleToLongBits(v);
                for (int j = 0; j < 8; j++) o.write((byte) (bits >>> (8 * j)));
            }
        }
        Object result = PgTypeDecoder.decode(PgTypeDecoder.POLYGON, o.toByteArray());
        assertEquals("((0.0,0.0),(10.0,10.0),(20.0,0.0))", result);
    }

    @Test
    void testDecodeInet() {
        // Real PG on-disk format: family(1) + bits(1) + ipaddr[4|16] — no is_cidr byte
        // is_cidr only exists in wire-protocol binary, not on-disk.
        byte[] cidr = {2, 24, (byte) 192, (byte) 168, 1, 0};
        assertEquals("192.168.1.0/24", PgTypeDecoder.decode(PgTypeDecoder.CIDR, cidr));

        byte[] inet = {2, 32, 10, 0, 0, 1};
        assertEquals("10.0.0.1/32", PgTypeDecoder.decode(PgTypeDecoder.INET, inet));
    }

    @Test
    void testDecodeTsVector() {
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        write32(o, 2); // size
        // WordEntry: haspos(1) + len(11) + pos(20), LSB-aligned
        // we[0]: haspos=0, len=5, pos=0 → (5 << 1) | 0 = 10 = 0x0a
        write32(o, 10);
        // we[1]: haspos=0, len=5, pos=5 → (5 << 1) | (5 << 12) = 10 + 20480 = 20490 = 0x500a
        write32(o, 20490);
        // string data: "helloworld" (no null terminators needed with pos+len)
        try { o.write("helloworld".getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
        catch (java.io.IOException e) { throw new RuntimeException(e); }
        Object result = PgTypeDecoder.decode(PgTypeDecoder.TSVECTOR, o.toByteArray());
        assertEquals("hello world", result);
    }
}
