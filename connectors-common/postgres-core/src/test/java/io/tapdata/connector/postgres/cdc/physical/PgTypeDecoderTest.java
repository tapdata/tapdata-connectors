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

    private static void write16(java.io.ByteArrayOutputStream o, int v) {
        o.write(v & 0xFF);
        o.write((v >> 8) & 0xFF);
    }
}
