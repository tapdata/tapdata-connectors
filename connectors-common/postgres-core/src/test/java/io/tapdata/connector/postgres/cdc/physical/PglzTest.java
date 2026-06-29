package io.tapdata.connector.postgres.cdc.physical;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class PglzTest {

    @Test
    public void testLiteralOnly() {
        byte[] raw = "ABCDEFGH".getBytes(StandardCharsets.US_ASCII);
        // single control byte 0x00 -> 8 literals follow
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(0x00);
        o.write(raw, 0, raw.length);
        byte[] out = Pglz.decompress(o.toByteArray(), raw.length);
        assertArrayEquals(raw, out);
    }

    @Test
    public void testBackReference() {
        // produce "ababab" : literals 'a','b' then a copy len=4 off=2
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        // control: bit0=0 literal a, bit1=0 literal b, bit2=1 copy -> ctrl=0b100=0x04
        o.write(0x04);
        o.write('a');
        o.write('b');
        // copy tag: len=(b0&0x0f)+3 ; off=((b0&0xf0)<<4)|b1
        // want len=4 -> b0 low nibble = 1 ; off=2 -> b1=2, b0 high nibble=0
        o.write(0x01);
        o.write(0x02);
        byte[] out = Pglz.decompress(o.toByteArray(), 6);
        assertEquals("ababab", new String(out, StandardCharsets.US_ASCII));
    }
}
