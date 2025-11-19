package io.tapdata.connector.mysql.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class StringCompressUtilTest {

    @Test
    void compressJson() {
        String str = "{}";
        Assertions.assertDoesNotThrow(() -> {
            byte[] compress = StringCompressUtil.compress(str);
            String uncompress = StringCompressUtil.uncompress(compress);
            Assertions.assertEquals(str, uncompress);
        });
    }

    @Test
    void compressNull() {
        Assertions.assertDoesNotThrow(() -> {
            byte[] compress = StringCompressUtil.compress(null);
            String uncompress = StringCompressUtil.uncompress(compress);
            Assertions.assertNull(uncompress);
        });
    }

    @Test
    void compressEmpty() {
        String str = "";
        Assertions.assertDoesNotThrow(() -> {
            byte[] compress = StringCompressUtil.compress(str);
            String uncompress = StringCompressUtil.uncompress(compress);
            Assertions.assertEquals(str, uncompress);
        });
    }
}