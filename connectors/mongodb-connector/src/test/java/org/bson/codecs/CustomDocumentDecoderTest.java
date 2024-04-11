package org.bson.codecs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class CustomDocumentDecoderTest {
    @Nested
    class RegisterCustomReaderTest {
        @Test
        void registerNullTag() {
            CustomDocumentDecoder decoder = new CustomDocumentDecoder();
            Assertions.assertNotNull(decoder.registerCustomReader(null, null));
        }

        @Test
        void registerNullValue() {
            CustomDocumentDecoder decoder = new CustomDocumentDecoder();
            Assertions.assertNotNull(decoder.registerCustomReader("null", null));
        }
    }
}
