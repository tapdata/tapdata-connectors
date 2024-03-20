package org.bson.codecs;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class CustomDocumentCodecProviderTest {
    @Nested
    class GetTest {
        CustomDocumentCodecProvider provider;
        @BeforeEach
        void init() {
            provider = new CustomDocumentCodecProvider();
        }

        @Test
        void testDocument() {
            Codec<Document> documentCodec = provider.get(Document.class, null);
            Assertions.assertNotNull(documentCodec);
            Assertions.assertEquals(CustomDocumentDecoder.class.getName(), documentCodec.getClass().getName());
        }

        @Test
        void testNotDocument() {
            Codec<Document> documentCodec = provider.get(null, null);
            Assertions.assertNull(documentCodec);
        }
    }
}
