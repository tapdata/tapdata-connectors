package io.tapdata.mongodb.decoder;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Date;

public class CustomDocumentTest {

    @Nested
    class ParseTest {

        @Test
        void testParse() {
            Document parse = CustomDocument.parse("");
            Assertions.assertNotNull(parse);
        }
        @Test
        void testNullJsonParse() {
            Document parse = CustomDocument.parse(null);
            Assertions.assertNotNull(parse);
        }
        @Test
        void testNormalJsonParse() {
            Document parse = CustomDocument.parse("{\"create\":{\"$dynamicDate\":{\"format\":\"2024-03-07\"}}}");
            Assertions.assertNotNull(parse);
            Assertions.assertNotNull(parse.get("create"));
            Assertions.assertEquals(Date.class.getName(), parse.get("create").getClass().getName());
        }
        @Test
        void testJsonParseCovertToTimestamp(){
            Document parse = CustomDocument.parse("{\"create\":{\"$dateToTimestamp\": {\"$dynamicDate\":{\"format\":\"2024-03-07\"}}}}");
            Assertions.assertNotNull(parse);
            Assertions.assertNotNull(parse.get("create"));
            Assertions.assertEquals(Long.class.getName(), parse.get("create").getClass().getName());
        }
    }
}
