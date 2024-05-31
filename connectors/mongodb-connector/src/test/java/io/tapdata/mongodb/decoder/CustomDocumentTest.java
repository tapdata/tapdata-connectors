package io.tapdata.mongodb.decoder;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JsonParser;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CustomDocumentTest {

    @Nested
    class ParseTest {

        @Test
        void testParse() {
            Document parse = CustomDocument.parse("");
            assertNotNull(parse);
        }
        @Test
        void testNullJsonParse() {
            Document parse = CustomDocument.parse(null);
            assertNotNull(parse);
        }
        @Test
        void testNormalJsonParse() {
            Document parse = CustomDocument.parse("{\"create\":{\"$dynamicDate\":{\"format\":\"2024-03-07\"}}}");
            assertNotNull(parse);
            assertNotNull(parse.get("create"));
            Assertions.assertEquals(Date.class.getName(), parse.get("create").getClass().getName());
        }
        @Test
        void testJsonParseCovertToTimestamp(){
            Document parse = CustomDocument.parse("{\"create\":{\"$dateToTimestamp\": {\"$dynamicDate\":{\"format\":\"2024-03-07\"}}}}");
            assertNotNull(parse);
            assertNotNull(parse.get("create"));
            Assertions.assertEquals(Long.class.getName(), parse.get("create").getClass().getName());
        }

        @Test
        @DisplayName("test DateTimeReader")
        void testDateTime() {
            Document parse = CustomDocument.parse("{\"valDate\":{\"$dateTime\":{\"fraction\":3,\"nano\":438000000,\"originType\":90,\"seconds\":1717151086,\"timeZone\":null}}}");
            assertNotNull(parse);
            assertNotNull(parse.get("valDate"));
            Object valDate = parse.get("valDate");
            assertInstanceOf(Instant.class, valDate);
            assertEquals(1717151086, ((Instant) valDate).getEpochSecond());
            assertEquals(438000000, ((Instant) valDate).getNano());

            Document parse1 = CustomDocument.parse("{\"valDate\": {\"$dateTime\": \"test\"}}");
            assertNotNull(parse1);
            assertNotNull(parse1.get("valDate"));
            assertInstanceOf(String.class, parse1.get("valDate"));
            assertEquals("test", parse1.get("valDate"));
        }
    }
}
