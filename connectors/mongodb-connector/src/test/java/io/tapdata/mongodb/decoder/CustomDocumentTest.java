package io.tapdata.mongodb.decoder;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.mongodb.decoder.impl.DateTimeReader;
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
        @DisplayName("DateTimeReader test handle DateTime")
        void testHandleDateTime() {
            Map<String, Object> map = new HashMap<>();
            map.put("_date", new HashMap<String, Object>() {{
                put(DateTimeReader.DATE_TIME_KEY, new DateTime(Instant.ofEpochSecond(1717151086).plusNanos(438000000)));
            }});
            Document parse = CustomDocument.parse(TapSimplify.toJson(map, JsonParser.ToJsonFeature.WriteMapNullValue));
            assertNotNull(parse);
            assertNotNull(parse.get("_date"));
            Object result = parse.get("_date");
            assertInstanceOf(Instant.class, result);
            assertEquals(1717151086, ((Instant) result).getEpochSecond());
            assertEquals(438000000, ((Instant) result).getNano());

            parse = CustomDocument.parse("{\"_date\":{\"" + DateTimeReader.DATE_TIME_KEY + "\":{\"seconds\":\"test error string\"}}}");
            assertNotNull(parse);
            assertNotNull(parse.get("_date"));
            assertInstanceOf(Document.class, parse.get("_date"));
            assertEquals("test error string", ((Document) parse.get("_date")).get("seconds"));
        }

        @Test
        @DisplayName("DateTimeReader test handle String")
        void testHandleString() {
            Map<String, Object> map = new HashMap<>();
            map.put("_date1", new HashMap<String, Object>() {{
                put(DateTimeReader.DATE_TIME_KEY, "2024-05-31T17:59:01.123Z");
            }});
			map.put("_date2", new HashMap<String, Object>() {{
				put(DateTimeReader.DATE_TIME_KEY, "test");
			}});
			map.put("_date3", new HashMap<String, Object>() {{
				put(DateTimeReader.DATE_TIME_KEY, "2024-05-31 17:59:01");
			}});
            Document parse = CustomDocument.parse(TapSimplify.toJson(map, JsonParser.ToJsonFeature.WriteMapNullValue));
            assertNotNull(parse);
            assertNotNull(parse.get("_date1"));
            assertInstanceOf(Instant.class, parse.get("_date1"));
            assertEquals("2024-05-31T17:59:01.123Z", ((Instant) parse.get("_date1")).toString());
			assertNotNull(parse.get("_date2"));
			assertInstanceOf(String.class, parse.get("_date2"));
			assertEquals("test", parse.get("_date2"));
            assertEquals("test", parse.get("_date2"));
			assertNotNull(parse.get("_date3"));
			assertInstanceOf(Instant.class, parse.get("_date3"));
			assertEquals("2024-05-31T17:59:01Z", ((Instant) parse.get("_date3")).toString());
        }
    }
}
