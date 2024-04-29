package io.tapdata.connector.doris.streamload;

import io.tapdata.connector.doris.streamload.CsvSerializer;
import io.tapdata.connector.doris.streamload.JsonSerializer;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.*;

import java.util.Collections;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

@DisplayName("MessageSerializer Implementation Class Test")
public class MessageSerializerTest {

    TapInsertRecordEvent tapInsertRecordEvent;
    TapUpdateRecordEvent tapUpdateRecordEventAll;
    TapUpdateRecordEvent tapUpdateRecordEventPrimary;
    TapDeleteRecordEvent tapDeleteRecordEvent;
    TapTable tapTable;

    @BeforeEach
    void init() {
        tapInsertRecordEvent = new TapInsertRecordEvent().init().after(
                map(
                        entry("id", 1),
                        entry("name", "jarad"),
                        entry("age", 18)
                ));
        tapUpdateRecordEventAll = new TapUpdateRecordEvent().init().before(
                map(
                        entry("id", 1),
                        entry("name", "jarad"),
                        entry("age", 18)
                )).after(
                map(
                        entry("id", 1),
                        entry("name", "jarad"),
                        entry("age", 19)
                ));
        tapUpdateRecordEventPrimary = new TapUpdateRecordEvent().init().before(
                map(
                        entry("id", 1)
                )).after(
                map(
                        entry("name", "jarad"),
                        entry("age", 19)
                ));
        tapDeleteRecordEvent = new TapDeleteRecordEvent().init().before(
                map(
                        entry("id", 1),
                        entry("name", "jarad")
                ));
        tapTable = new TapTable("test", "test")
                .add(new TapField("id", "int"))
                .add(new TapField("name", "string"))
                .add(new TapField("age", "int"));
    }

    @Nested
    class JsonSerializerTest {

        JsonSerializer jsonSerializer = new JsonSerializer();

        @Test
        void testSerializeInsert() throws Throwable {
            Assertions.assertThrows(IllegalArgumentException.class, () -> jsonSerializer.serialize(null, tapInsertRecordEvent, false));
            Assertions.assertEquals("{\"id\":\"1\",\"name\":\"jarad\",\"age\":\"18\",\"__DORIS_DELETE_SIGN__\":0}", new String(jsonSerializer.serialize(tapTable, tapInsertRecordEvent, false)));
        }

        @Test
        void testSerializeUpdate() throws Throwable {
            Assertions.assertEquals("{\"id\":\"1\",\"name\":\"jarad\",\"age\":\"19\",\"__DORIS_DELETE_SIGN__\":0}", new String(jsonSerializer.serialize(tapTable, tapUpdateRecordEventAll, false)));
            Assertions.assertEquals("{\"name\":\"jarad\",\"age\":\"19\",\"__DORIS_DELETE_SIGN__\":0}", new String(jsonSerializer.serialize(tapTable, tapUpdateRecordEventPrimary, false)));
        }

        @Test
        void testSerializeDelete() throws Throwable {
            Assertions.assertEquals("{\"id\":\"1\",\"name\":\"jarad\",\"__DORIS_DELETE_SIGN__\":1}", new String(jsonSerializer.serialize(tapTable, tapDeleteRecordEvent, false)));
            Assertions.assertEquals("{\"id\":\"1\",\"name\":\"jarad\",\"age\":null,\"__DORIS_DELETE_SIGN__\":1}", new String(jsonSerializer.serialize(tapTable, tapDeleteRecordEvent, true)));
        }
    }

    @Nested
    class CsvSerializerTest {

        CsvSerializer csvSerializer = new CsvSerializer();

        @Test
        void testSerializeInsert() throws Throwable {
            Assertions.assertEquals("1||%%||jarad||%%||18||%%||0", new String(csvSerializer.serialize(tapTable, tapInsertRecordEvent, false)));
        }

        @Test
        void testSerializeUpdate() throws Throwable {
            Assertions.assertEquals("1||%%||jarad||%%||19||%%||0", new String(csvSerializer.serialize(tapTable, tapUpdateRecordEventAll, false)));
            Assertions.assertEquals("jarad||%%||19||%%||0", new String(csvSerializer.serialize(tapTable, tapUpdateRecordEventPrimary, false)));
        }

        @Test
        void testSerializeDelete() throws Throwable {
            Assertions.assertEquals("1||%%||jarad||%%||1", new String(csvSerializer.serialize(tapTable, tapDeleteRecordEvent, false)));
            Assertions.assertEquals("1||%%||jarad||%%||\\N||%%||1", new String(csvSerializer.serialize(tapTable, tapDeleteRecordEvent, true)));
        }

        @Test
        void testSerializeBlank() throws Throwable {
            Assertions.assertEquals("", new String(csvSerializer.serialize(tapTable, new TapInsertRecordEvent().init().after(Collections.emptyMap()), false)));
        }
    }
}
