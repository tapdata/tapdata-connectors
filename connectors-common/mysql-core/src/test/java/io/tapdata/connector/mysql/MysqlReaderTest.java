package io.tapdata.connector.mysql;

import io.debezium.type.TapIllegalDate;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.entity.MysqlStreamEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.core.api.impl.JsonParserImpl;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class MysqlReaderTest {

    @Nested
    class wrapDDL{
        MysqlReader mysqlReader = new MysqlReader(mock(MysqlJdbcContextV2.class),mock(Log.class),()->{return true;});
        @Test
        void test(){
            SourceRecord sourceRecord = mock(SourceRecord.class);
            Struct struct = mock(Struct.class);
            when(sourceRecord.value()).thenReturn(struct);
            when(struct.getStruct(anyString())).thenReturn(mock(Struct.class));
            when(struct.getString(anyString())).thenReturn("ALTER TABLE \"C##TAPDATA\".\"TT_DDL\" \n" +
                    "ADD (\"TT\" VARCHAR2(255));");
            List<MysqlStreamEvent> result = mysqlReader.wrapDDL(sourceRecord);
            TapDDLEvent resultEvent =(TapDDLEvent) result.get(0).getTapEvent();
            assertEquals("ALTER TABLE \"C##TAPDATA\".\"TT_DDL\" \n" +
                    "ADD (\"TT\" VARCHAR2(255));",resultEvent.getOriginDDL());
        }
        @Test
        void testUnknownEvent(){
            SourceRecord sourceRecord = mock(SourceRecord.class);
            Struct struct = mock(Struct.class);
            when(sourceRecord.value()).thenReturn(struct);
            when(struct.getStruct(anyString())).thenReturn(mock(Struct.class));
            when(struct.getString(anyString())).thenReturn("Unknown DDL");
            try(MockedStatic<DDLFactory> ddlFactoryMockedStatic = Mockito.mockStatic(DDLFactory.class)){
                ddlFactoryMockedStatic.when(()->DDLFactory.ddlToTapDDLEvent(any(),any(),any(),any(),any())).thenThrow(new Throwable("ERROR"));
                List<MysqlStreamEvent> result = mysqlReader.wrapDDL(sourceRecord);
                TapDDLEvent resultEvent =(TapDDLEvent) result.get(0).getTapEvent();
                assertEquals("Unknown DDL",resultEvent.getOriginDDL());
            }
        }

    }
    @Nested
    class WrapDMLTest{
        private MysqlReader mysqlReader;
        private SourceRecord record;
        private byte[] bytes;
        private Struct value;
        @BeforeEach
        void beforeEach(){
            MysqlJdbcContextV2 jdbcContextV2 = mock(MysqlJdbcContextV2.class);
            when(jdbcContextV2.getConfig()).thenReturn(mock(MysqlConfig.class));
            mysqlReader = new MysqlReader(jdbcContextV2,mock(Log.class),()->{return true;});
            mysqlReader = spy(mysqlReader);
            record = mock(SourceRecord.class);
            when(record.topic()).thenReturn("test_topic");
            value = mock(Struct.class);
            when(record.value()).thenReturn(value);
            Schema valueSchema = mock(Schema.class);
            when(record.valueSchema()).thenReturn(valueSchema);
            when(valueSchema.field("after")).thenReturn(mock(Field.class));
            when(value.getStruct(anyString())).thenReturn(mock(Struct.class));
            Schema schema = SchemaBuilder.struct().field("date",Schema.BYTES_SCHEMA).build();
            Struct invalid = spy(new Struct(schema));
            when(value.getStruct("invalid")).thenReturn(invalid);
            bytes = "test".getBytes();
            when(invalid.getBytes("date")).thenReturn(bytes);
        }
        @Test
        @DisplayName("test wrapDML method for insert event")
        void test1() {
            try (MockedStatic<TapIllegalDate> mb = Mockito
                    .mockStatic(TapIllegalDate.class)) {
                when(value.getString("op")).thenReturn("c");
                mb.when(()->TapIllegalDate.byteToIllegalDate(bytes)).thenReturn(mock(TapIllegalDate.class));
                Map<String, Object> after = new HashMap<>();
                after.put("date","111");
                doReturn(after).when(mysqlReader).struct2Map(any(Struct.class),anyString());
                MysqlStreamEvent actual = mysqlReader.wrapDML(record);
                assertEquals(true,((TapRecordEvent)actual.getTapEvent()).getContainsIllegalDate());
            }
        }
        @Test
        @DisplayName("test wrapDML method for update event")
        void test2() {
            try (MockedStatic<TapIllegalDate> mb = Mockito
                    .mockStatic(TapIllegalDate.class)) {
                when(value.getString("op")).thenReturn("u");
                mb.when(()->TapIllegalDate.byteToIllegalDate(bytes)).thenReturn(mock(TapIllegalDate.class));
                Map<String, Object> after = new HashMap<>();
                after.put("date","111");
                doReturn(after).when(mysqlReader).struct2Map(any(Struct.class),anyString());
                MysqlStreamEvent actual = mysqlReader.wrapDML(record);
                assertEquals(true,((TapRecordEvent)actual.getTapEvent()).getContainsIllegalDate());
            }
        }
        @Test
        @DisplayName("test wrapDML method for IOException")
        void test3() {
            try (MockedStatic<TapIllegalDate> mb = Mockito
                    .mockStatic(TapIllegalDate.class)) {
                when(value.getString("op")).thenReturn("u");
                mb.when(()->TapIllegalDate.byteToIllegalDate(bytes)).thenThrow(IOException.class);
                assertThrows(RuntimeException.class,()->mysqlReader.wrapDML(record));
            }
        }
        @Test
        @DisplayName("test wrapDML method for ClassNotFoundException")
        void test4() {
            try (MockedStatic<TapIllegalDate> mb = Mockito
                    .mockStatic(TapIllegalDate.class)) {
                when(value.getString("op")).thenReturn("u");
                mb.when(()->TapIllegalDate.byteToIllegalDate(bytes)).thenThrow(ClassNotFoundException.class);
                assertThrows(RuntimeException.class,()->mysqlReader.wrapDML(record));
            }
        }
    }
}
