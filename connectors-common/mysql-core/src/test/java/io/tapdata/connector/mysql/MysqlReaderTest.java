package io.tapdata.connector.mysql;

import io.debezium.type.TapIllegalDate;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.entity.MysqlStreamEvent;
import io.tapdata.connector.mysql.util.StringCompressUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class MysqlReaderTest {

    @Nested
    class wrapDDL {
        MysqlReader mysqlReader = new MysqlReader(mock(MysqlJdbcContextV2.class), mock(Log.class), () -> {
            return true;
        });

        @Test
        void test() {
            SourceRecord sourceRecord = mock(SourceRecord.class);
            Struct struct = mock(Struct.class);
            when(sourceRecord.value()).thenReturn(struct);
            when(struct.getStruct(anyString())).thenReturn(mock(Struct.class));
            when(struct.getString(anyString())).thenReturn("ALTER TABLE \"C##TAPDATA\".\"TT_DDL\" \n" +
                    "ADD (\"TT\" VARCHAR2(255));");
            List<MysqlStreamEvent> result = mysqlReader.wrapDDL(sourceRecord);
            TapDDLEvent resultEvent = (TapDDLEvent) result.get(0).getTapEvent();
            assertEquals("ALTER TABLE \"C##TAPDATA\".\"TT_DDL\" \n" +
                    "ADD (\"TT\" VARCHAR2(255));", resultEvent.getOriginDDL());
        }

        @Test
        void testUnknownEvent() {
            SourceRecord sourceRecord = mock(SourceRecord.class);
            Struct struct = mock(Struct.class);
            when(sourceRecord.value()).thenReturn(struct);
            when(struct.getStruct(anyString())).thenReturn(mock(Struct.class));
            when(struct.getString(anyString())).thenReturn("Unknown DDL");
            try (MockedStatic<DDLFactory> ddlFactoryMockedStatic = Mockito.mockStatic(DDLFactory.class)) {
                ddlFactoryMockedStatic.when(() -> DDLFactory.ddlToTapDDLEvent(any(), any(), any(), any(), any())).thenThrow(new Throwable("ERROR"));
                List<MysqlStreamEvent> result = mysqlReader.wrapDDL(sourceRecord);
                TapDDLEvent resultEvent = (TapDDLEvent) result.get(0).getTapEvent();
                assertEquals("Unknown DDL", resultEvent.getOriginDDL());
            }
        }

    }

    @Nested
    class WrapDMLTest {
        private MysqlReader mysqlReader;
        private SourceRecord record;
        private byte[] bytes;
        private Struct value;

        @BeforeEach
        void beforeEach() {
            MysqlJdbcContextV2 jdbcContextV2 = mock(MysqlJdbcContextV2.class);
            when(jdbcContextV2.getConfig()).thenReturn(mock(MysqlConfig.class));
            mysqlReader = new MysqlReader(jdbcContextV2, mock(Log.class), () -> {
                return true;
            });
            mysqlReader = spy(mysqlReader);
            record = mock(SourceRecord.class);
            when(record.topic()).thenReturn("test_topic");
            value = mock(Struct.class);
            when(record.value()).thenReturn(value);
            Schema valueSchema = mock(Schema.class);
            when(record.valueSchema()).thenReturn(valueSchema);
            when(valueSchema.field("after")).thenReturn(mock(Field.class));
            when(value.getStruct("source")).thenReturn(mock(Struct.class));
            Schema schema = SchemaBuilder.struct().field("date", Schema.BYTES_SCHEMA).build();
            Struct invalid = spy(new Struct(schema));
            when(value.getStruct("invalid")).thenReturn(invalid);
            bytes = "test".getBytes();
            when(invalid.getBytes("date")).thenReturn(bytes);
        }

        //        @Test
        @DisplayName("test wrapDML method for insert event")
        void test1() {
            try (MockedStatic<TapIllegalDate> mb = Mockito
                    .mockStatic(TapIllegalDate.class)) {
                when(value.getString("op")).thenReturn("c");
                Schema schema = mock(Schema.class);
                when(value.schema()).thenReturn(schema);
                when(schema.field("beforeInvalid")).thenReturn(mock(Field.class));
                Struct invalid = mock(Struct.class);
                when(value.getStruct("beforeValid")).thenReturn(invalid);
                Schema invalidSchema = mock(Schema.class);
                when(invalid.schema()).thenReturn(invalidSchema);
                List<Field> fields = new ArrayList<>();
                fields.add(mock(Field.class));
                when(invalidSchema.fields()).thenReturn(fields);
                mb.when(() -> TapIllegalDate.byteToIllegalDate(bytes)).thenReturn(mock(TapIllegalDate.class));
                Map<String, Object> after = new HashMap<>();
                after.put("date", "111");
                doReturn(after).when(mysqlReader).struct2Map(any(Struct.class), anyString());
                MysqlStreamEvent actual = mysqlReader.wrapDML(record);
                assertEquals(true, ((TapRecordEvent) actual.getTapEvent()).getContainsIllegalDate());
            }
        }

        //        @Test
        @DisplayName("test wrapDML method for update event")
        void test2() {
            try (MockedStatic<TapIllegalDate> mb = Mockito
                    .mockStatic(TapIllegalDate.class)) {
                when(value.getString("op")).thenReturn("u");
                mb.when(() -> TapIllegalDate.byteToIllegalDate(bytes)).thenReturn(mock(TapIllegalDate.class));
                Map<String, Object> after = new HashMap<>();
                after.put("date", "111");
                doReturn(after).when(mysqlReader).struct2Map(any(Struct.class), anyString());
                MysqlStreamEvent actual = mysqlReader.wrapDML(record);
                assertEquals(true, ((TapRecordEvent) actual.getTapEvent()).getContainsIllegalDate());
            }
        }

        @Test
        @DisplayName("test wrapDML method for IOException")
        void test3() {
            try (MockedStatic<TapIllegalDate> mb = Mockito
                    .mockStatic(TapIllegalDate.class)) {
                when(value.getString("op")).thenReturn("u");
                mb.when(() -> TapIllegalDate.byteToIllegalDate(bytes)).thenThrow(IOException.class);
                assertThrows(RuntimeException.class, () -> mysqlReader.wrapDML(record));
            }
        }

        @Test
        @DisplayName("test wrapDML method for ClassNotFoundException")
        void test4() {
            try (MockedStatic<TapIllegalDate> mb = Mockito
                    .mockStatic(TapIllegalDate.class)) {
                when(value.getString("op")).thenReturn("u");
                mb.when(() -> TapIllegalDate.byteToIllegalDate(bytes)).thenThrow(ClassNotFoundException.class);
                assertThrows(RuntimeException.class, () -> mysqlReader.wrapDML(record));
            }
        }
    }

    @Nested
    class WrapHandleDateTimeTest {

        private MysqlReader mysqlReader;
        private KVReadOnlyMap<TapTable> tapTableMap;

        @BeforeEach
        void beforeEach() {
            mysqlReader = mock(MysqlReader.class);
            when(mysqlReader.handleDatetime(any(), any(), any())).thenCallRealMethod();
            tapTableMap = mock(KVReadOnlyMap.class);
            ReflectionTestUtils.setField(mysqlReader, "tapTableMap", tapTableMap);
            ReflectionTestUtils.setField(mysqlReader, "diff", 8 * 60 * 60 * 1000);
            ReflectionTestUtils.setField(mysqlReader, "dbTimeZone", TimeZone.getTimeZone("GMT+07:00"));
        }

        @Test
        @DisplayName("test handleDateTime method1")
        void testAbnormal1() {
            when(tapTableMap.get(any())).thenReturn(null);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", "2021-07-01 00:00:00"), "2021-07-01 00:00:00");
        }

        @Test
        @DisplayName("test handleDateTime method2")
        void testAbnormal2() {
            TapTable table = mock(TapTable.class);
            when(table.getNameFieldMap()).thenReturn(null);
            when(tapTableMap.get(any())).thenReturn(table);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", "2021-07-01 00:00:00"), "2021-07-01 00:00:00");
        }

        @Test
        @DisplayName("test handleDateTime method3")
        void testAbnormal3() {
            TapTable table = mock(TapTable.class);
            table.add(new TapField("empty", "datetime"));
            when(tapTableMap.get(any())).thenReturn(table);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", "2021-07-01 00:00:00"), "2021-07-01 00:00:00");
        }

        @Test
        @DisplayName("test handleDateTime method for date integer")
        void testDate1() {
            TapTable table = new TapTable("test_table");
            table.add(new TapField("test_column", "date").tapType(new TapDate()));
            when(tapTableMap.get("test_table")).thenReturn(table);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", 1000), 86400000000L);
        }

        @Test
        @DisplayName("test handleDateTime method for date string")
        void testDate2() {
            TapTable table = new TapTable("test_table");
            table.add(new TapField("test_column", "date").tapType(new TapDate()));
            when(tapTableMap.get("test_table")).thenReturn(table);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", "2024-01-01"), "2024-01-01");
        }

        @Test
        @DisplayName("test handleDateTime method for time")
        void testTime() {
            TapTable table = new TapTable("test_table");
            table.add(new TapField("test_column", "time").tapType(new TapTime()));
            when(tapTableMap.get("test_table")).thenReturn(table);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", "12:12:12"), "12:12:12");
        }

        @Test
        @DisplayName("test handleDateTime method for datetime fraction3")
        void testDateTime1() {
            TapTable table = new TapTable("test_table");
            table.add(new TapField("test_column", "datetime").tapType(new TapDateTime().fraction(3)));
            when(tapTableMap.get("test_table")).thenReturn(table);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", 1721028035L), 1749828035L);
        }

        @Test
        @DisplayName("test handleDateTime method for datetime fraction6")
        void testDateTime2() {
            TapTable table = new TapTable("test_table");
            table.add(new TapField("test_column", "datetime").tapType(new TapDateTime().fraction(6)));
            when(tapTableMap.get("test_table")).thenReturn(table);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", 1721028035000L), 1749828035000L);
        }

        @Test
        @DisplayName("test handleDateTime method for datetime string")
        void testDateTime3() {
            TapTable table = new TapTable("test_table");
            table.add(new TapField("test_column", "datetime").tapType(new TapDateTime().fraction(0)));
            when(tapTableMap.get("test_table")).thenReturn(table);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", "2021-07-01T00:00:00Z"), ZonedDateTime.parse("2021-07-01T07:00:00Z"));
        }

        @Test
        @DisplayName("test handleDateTime method for datetime Timestamp")
        void testDateTime4() {
            TapTable table = new TapTable("test_table");
            table.add(new TapField("test_column", "datetime").tapType(new TapDateTime().fraction(0)));
            when(tapTableMap.get("test_table")).thenReturn(table);
            Assertions.assertEquals(mysqlReader.handleDatetime("test_table", "test_column", Timestamp.valueOf("2021-07-01 00:00:00")), Timestamp.valueOf("2021-07-01 00:00:00"));
        }
    }

    @Nested
    class initSchemaAndSaveSchema {
        MysqlReader reader;
        Log tapLogger;
        TapConnectorContext tapConnectorContext;
        KVMap<Object> stateMap;
        MysqlSchemaHistoryTransfer mysqlSchemaHistoryTransfer;

        @BeforeEach
        void init() {
            this.mysqlSchemaHistoryTransfer = new MysqlSchemaHistoryTransfer();
            LockManager.mysqlSchemaHistoryTransferManager.put("name", this.mysqlSchemaHistoryTransfer);
            reader = mock(MysqlReader.class);
            tapLogger = mock(Log.class);
            reader.tapLogger = tapLogger;
            ReflectionTestUtils.setField(reader, "serverName", "name");
            Supplier<Boolean> isAlive = () -> true;
            ReflectionTestUtils.setField(reader, "isAlive", isAlive);
            stateMap = mock(KVMap.class);
            tapConnectorContext = mock(TapConnectorContext.class);
            when(tapConnectorContext.getStateMap()).thenReturn(stateMap);
            doNothing().when(tapLogger).debug(anyString(), anyLong());
        }


        @Nested
        class initMysqlSchemaHistory {
            @BeforeEach
            void init() {
                doCallRealMethod().when(reader).initMysqlSchemaHistory(tapConnectorContext);
            }

            @Test
            void testNormal() throws IOException {
                try (MockedStatic<StringCompressUtil> scu = mockStatic(StringCompressUtil.class)) {
                    when(stateMap.get(MysqlReader.MYSQL_SCHEMA_HISTORY)).thenReturn("xxx");
                    scu.when(() -> StringCompressUtil.uncompress(anyString())).thenReturn("{\"key\": [\"1\"]}");
                    Assertions.assertDoesNotThrow(() -> reader.initMysqlSchemaHistory(tapConnectorContext));
                }
            }
            @Test
            void testNotString() {
                try (MockedStatic<StringCompressUtil> scu = mockStatic(StringCompressUtil.class)) {
                    scu.when(() -> StringCompressUtil.uncompress(anyString())).thenReturn("{\"key\": 1}");
                    when(stateMap.get(MysqlReader.MYSQL_SCHEMA_HISTORY)).thenReturn(100);
                    Assertions.assertDoesNotThrow(() -> reader.initMysqlSchemaHistory(tapConnectorContext));
                }
            }

            @Test
            void testException() {
                try (MockedStatic<StringCompressUtil> scu = mockStatic(StringCompressUtil.class)) {
                    scu.when(() -> StringCompressUtil.uncompress(anyString())).thenAnswer(a -> {
                        throw new IOException("");
                    });
                    when(stateMap.get(MysqlReader.MYSQL_SCHEMA_HISTORY)).thenReturn("123456789");
                    Assertions.assertThrows(CoreException.class, () -> reader.initMysqlSchemaHistory(tapConnectorContext));
                }
            }
        }

        @Nested
        class saveMysqlSchemaHistory {
            MysqlSchemaHistoryTransfer mysqlSchemaHistoryTransfer;
            @BeforeEach
            void init() {
                this.mysqlSchemaHistoryTransfer = mock(MysqlSchemaHistoryTransfer.class);
                doNothing().when(stateMap).put(anyString(), anyString());
                doNothing().when(tapConnectorContext).configContext();
                doNothing().when(tapLogger).warn(anyString());
                reader.tapLogger = tapLogger;
                doCallRealMethod().when(reader).saveMysqlSchemaHistory(tapConnectorContext);
                ReflectionTestUtils.setField(reader,"schemaHistoryTransfer",mysqlSchemaHistoryTransfer);
            }

            @Test
            void testNormal() {
                try (
                        MockedStatic<StringCompressUtil> scu = mockStatic(StringCompressUtil.class)) {
                    when(mysqlSchemaHistoryTransfer.isSave()).thenReturn(false);
                    doAnswer((a) -> {
                        return null;
                    }).when(mysqlSchemaHistoryTransfer).executeWithLock(any(Predicate.class), any(MysqlSchemaHistoryTransfer.Runner.class));
                    doAnswer(a -> {
                        Predicate predicate = a.getArgument(0, Predicate.class);
                        predicate.test(null);
                        MysqlSchemaHistoryTransfer.Runner argument = a.getArgument(1, MysqlSchemaHistoryTransfer.Runner.class);
                        argument.execute();
                        return null;
                    }).when(mysqlSchemaHistoryTransfer).executeWithLock(any(Predicate.class), any(MysqlSchemaHistoryTransfer.Runner.class));
                    doAnswer(a -> null).when(mysqlSchemaHistoryTransfer).save();
                    scu.when(() -> StringCompressUtil.compress(anyString())).thenReturn("");
                    reader.saveMysqlSchemaHistory(tapConnectorContext);
                }
            }
            @Test
            void testIsSave() {
                try (MockedStatic<MysqlSchemaHistoryTransfer> msht = mockStatic(MysqlSchemaHistoryTransfer.class);
                     MockedStatic<StringCompressUtil> scu = mockStatic(StringCompressUtil.class)) {
                    msht.when(mysqlSchemaHistoryTransfer::isSave).thenReturn(true);
                    msht.when(() -> mysqlSchemaHistoryTransfer.executeWithLock(any(Predicate.class), any(MysqlSchemaHistoryTransfer.Runner.class))).thenAnswer(a -> {
                        Predicate predicate = a.getArgument(0, Predicate.class);
                        predicate.test(null);
                        MysqlSchemaHistoryTransfer.Runner argument = a.getArgument(1, MysqlSchemaHistoryTransfer.Runner.class);
                        argument.execute();
                        return null;
                    });
                    msht.when(mysqlSchemaHistoryTransfer::save).thenAnswer(a -> null);
                    scu.when(() -> StringCompressUtil.compress(anyString())).thenReturn("");
                    reader.saveMysqlSchemaHistory(tapConnectorContext);
                }
            }

            @Test
            void testException() {
                try (MockedStatic<MysqlSchemaHistoryTransfer> msht = mockStatic(MysqlSchemaHistoryTransfer.class);
                     MockedStatic<StringCompressUtil> scu = mockStatic(StringCompressUtil.class)) {
                    msht.when(mysqlSchemaHistoryTransfer::isSave).thenReturn(false);
                    msht.when(() -> mysqlSchemaHistoryTransfer.executeWithLock(any(Predicate.class), any(MysqlSchemaHistoryTransfer.Runner.class))).thenAnswer(a -> {
                        MysqlSchemaHistoryTransfer.Runner argument = a.getArgument(1, MysqlSchemaHistoryTransfer.Runner.class);
                        argument.execute();
                        return null;
                    });
                    msht.when(mysqlSchemaHistoryTransfer::save).thenAnswer(a -> null);
                    scu.when(() -> StringCompressUtil.compress(anyString())).thenAnswer(a -> { throw new IOException("");});
                    reader.saveMysqlSchemaHistory(tapConnectorContext);
                }
            }
        }
    }
}
