package io.tapdata.connector.mysql;

import io.debezium.type.TapIllegalDate;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.utils.UnitTestUtils;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class MysqlConnectorTest {
    @Test
    void testRegisterCapabilitiesQueryTableHash(){
        MysqlConnector postgresConnector = new MysqlConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(postgresConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertTrue(connectorFunctions.getQueryHashByAdvanceFilterFunction()!=null);
    }


    @Test
    void testQueryTableHash() throws SQLException {

        MysqlConnector postgresConnector = new MysqlConnector();
        TapConnectorContext connectorContext = Mockito.mock(TapConnectorContext.class);
        TapAdvanceFilter filter = new TapAdvanceFilter();
        TapTable table = new TapTable();
        LinkedHashMap<String, TapField> map = new LinkedHashMap<>();
        table.setNameFieldMap(map);
        JdbcContext jdbcContext = Mockito.mock(JdbcContext.class);
        ReflectionTestUtils.setField(postgresConnector,"jdbcContext",jdbcContext);
        CommonSqlMaker commonSqlMaker =new CommonSqlMaker('`');;
        ReflectionTestUtils.setField(postgresConnector,"commonSqlMaker",commonSqlMaker);
        doNothing().when(jdbcContext).query(Mockito.anyString(),Mockito.any());

        Consumer<TapHashResult<String>> consumer = new Consumer<TapHashResult<String>>() {
            @Override
            public void accept(TapHashResult<String> stringTapHashResult) {
                Assertions.assertTrue(stringTapHashResult == null);
            }
        };
        ReflectionTestUtils.invokeMethod(postgresConnector,"queryTableHash",connectorContext,filter,table,consumer);

    }


    @Test
    void testBuildHashSql() {

        MysqlConnector mysqlConnector = new MysqlConnector();
        TapAdvanceFilter filter = new TapAdvanceFilter();
        TapTable table = new TapTable();
        LinkedHashMap<String, TapField> map = new LinkedHashMap<>();

        buildNumberTapField("double",map);
        buildNumberTapField("decimal",map);
        buildNumberTapField("float",map);



        TapField booleanTapField  = new TapField();
        booleanTapField.setTapType(new TapBoolean());
        booleanTapField.setName("boolean");
        booleanTapField.setDataType("bit");
        map.put("boolean",booleanTapField);


        TapField datetimeTapField  = new TapField();
        datetimeTapField.setTapType(new TapDateTime());
        datetimeTapField.setName("timestamp");
        datetimeTapField.setDataType("timestamp");
        map.put("timestamp",datetimeTapField);

        TapField binaryTapField  = new TapField();
        binaryTapField.setTapType(new TapBinary());
        binaryTapField.setName("binary");
        binaryTapField.setDataType("binary");
        map.put("binary",binaryTapField);

        TapField intTapField  = new TapField();
        intTapField.setTapType(new TapNumber());
        intTapField.setName("bigint");
        intTapField.setDataType("bigint");
        map.put("int",intTapField);

        table.setNameFieldMap(map);
        JdbcContext jdbcContext = Mockito.mock(JdbcContext.class);
        ReflectionTestUtils.setField(mysqlConnector,"jdbcContext",jdbcContext);
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker('`');;
        ReflectionTestUtils.setField(mysqlConnector,"commonSqlMaker",commonSqlMaker);

        String actualData =ReflectionTestUtils.invokeMethod(mysqlConnector,"buildHashSql",filter,table);

        Assertions.assertTrue(actualData.contains("TRUNCATE(`double`,0)"));
        Assertions.assertTrue(actualData.contains("TRUNCATE(`decimal`,0)"));
        Assertions.assertTrue(actualData.contains("TRUNCATE(`float`,0)"));
        Assertions.assertTrue(actualData.contains("CAST(`boolean` AS unsigned)"));
        Assertions.assertTrue(actualData.contains("round(UNIX_TIMESTAMP( CAST(`timestamp` as char(19)) ))"));
        Assertions.assertTrue(actualData.contains("int"));


    }



    public void  buildNumberTapField(String name,LinkedHashMap<String, TapField> map){
        TapField numberTapField  = new TapField();
        numberTapField.setTapType(new TapNumber());
        numberTapField.setName(name);
        numberTapField.setDataType(name);
        map.put(name,numberTapField);
    }

    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        MysqlConnector mysqlConnector = new MysqlConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(mysqlConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }

    @Nested
    class FilterTimeForMysqlTest{
        MysqlConnector mysqlConnector = new MysqlConnector();
        ResultSet resultSet;
        ResultSetMetaData metaData;
        Set<String> dateTypeSet;
        TapRecordEvent recordEvent;
        MysqlConnector.IllegalDateConsumer illegalDateConsumer;
        @BeforeEach
        void beforeEach(){
            resultSet = mock(ResultSet.class);
            metaData = mock(ResultSetMetaData.class);
            dateTypeSet = new HashSet<>();
            recordEvent = new TapInsertRecordEvent();
//            illegalDateConsumer = mock(MysqlConnector.IllegalDateConsumer.class);
            illegalDateConsumer = new MysqlConnector.IllegalDateConsumer() {
                @Override
                public void containsIllegalDate(TapRecordEvent event, boolean containsIllegalDate) {
                    event.setContainsIllegalDate(containsIllegalDate);
                }
                @Override
                public void buildIllegalDateFieldName(TapRecordEvent event, List<String> illegalDateFieldName) {
                    ((TapInsertRecordEvent)event).setAfterIllegalDateFieldName(illegalDateFieldName);
                }
            };
        }
        @Test
        @DisplayName("test filterTimeForMysql method for TIME")
        void test1() throws SQLException {
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_time");
            when(metaData.getColumnTypeName(1)).thenReturn("TIME");
            when(resultSet.getString(1)).thenReturn("00:00:00");
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, dateTypeSet, recordEvent, illegalDateConsumer);
            Map<String, Object> except = new HashMap<>();
            except.put("_time","00:00:00");
            assertEquals(except, actual);
            assertFalse(recordEvent.getContainsIllegalDate());
        }
        @Test
        @DisplayName("test filterTimeForMysql method for TIMESTAMP")
        void test2() throws SQLException {
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_timestamp");
            when(metaData.getColumnTypeName(1)).thenReturn("TIMESTAMP");
            when(resultSet.getString(1)).thenReturn("2024-05-17 00:00:00");
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, dateTypeSet, recordEvent, illegalDateConsumer);
            Map<String, Object> except = new HashMap<>();
            except.put("_timestamp","2024-05-17 00:00:00");
            assertEquals(except, actual);
            assertFalse(recordEvent.getContainsIllegalDate());
        }
        @Test
        @DisplayName("test filterTimeForMysql method for DATE")
        void test3() throws SQLException {
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_date");
            when(metaData.getColumnTypeName(1)).thenReturn("DATE");
            when(resultSet.getString(1)).thenReturn("2024-05-17");
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, dateTypeSet, recordEvent, illegalDateConsumer);
            Map<String, Object> except = new HashMap<>();
            except.put("_date","2024-05-17");
            assertEquals(except, actual);
            assertFalse(recordEvent.getContainsIllegalDate());
        }
        @Test
        @DisplayName("test filterTimeForMysql method for DATETIME")
        void test4() throws SQLException {
            dateTypeSet.add("_datetime");
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_datetime");
            when(metaData.getColumnTypeName(1)).thenReturn("DATETIME");
            when(resultSet.getObject(1)).thenReturn("2024-05-17 00:00:00");
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, dateTypeSet, recordEvent, illegalDateConsumer);
            Map<String, Object> except = new HashMap<>();
            except.put("_datetime","2024-05-17 00:00:00");
            assertEquals(except, actual);
            assertFalse(recordEvent.getContainsIllegalDate());
        }
        @Test
        @DisplayName("test filterTimeForMysql method for INTEGER")
        void test5() throws SQLException {
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("id");
            when(metaData.getColumnTypeName(1)).thenReturn("INTEGER");
            when(resultSet.getObject(1)).thenReturn(1);
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, dateTypeSet, recordEvent, illegalDateConsumer);
            Map<String, Object> except = new HashMap<>();
            except.put("id",1);
            assertEquals(except, actual);
            assertFalse(recordEvent.getContainsIllegalDate());
        }
        @Test
        @DisplayName("test filterTimeForMysql method for illegal date")
        void test6() throws SQLException {
            dateTypeSet.add("_datetime");
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_datetime");
            when(metaData.getColumnTypeName(1)).thenReturn("DATETIME");
            when(resultSet.getObject(1)).thenReturn("2024-00-00 00:00:00");
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, dateTypeSet, recordEvent, illegalDateConsumer);
            assertInstanceOf(TapIllegalDate.class,actual.get("_datetime"));
            assertTrue(recordEvent.getContainsIllegalDate());
            assertEquals("_datetime",((TapInsertRecordEvent)recordEvent).getAfterIllegalDateFieldName().get(0));
        }
        @Test
        @DisplayName("test filterTimeForMysql method for TIMESTAMP when value is illegal and getObject return null")
        void test7() throws SQLException {
            mysqlConnector = mock(MysqlConnector.class);
            dateTypeSet.add("_timestamp");
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_timestamp");
            when(metaData.getColumnTypeName(1)).thenReturn("TIMESTAMP");
            when(resultSet.getObject(1)).thenReturn(null);
            when(resultSet.getString(1)).thenReturn("0000-00-00 00:00:00");
            doCallRealMethod().when(mysqlConnector).filterTimeForMysql(resultSet, metaData, dateTypeSet, recordEvent, illegalDateConsumer);
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, dateTypeSet, recordEvent, illegalDateConsumer);
            assertInstanceOf(TapIllegalDate.class,actual.get("_timestamp"));
            assertEquals("_timestamp",((TapInsertRecordEvent)recordEvent).getAfterIllegalDateFieldName().get(0));
        }
    }

    @Nested
    class BatchReadSqlTest {
        TapTable tapTable;
        MysqlConfig mysqlConfig;
        MysqlConnector connector;

        @BeforeEach
        void setUp() {
            tapTable = mock(TapTable.class);
            mysqlConfig = mock(MysqlConfig.class);
            connector = mock(MysqlConnector.class);
            doCallRealMethod().when(connector).batchReadSql(tapTable);
            UnitTestUtils.injectField(MysqlConnector.class, connector, "mysqlConfig", mysqlConfig);
        }

        private LinkedHashMap<String, TapField> generateFieldMap(TapField... fields) {
            if (null == fields) {
                return null;
            }
            LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
            for (TapField field : fields) {
                fieldMap.put(field.getName(), field);
            }
            return fieldMap;
        }

        @Test
        void testFieldSizeLargeThen50() {
            int fieldSize = 51;
            TapField[] fields = new TapField[fieldSize];
            for (int i = 0; i < fieldSize; i++) {
                fields[i] = new TapField("f" + i, "INT");
            }
            when(tapTable.getNameFieldMap()).thenReturn(generateFieldMap(fields));
            assertTrue(connector.batchReadSql(tapTable).toLowerCase().startsWith("select *"));
        }

        @Test
        void testFieldSizeLessThen50() {
            int fieldSize = 1;
            TapField[] fields = new TapField[fieldSize];
            for (int i = 0; i < fieldSize; i++) {
                fields[i] = new TapField("f" + i, "INT");
            }
            when(tapTable.getNameFieldMap()).thenReturn(generateFieldMap(fields));
            assertFalse(connector.batchReadSql(tapTable).toLowerCase().startsWith("select *"));
        }
    }

    @Nested
    class GetHashSplitStringSqlTest {
        TapTable tapTable;
        MysqlConnector connector;

        @BeforeEach
        void setUp() {
            connector = mock(MysqlConnector.class);
            tapTable = mock(TapTable.class);
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
        }

        private LinkedHashMap<String, TapField> generateFieldMap(TapField... fields) {
            if (null == fields) {
                return null;
            }
            LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
            for (TapField field : fields) {
                fieldMap.put(field.getName(), field);
            }
            return fieldMap;
        }

        @Test
        void testEmptyField() {
            // null getNameFieldMap
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));

            // empty getNameFieldMap
            when(tapTable.getNameFieldMap()).thenReturn(new LinkedHashMap<>());
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testNotPrimaryKeys() {
            LinkedHashMap<String, TapField> fieldMap = generateFieldMap(
                new TapField("ID", "INT"),
                new TapField("TITLE", "VARCHAR(64)")
            );
            when(tapTable.getNameFieldMap()).thenReturn(fieldMap);

            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testTrue() {
            LinkedHashMap<String, TapField> fieldMap = generateFieldMap(
                new TapField("ID", "INT").primaryKeyPos(1),
                new TapField("TITLE", "VARCHAR(64)")
            );
            when(tapTable.getNameFieldMap()).thenReturn(fieldMap);

            assertNotNull(connector.getHashSplitStringSql(tapTable));
        }
    }

    @Nested
    class BatchReadWithHashSplitTest {
        CommonDbConfig commonDbConfig;
        JdbcContext jdbcContext;
        Log tapLogger;
        TapConnectorContext tapConnectorContext;
        TapTable tapTable;
        Object offsetState;
        int eventBatchSize;
        BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer;
        MysqlConnector connector;

        @BeforeEach
        void setUp() throws Throwable {
            commonDbConfig = mock(CommonDbConfig.class);
            jdbcContext = mock(JdbcContext.class);
            tapLogger = mock(Log.class);
            tapTable = mock(TapTable.class);
            connector = mock(MysqlConnector.class);
            UnitTestUtils.injectField(CommonDbConnector.class, connector, "jdbcContext", jdbcContext);
            UnitTestUtils.injectField(CommonDbConnector.class, connector, "commonDbConfig", commonDbConfig);
            UnitTestUtils.injectField(CommonDbConnector.class, connector, "tapLogger", tapLogger);
            doCallRealMethod().when(connector).batchReadWithHashSplit(tapConnectorContext, tapTable, offsetState, eventBatchSize, eventsOffsetConsumer);
        }

        @Test
        void testApplySplit() {
            int expectedMaxSplit = 5;
            when(commonDbConfig.getHashSplit()).thenReturn(true);
            when(commonDbConfig.getMaxSplit()).thenReturn(expectedMaxSplit);
            assertDoesNotThrow(()-> connector.batchReadWithHashSplit(tapConnectorContext, tapTable, offsetState, eventBatchSize, eventsOffsetConsumer));
            verify(connector, times(expectedMaxSplit)).resultSetConsumer(any(), anyInt(), any());
        }
    }



}
