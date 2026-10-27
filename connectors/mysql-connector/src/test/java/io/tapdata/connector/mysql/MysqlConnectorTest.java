package io.tapdata.connector.mysql;

import io.debezium.type.TapIllegalDate;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.JdbcContext;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.util.MysqlBinlogPositionUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
import io.tapdata.utils.UnitTestUtils;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class MysqlConnectorTest {
    @Test
    void executeWithTimeoutUsesMysqlStreamingAndConversionPath() throws Throwable {
        MysqlConnector connector = Mockito.spy(new MysqlConnector());
        JdbcContext jdbcContext = mock(JdbcContext.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        Map<String, Object> convertedRow = Collections.singletonMap("created_at", "mysql-value");
        ReflectionTestUtils.setField(connector, "jdbcContext", jdbcContext);
        ReflectionTestUtils.setField(connector, "isAlive", new AtomicBoolean(true));
        when(jdbcContext.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute("select created_at from records")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(resultSet.next()).thenReturn(true, false);
        when(statement.getMoreResults()).thenReturn(false);
        doReturn(convertedRow).when(connector).filterTimeForMysql(eq(resultSet), any(ResultSetMetaData.class));
        List<Object> batches = new ArrayList<>();

        connector.execute("select created_at from records", batches::add, 10, 8);

        verify(statement).setQueryTimeout(8);
        verify(statement).setFetchSize(Integer.MIN_VALUE);
        verify(connector).filterTimeForMysql(eq(resultSet), any(ResultSetMetaData.class));
        assertEquals(Collections.singletonList(convertedRow), batches.get(0));
    }

    @Test
    void executeKeysRowsBySqlAliasAndKeepsTypeConversion() throws Throwable {
        MysqlConnector connector = new MysqlConnector();
        JdbcContext jdbcContext = mock(JdbcContext.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        ReflectionTestUtils.setField(connector, "jdbcContext", jdbcContext);
        ReflectionTestUtils.setField(connector, "isAlive", new AtomicBoolean(true));
        when(jdbcContext.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        String sql = "select id as `key`, created_at as created, value from records";
        when(statement.execute(sql)).thenReturn(true);
        when(statement.getResultSet()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(resultSet.next()).thenReturn(true, false);
        when(statement.getMoreResults()).thenReturn(false);
        when(metaData.getColumnCount()).thenReturn(3);
        when(metaData.getColumnName(1)).thenReturn("id");
        when(metaData.getColumnLabel(1)).thenReturn("key");
        when(metaData.getColumnTypeName(1)).thenReturn("INT");
        when(resultSet.getObject(1)).thenReturn(1);
        when(metaData.getColumnName(2)).thenReturn("created_at");
        when(metaData.getColumnLabel(2)).thenReturn("created");
        when(metaData.getColumnTypeName(2)).thenReturn("TIMESTAMP");
        Timestamp created = Timestamp.valueOf("2026-10-20 12:30:00");
        when(resultSet.getObject(2)).thenReturn(created);
        when(resultSet.getString(2)).thenReturn("2026-10-20 12:30:00");
        when(metaData.getColumnName(3)).thenReturn("value");
        when(metaData.getColumnLabel(3)).thenReturn("");
        when(metaData.getColumnTypeName(3)).thenReturn("INT");
        when(resultSet.getObject(3)).thenReturn(2);
        List<Object> batches = new ArrayList<>();

        connector.execute(sql, batches::add, 10, 0);

        Map<String, Object> row = (Map<String, Object>) ((List<?>) batches.get(0)).get(0);
        assertEquals(1, row.get("key"));
        assertEquals(created.toLocalDateTime().atZone(ZoneOffset.UTC), row.get("created"));
        assertEquals(2, row.get("value"));
        assertFalse(row.containsKey("id"));
        assertFalse(row.containsKey("created_at"));
    }

    @Test
    void omittedAndZeroTimeoutKeepMysqlStreamingWithoutSettingJdbcTimeout() throws Throwable {
        MysqlConnector connector = new MysqlConnector();
        JdbcContext jdbc = mock(JdbcContext.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ReflectionTestUtils.setField(connector, "jdbcContext", jdbc);
        when(jdbc.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        connector.execute("select 1", ignored -> {}, 10);
        connector.execute("select 1", ignored -> {}, 10, 0);
        verify(statement, never()).setQueryTimeout(anyInt());
        verify(statement, times(2)).setFetchSize(Integer.MIN_VALUE);
    }

    @Test
    void discoverSchemaMarksInformationSchemaViewAsView() throws SQLException {
        MysqlConnector connector = new MysqlConnector();
        JdbcContext jdbcContext = mock(JdbcContext.class);
        when(jdbcContext.queryAllColumns(anyList())).thenReturn(Collections.emptyList());
        when(jdbcContext.queryAllIndexes(anyList())).thenReturn(Collections.emptyList());
        when(jdbcContext.queryAllForeignKeys(anyList())).thenReturn(Collections.emptyList());
        ReflectionTestUtils.setField(connector, "jdbcContext", jdbcContext);
        DataMap view = DataMap.create();
        view.put("tableName", "orders_view");
        view.put("tableType", "VIEW");
        List<TapTable> discovered = new ArrayList<>();

        connector.singleThreadDiscoverSchema(Collections.singletonList(view), discovered::addAll);

        Assertions.assertEquals(1, discovered.size());
        Assertions.assertEquals("view", discovered.get(0).getType());
    }

    @Test
    void testRegisterCapabilitiesQueryTableHash() {
        MysqlConnector postgresConnector = new MysqlConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(postgresConnector, "registerCapabilities", connectorFunctions, codecRegistry);
        Assertions.assertTrue(connectorFunctions.getQueryHashByAdvanceFilterFunction() != null);
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
        ReflectionTestUtils.setField(postgresConnector, "jdbcContext", jdbcContext);
        CommonSqlMaker commonSqlMaker = new CommonSqlMaker('`');
        ;
        ReflectionTestUtils.setField(postgresConnector, "commonSqlMaker", commonSqlMaker);
        doNothing().when(jdbcContext).query(Mockito.anyString(), Mockito.any());

        Consumer<TapHashResult<String>> consumer = new Consumer<TapHashResult<String>>() {
            @Override
            public void accept(TapHashResult<String> stringTapHashResult) {
                Assertions.assertTrue(stringTapHashResult == null);
            }
        };
        ReflectionTestUtils.invokeMethod(postgresConnector, "queryTableHash", connectorContext, filter, table, consumer);

    }

    public void buildNumberTapField(String name, LinkedHashMap<String, TapField> map) {
        TapField numberTapField = new TapField();
        numberTapField.setTapType(new TapNumber());
        numberTapField.setName(name);
        numberTapField.setDataType(name);
        map.put(name, numberTapField);
    }

    @Test
    void testRegisterCapabilitiesCountByPartitionFilter() {
        MysqlConnector mysqlConnector = new MysqlConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(mysqlConnector, "registerCapabilities", connectorFunctions, codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }

    @Test
    void testFieldSizeLargeThen50() {
        TapTable tapTable = new TapTable();
        LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
        for (int i = 0; i < 51; i++) {
            TapField field = new TapField("f" + i, "INT");
            fieldMap.put(field.getName(), field);
        }
        tapTable.setId("AA_0607");
        tapTable.setNameFieldMap(fieldMap);

        CommonDbConfig commonDbConfig = mock(CommonDbConfig.class);
        when(commonDbConfig.getSchema()).thenReturn("COOLGJ");
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        MysqlConnector connector = spy(new MysqlConnector());
        UnitTestUtils.injectField(CommonDbConnector.class, connector, "commonDbConfig", commonDbConfig);

        assertEquals("SELECT * FROM `COOLGJ`.`AA_0607`", connector.getBatchReadSelectSql(tapTable));
    }

    @Nested
    class FilterTimeForMysqlTest {
        MysqlConnector mysqlConnector = new MysqlConnector();
        ResultSet resultSet;
        ResultSetMetaData metaData;
        Set<String> dateTypeSet;
        TapRecordEvent recordEvent;
        MysqlConnector.IllegalDateConsumer illegalDateConsumer;
        MysqlConfig mysqlConfig;

        @BeforeEach
        void beforeEach() {
            resultSet = mock(ResultSet.class);
            metaData = mock(ResultSetMetaData.class);
            dateTypeSet = new HashSet<>();
            recordEvent = new TapInsertRecordEvent();
            mysqlConfig = mock(MysqlConfig.class);
//            illegalDateConsumer = mock(MysqlConnector.IllegalDateConsumer.class);
            illegalDateConsumer = new MysqlConnector.IllegalDateConsumer() {
                @Override
                public void containsIllegalDate(TapRecordEvent event, boolean containsIllegalDate) {
                    event.setContainsIllegalDate(containsIllegalDate);
                }

                @Override
                public void buildIllegalDateFieldName(TapRecordEvent event, List<String> illegalDateFieldName) {
                    ((TapInsertRecordEvent) event).setAfterIllegalDateFieldName(illegalDateFieldName);
                }
            };
        }

        @Test
        @DisplayName("test filterTimeForMysql method for TIME")
        void test1() throws SQLException {
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_time");
            when(metaData.getColumnTypeName(1)).thenReturn("TIME");
            when(resultSet.getBytes(1)).thenReturn("00:00:00".getBytes(StandardCharsets.UTF_8));
            when(resultSet.getString(1)).thenReturn("00:00:00");
            when(resultSet.getObject(1)).thenReturn("00:00:00");
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, recordEvent, illegalDateConsumer);
            Map<String, Object> except = new HashMap<>();
            except.put("_time", "00:00:00");
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
            when(resultSet.getObject(1)).thenReturn(Timestamp.valueOf("2024-05-17 00:00:00"));
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, recordEvent, illegalDateConsumer);
            Timestamp except = Timestamp.valueOf("2024-05-17 00:00:00");
            assertEquals(except.toLocalDateTime().atZone(ZoneOffset.UTC), actual.get("_timestamp"));
            assertFalse(recordEvent.getContainsIllegalDate());
        }

        @Test
        @DisplayName("test filterTimeForMysql method for DATE")
        void test3() throws SQLException {
            ReflectionTestUtils.setField(mysqlConnector, "mysqlConfig", mysqlConfig);
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_date");
            when(metaData.getColumnTypeName(1)).thenReturn("DATE");
            when(resultSet.getString(1)).thenReturn("2024-05-17");
            when(resultSet.getObject(1)).thenReturn(java.sql.Date.valueOf("2024-05-17"));
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, recordEvent, illegalDateConsumer);
            LocalDateTime except = LocalDateTime.of(2024, 5, 17, 0, 0, 0);
            assertEquals(except, actual.get("_date"));
            assertFalse(recordEvent.getContainsIllegalDate());
        }

        @Test
        @DisplayName("test filterTimeForMysql method for DATETIME")
        void test4() throws SQLException {
            ReflectionTestUtils.setField(mysqlConnector, "mysqlConfig", mysqlConfig);
            dateTypeSet.add("_datetime");
            when(mysqlConfig.getOldVersionTimezone()).thenReturn(false);
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_datetime");
            when(metaData.getColumnTypeName(1)).thenReturn("DATETIME");
            when(resultSet.getObject(1)).thenReturn(LocalDateTime.parse("2024-05-17T00:00:00"));
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, recordEvent, illegalDateConsumer);
            Map<String, Object> except = new HashMap<>();
            except.put("_datetime", LocalDateTime.parse("2024-05-17T00:00:00"));
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
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, recordEvent, illegalDateConsumer);
            Map<String, Object> except = new HashMap<>();
            except.put("id", 1);
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
            when(resultSet.getString(1)).thenReturn("2024-00-00 00:00:00");
            when(resultSet.getObject(1)).thenReturn(null);
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, recordEvent, illegalDateConsumer);
            assertInstanceOf(TapIllegalDate.class, actual.get("_datetime"));
            assertTrue(recordEvent.getContainsIllegalDate());
            assertEquals("_datetime", ((TapInsertRecordEvent) recordEvent).getAfterIllegalDateFieldName().get(0));
        }

        @Test
        @DisplayName("test filterTimeForMysql method for TIMESTAMP when value is illegal and getObject return null")
        void test7() throws SQLException {
            mysqlConnector = mock(MysqlConnector.class);
            dateTypeSet.add("_timestamp");
            when(metaData.getColumnCount()).thenReturn(1);
            when(metaData.getColumnName(1)).thenReturn("_timestamp");
            when(metaData.getColumnTypeName(1)).thenReturn("TIMESTAMP");
            when(resultSet.getObject(1)).thenThrow(new RuntimeException());
            when(resultSet.getString(1)).thenReturn("0000-00-00 00:00:00");
            doCallRealMethod().when(mysqlConnector).filterTimeForMysql(resultSet, metaData, recordEvent, illegalDateConsumer);
            Map<String, Object> actual = mysqlConnector.filterTimeForMysql(resultSet, metaData, recordEvent, illegalDateConsumer);
            assertInstanceOf(TapIllegalDate.class, actual.get("_timestamp"));
            assertEquals("_timestamp", ((TapInsertRecordEvent) recordEvent).getAfterIllegalDateFieldName().get(0));
        }
    }

    @Nested
    class GetHashSplitStringSqlTest {
        TapTable tapTable;
        MysqlConnector connector;

        @BeforeEach
        void setUp() {
            connector = mock(MysqlConnector.class);
            tapTable = new TapTable();
            tapTable.setNameFieldMap(new LinkedHashMap<>());
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
        }

        @Test
        void testEmptyField() {
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertNotNull(connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testNotPrimaryKeys() {
            tapTable.add(new TapField("ID", "INT"));
            tapTable.add(new TapField("TITLE", "VARCHAR(64)"));

            assertNotNull(connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testTrue() {
            tapTable.add(new TapField("ID", "INT").primaryKeyPos(1));
            tapTable.add(new TapField("TITLE", "VARCHAR(64)"));
            assertNotNull(connector.getHashSplitStringSql(tapTable));
        }
    }

    @Nested
    class BatchReadWithHashSplitTest {
        CommonDbConfig commonDbConfig;
        JdbcContext jdbcContext;
        MysqlJdbcContextV2 mysqlJdbcContextV2;
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
            mysqlJdbcContextV2 = mock(MysqlJdbcContextV2.class);
            jdbcContext = mysqlJdbcContextV2;
            tapLogger = mock(Log.class);
            tapTable = mock(TapTable.class);
            connector = mock(MysqlConnector.class);
            UnitTestUtils.injectField(CommonDbConnector.class, connector, "jdbcContext", jdbcContext);
            UnitTestUtils.injectField(MysqlConnector.class, connector, "mysqlJdbcContext", jdbcContext);
            UnitTestUtils.injectField(CommonDbConnector.class, connector, "commonDbConfig", commonDbConfig);
            UnitTestUtils.injectField(CommonDbConnector.class, connector, "tapLogger", tapLogger);
            UnitTestUtils.injectField(CommonDbConnector.class, connector, "exceptionCollector", mock(ExceptionCollector.class));
            doCallRealMethod().when(connector).batchReadWithHashSplit(tapConnectorContext, tapTable, offsetState, eventBatchSize, eventsOffsetConsumer);
        }

        @Test
        void testApplySplit() throws Throwable {
            int expectedMaxSplit = 5;
            when(commonDbConfig.getHashSplit()).thenReturn(true);
            when(commonDbConfig.getMaxSplit()).thenReturn(expectedMaxSplit);
            when(commonDbConfig.getBatchReadThreadSize()).thenReturn(3);
            when(connector.isAlive()).thenReturn(true);
            doReturn(new io.tapdata.common.entity.HashReadOffset(expectedMaxSplit)).when(connector).resolveHashReadOffset(any());
            doReturn((io.tapdata.common.ResultSetConsumer) resultSet -> { }).when(connector).resultSetConsumer(any(), anyInt(), any(), any());
            doAnswer(invocation -> {
                io.tapdata.common.ResultSetConsumer resultSetConsumer = invocation.getArgument(1);
                resultSetConsumer.accept(mock(ResultSet.class));
                return null;
            }).when(mysqlJdbcContextV2).queryWithStream(anyString(), any());
            assertDoesNotThrow(() -> connector.batchReadWithHashSplit(tapConnectorContext, tapTable, offsetState, eventBatchSize, eventsOffsetConsumer));
            verify(connector, times(expectedMaxSplit)).resultSetConsumer(any(), anyInt(), any(), any());
        }
    }

    @Nested
    class TimestampToStreamOffsetTest {

        @Test
        void testBinlogClose() throws Throwable {
            MysqlConnector mysqlConnector = new MysqlConnector();
            MysqlJdbcContextV2 mysqlJdbcContext = mock(MysqlJdbcContextV2.class);
            ReflectionTestUtils.setField(mysqlConnector, "mysqlJdbcContext", mysqlJdbcContext);
            ExceptionCollector exceptionCollector = new MysqlExceptionCollector();
            ReflectionTestUtils.setField(mysqlConnector, "exceptionCollector", exceptionCollector);

            when(mysqlJdbcContext.readBinlogPosition()).thenReturn(null);
            TapConnectorContext tapConnectorContext = mock(TapConnectorContext.class);
            try {
                ReflectionTestUtils.invokeMethod(mysqlConnector, "timestampToStreamOffset",
                        tapConnectorContext, null);
            } catch (Throwable e) {
                Assertions.assertTrue(e.getMessage().contains("please open mysql binlog config"));
            }
        }

        @Test
        void testBinlogOpen() throws Throwable {
            MysqlConnector mysqlConnector = new MysqlConnector();
            MysqlJdbcContextV2 mysqlJdbcContext = mock(MysqlJdbcContextV2.class);
            ReflectionTestUtils.setField(mysqlConnector, "mysqlJdbcContext", mysqlJdbcContext);
            MysqlBinlogPosition mysqlBinlogPosition = new MysqlBinlogPosition();
            long position = 123455L;
            mysqlBinlogPosition.setPosition(position);
            when(mysqlJdbcContext.readBinlogPosition()).thenReturn(mysqlBinlogPosition);
            TapConnectorContext tapConnectorContext = mock(TapConnectorContext.class);
            MysqlBinlogPosition actualData = ReflectionTestUtils.invokeMethod(mysqlConnector, "timestampToStreamOffset",
                    tapConnectorContext, null);
            Assertions.assertTrue(actualData.getPosition() == position);
        }
    }

    /**
     * 覆盖分支上新增/改动行（Sonar New Code）的执行路径，避免 New Code 覆盖率不达标。
     */
    @Nested
    class NewCodeCoverageTest {

        private MysqlConnector connector;
        private JdbcContext jdbcContext;
        private MysqlConfig mysqlConfig;

        @BeforeEach
        void setUp() {
            connector = new MysqlConnector();
            jdbcContext = mock(JdbcContext.class);
            mysqlConfig = mock(MysqlConfig.class);
            ReflectionTestUtils.setField(connector, "jdbcContext", jdbcContext);
            ReflectionTestUtils.setField(connector, "mysqlConfig", mysqlConfig);
            ReflectionTestUtils.setField(connector, "commonDbConfig", new MysqlConfig());
            ReflectionTestUtils.setField(connector, "tapLogger", mock(Log.class));
            ReflectionTestUtils.setField(connector, "isAlive", new AtomicBoolean(true));
        }

        @Test
        void clearTableCoversExistingAndMissingTableBranches() throws SQLException {
            TapClearTableEvent clearEvent = new TapClearTableEvent();
            clearEvent.setTableId("t1");
            when(jdbcContext.queryAllTables(anyList()))
                    .thenReturn(Collections.singletonList(DataMap.create()))
                    .thenReturn(Collections.emptyList());

            connector.clearTable(mock(TapConnectorContext.class), clearEvent);
            connector.clearTable(mock(TapConnectorContext.class), clearEvent);

            ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
            verify(jdbcContext, times(1)).batchExecute(captor.capture());
            assertEquals("truncate table `t1`", captor.getValue().get(1));
        }

        @Test
        void dropTableCoversExistingAndMissingTableBranches() throws SQLException {
            TapDropTableEvent dropEvent = new TapDropTableEvent();
            dropEvent.setTableId("t1");
            when(jdbcContext.queryAllTables(anyList()))
                    .thenReturn(Collections.singletonList(DataMap.create()))
                    .thenReturn(Collections.emptyList());

            connector.dropTable(mock(TapConnectorContext.class), dropEvent);
            connector.dropTable(mock(TapConnectorContext.class), dropEvent);

            ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
            verify(jdbcContext, times(1)).batchExecute(captor.capture());
            assertEquals("drop table `t1`", captor.getValue().get(1));
        }

        @Test
        void getAfterUniqueAutoIncrementFieldsCoversCreateAutoIncBranches() {
            TapTable table = new TapTable("t1");
            table.add(new TapField("id", "int").autoInc(true).isPrimaryKey(true));
            table.add(new TapField("code", "varchar").autoInc(true).isPrimaryKey(false));
            TapIndex uniqueIndex = new TapIndex().name("uk_code").unique(true)
                    .indexField(new TapIndexField().name("code"));

            when(mysqlConfig.getCreateAutoInc()).thenReturn(false);
            assertTrue(connector.getAfterUniqueAutoIncrementFields(table, Collections.singletonList(uniqueIndex)).isEmpty());

            when(mysqlConfig.getCreateAutoInc()).thenReturn(true);
            List<String> sqls = connector.getAfterUniqueAutoIncrementFields(table, Collections.singletonList(uniqueIndex));
            assertEquals(Collections.singletonList("ALTER TABLE `t1` MODIFY COLUMN `code` varchar AUTO_INCREMENT"), sqls);
        }

        @Test
        void callExecutesCallableStatementWithoutParams() throws Exception {
            Connection connection = mock(Connection.class);
            CallableStatement callableStatement = mock(CallableStatement.class);
            when(jdbcContext.getConnection()).thenReturn(connection);
            when(connection.prepareCall("{call demo_proc()}")).thenReturn(callableStatement);
            when(callableStatement.execute()).thenReturn(false);

            ExecuteResult<?> result = connector.call("demo_proc", Collections.emptyList());

            assertNull(result.getError());
            verify(connection).prepareCall("{call demo_proc()}");
            // createMysqlCallableStatement 与 call 各设置一次
            verify(callableStatement, times(2)).setFetchSize(Integer.MIN_VALUE);
            verify(callableStatement).execute();
        }

        @Test
        void streamReadCoversHighPerformanceAndBinlogBranches() throws Throwable {
            MysqlJdbcContextV2 mysqlJdbcContext = mock(MysqlJdbcContextV2.class);
            MysqlReader mysqlReader = mock(MysqlReader.class);
            ReflectionTestUtils.setField(connector, "mysqlJdbcContext", mysqlJdbcContext);
            ReflectionTestUtils.setField(connector, "mysqlReader", mysqlReader);
            ReflectionTestUtils.setField(connector, "dbTimeZone", java.util.TimeZone.getDefault());
            TapConnectorContext tapConnectorContext = mock(TapConnectorContext.class);
            StreamReadConsumer consumer = mock(StreamReadConsumer.class);

            when(mysqlConfig.getHighPerformance()).thenReturn(true);
            try (MockedConstruction<MysqlReaderV2> readerV2 = mockConstruction(MysqlReaderV2.class)) {
                ReflectionTestUtils.invokeMethod(connector, "streamRead",
                        tapConnectorContext, Collections.singletonList("t1"), null, 10, consumer);
                assertEquals(1, readerV2.constructed().size());
                MysqlReaderV2 constructed = readerV2.constructed().get(0);
                verify(constructed).init(Collections.singletonList("t1"), null, null, 10, consumer);
                verify(constructed).startMiner(any());
            }

            when(mysqlConfig.getHighPerformance()).thenReturn(false);
            ReflectionTestUtils.invokeMethod(connector, "streamRead",
                    tapConnectorContext, Collections.singletonList("t1"), null, 10, consumer);
            verify(mysqlReader).readBinlog(tapConnectorContext, Collections.singletonList("t1"), null, 10,
                    DDLParserType.MYSQL_CCJ_SQL_PARSER, consumer, null);
        }

        @Test
        void streamReadMultiConnectionCoversHighPerformanceAndBinlogBranches() throws Throwable {
            MysqlJdbcContextV2 mysqlJdbcContext = mock(MysqlJdbcContextV2.class);
            MysqlReader mysqlReader = mock(MysqlReader.class);
            ReflectionTestUtils.setField(connector, "mysqlJdbcContext", mysqlJdbcContext);
            ReflectionTestUtils.setField(connector, "mysqlReader", mysqlReader);
            ReflectionTestUtils.setField(connector, "dbTimeZone", java.util.TimeZone.getDefault());
            TapConnectorContext tapConnectorContext = mock(TapConnectorContext.class);
            StreamReadConsumer consumer = mock(StreamReadConsumer.class);

            ConnectionConfigWithTables configWithTables = mock(ConnectionConfigWithTables.class);
            when(configWithTables.getConnectionConfig()).thenReturn(DataMap.create(Collections.singletonMap("database", "db1")));
            when(configWithTables.getTables()).thenReturn(Collections.singletonList("t1"));
            List<ConnectionConfigWithTables> connectionConfigs = Collections.singletonList(configWithTables);

            when(mysqlConfig.getHighPerformance()).thenReturn(true);
            try (MockedConstruction<MysqlReaderV2> readerV2 = mockConstruction(MysqlReaderV2.class)) {
                ReflectionTestUtils.invokeMethod(connector, "streamReadMultiConnection",
                        tapConnectorContext, connectionConfigs, null, 10, consumer);
                assertEquals(1, readerV2.constructed().size());
                MysqlReaderV2 constructed = readerV2.constructed().get(0);
                verify(constructed).multiInit(connectionConfigs, null, null, 10, consumer);
                verify(constructed).startMiner(any());
            }
            // highPerformance 分支不会提前 return，仍会走一次 binlog 读取
            verify(mysqlReader, times(1)).readBinlog(any(), any(), any(), any(), anyInt(), any(), any(), any());

            clearInvocations(mysqlReader);
            when(mysqlConfig.getHighPerformance()).thenReturn(false);
            ReflectionTestUtils.invokeMethod(connector, "streamReadMultiConnection",
                    tapConnectorContext, connectionConfigs, null, 10, consumer);
            verify(mysqlReader, times(1)).readBinlog(any(), any(), any(), any(), anyInt(), any(), any(), any());
        }

        @Test
        void timestampToStreamOffsetUsesBinlogPositionUtilWhenHighPerformanceEnabled() throws Throwable {
            when(mysqlConfig.getHighPerformance()).thenReturn(true);
            MysqlBinlogPosition expected = new MysqlBinlogPosition();
            expected.setPosition(2048L);
            try (MockedConstruction<MysqlBinlogPositionUtil> positionUtil =
                         mockConstruction(MysqlBinlogPositionUtil.class, (mock, context) ->
                                 when(mock.findByLessTimestamp(1700000000000L, true)).thenReturn(expected))) {
                Object offset = ReflectionTestUtils.invokeMethod(connector, "timestampToStreamOffset",
                        mock(TapConnectorContext.class), Long.valueOf(1700000000000L));

                assertSame(expected, offset);
                assertEquals(1, positionUtil.constructed().size());
            }
        }

        @Test
        void timestampToStreamOffsetReturnsStartTimeWhenHighPerformanceDisabled() throws Throwable {
            when(mysqlConfig.getHighPerformance()).thenReturn(false);
            Object offset = ReflectionTestUtils.invokeMethod(connector, "timestampToStreamOffset",
                    mock(TapConnectorContext.class), Long.valueOf(1700000000000L));
            assertEquals(1700000000000L, offset);
        }
    }
}
