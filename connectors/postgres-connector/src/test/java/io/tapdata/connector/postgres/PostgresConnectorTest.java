package io.tapdata.connector.postgres;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.cdc.physical.PhysicalWalLogMiner;
import io.tapdata.connector.postgres.partition.PostgresPartitionContext;
import io.tapdata.connector.postgres.partition.TableType;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapPartitionResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class PostgresConnectorTest {

    @Test
    void testRegisterCapabilitiesQueryTableHash(){
        PostgresConnector postgresConnector = new PostgresConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(postgresConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertTrue(connectorFunctions.getQueryHashByAdvanceFilterFunction()!=null);
    }


    @Test
    void testQueryTableHash() throws SQLException {

        PostgresConnector postgresConnector = new PostgresConnector();
        TapConnectorContext connectorContext = Mockito.mock(TapConnectorContext.class);
        TapAdvanceFilter filter = new TapAdvanceFilter();
        TapTable table = new TapTable();
        LinkedHashMap<String, TapField> map = new LinkedHashMap<>();
        table.setNameFieldMap(map);
        JdbcContext jdbcContext = Mockito.mock(JdbcContext.class);
        ReflectionTestUtils.setField(postgresConnector,"jdbcContext",jdbcContext);
        CommonSqlMaker commonSqlMaker = new PostgresSqlMaker();
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
    void testBuildHashSql() throws SQLException {

        PostgresConnector postgresConnector = new PostgresConnector();
        TapAdvanceFilter filter = new TapAdvanceFilter();
        TapTable table = new TapTable();
        LinkedHashMap<String, TapField> map = new LinkedHashMap<>();

        buildNumberTapField("real",map);
        buildNumberTapField("double",map);
        buildNumberTapField("numeric",map);
        buildNumberTapField("float",map);


        TapField stringTapField  = new TapField();
        stringTapField.setTapType(new TapString());
        stringTapField.setName("character");
        stringTapField.setDataType("character(6)");
        map.put("character",stringTapField);


        TapField booleanTapField  = new TapField();
        booleanTapField.setTapType(new TapBoolean());
        booleanTapField.setName("boolean");
        booleanTapField.setDataType("boolean");
        map.put("boolean",booleanTapField);

        TapField timeTapField  = new TapField();
        timeTapField.setTapType(new TapTime());
        timeTapField.setName("time");
        timeTapField.setDataType("time with time zone");
        map.put("time",timeTapField);

        TapField datetimeTapField  = new TapField();
        datetimeTapField.setTapType(new TapDateTime());
        datetimeTapField.setName("timestamp");
        datetimeTapField.setDataType("timestamp");
        map.put("timestamp",datetimeTapField);

        TapField binaryTapField  = new TapField();
        binaryTapField.setTapType(new TapBinary());
        binaryTapField.setName("bytea");
        binaryTapField.setDataType("bytea");
        map.put("bytea",binaryTapField);

        TapField intTapField  = new TapField();
        intTapField.setTapType(new TapNumber());
        intTapField.setName("bigint");
        intTapField.setDataType("bigint");
        map.put("int",intTapField);

        table.setNameFieldMap(map);
        JdbcContext jdbcContext = Mockito.mock(JdbcContext.class);
        ReflectionTestUtils.setField(postgresConnector,"jdbcContext",jdbcContext);
        CommonSqlMaker commonSqlMaker = new PostgresSqlMaker();
        ReflectionTestUtils.setField(postgresConnector,"commonSqlMaker",commonSqlMaker);

        String actualData =ReflectionTestUtils.invokeMethod(postgresConnector,"buildHashSql",filter,table);

        Assertions.assertTrue(actualData.contains("trunc(\"real\")"));
        Assertions.assertTrue(actualData.contains("trunc(\"double\")"));
        Assertions.assertTrue(actualData.contains("trunc(\"numeric\")"));
        Assertions.assertTrue(actualData.contains("trunc(\"float\")"));
        Assertions.assertTrue(actualData.contains("TRIM( \"character\" )"));
        Assertions.assertTrue(actualData.contains("CAST( \"boolean\" as int )"));
        Assertions.assertTrue(actualData.contains("SUBSTRING(cast(\"time\" as varchar) FROM 1 FOR 8)"));
        Assertions.assertTrue(!actualData.contains("bytea"));
        Assertions.assertTrue(actualData.contains("EXTRACT(epoch FROM CAST(date_trunc('second',\"timestamp\" ) AS TIMESTAMP))"));
        Assertions.assertTrue(actualData.contains("int"));


    }



    public void  buildNumberTapField(String name,LinkedHashMap<String, TapField> map){
        TapField numberTapField  = new TapField();
        numberTapField.setTapType(new TapNumber());
        numberTapField.setName(name);
        numberTapField.setDataType(name);
        map.put(name,numberTapField);
    }

    @Nested
    class GetHashSplitStringSqlTest {
        TapTable tapTable;
        PostgresConnector connector;

        @BeforeEach
        void setUp() {
            connector = mock(PostgresConnector.class);
            tapTable = new TapTable();
            tapTable.setNameFieldMap(new LinkedHashMap<>());
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
        }

        @Test
        void testEmptyField() {
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testNotPrimaryKeys() {
            tapTable.add(new TapField("ID", "INT"));
            tapTable.add(new TapField("TITLE", "VARCHAR(64)"));

            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testTrue() {
            tapTable.add(new TapField("ID", "INT").primaryKeyPos(1));
            tapTable.add(new TapField("TITLE", "VARCHAR(64)"));

            assertNotNull(connector.getHashSplitStringSql(tapTable));
        }
    }

    @Test
    void testSplitTableForMultiDiscoverSchema() {
        PostgresConnector postgresConnector = new PostgresConnector();

        AtomicInteger counter = new AtomicInteger(0);

        List<DataMap> tables = Stream.generate(() -> {
            DataMap dataMap = new DataMap();
            dataMap.put("id", "integer");
            dataMap.put("name", "string");
            dataMap.put("tableType", counter.get() < 5 ? TableType.PARENT_TABLE : TableType.CHILD_TABLE);
            counter.incrementAndGet();
            return dataMap;
        }).limit(10).collect(Collectors.toList());
        postgresConnector.postgresVersion = "1";
        CopyOnWriteArraySet<List<DataMap>> result = postgresConnector.splitTableForMultiDiscoverSchema(tables, 1);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());

        postgresConnector.postgresVersion = "100001";
        result = postgresConnector.splitTableForMultiDiscoverSchema(tables, 1);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            postgresConnector.splitTableForMultiDiscoverSchema(tables, -1);
        });

        tables.clear();
        result = postgresConnector.splitTableForMultiDiscoverSchema(tables, 1);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.size());

    }

    @Test
    void testDiscoverPartitionInfoByParentName() throws SQLException {
        PostgresConnector postgresConnector = new PostgresConnector();

        PostgresPartitionContext postgresPartitionContext = mock(PostgresPartitionContext.class);
        postgresConnector.postgresPartitionContext = postgresPartitionContext;

        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        Consumer<Collection<TapPartitionResult>> consumer = (c) -> {};

        postgresConnector.discoverPartitionInfoByParentName(connectorContext, null, consumer);

        verify(postgresPartitionContext, times(1)).discoverPartitionInfoByParentName(any(), any(), any());

        postgresConnector.discoverPartitionInfo(Collections.emptyList());
        verify(postgresPartitionContext, times(1)).discoverPartitionInfo(anyList());
    }

    @Test
    void testCheckCdcSlaveConnectedKeepsCheckingWhenNoStandbyAvailable() throws Exception {
        PostgresConnector postgresConnector = new PostgresConnector();
        PostgresConfig config = new PostgresConfig();
        config.setCheckCdcSlave(true);
        ReflectionTestUtils.setField(postgresConnector, "postgresConfig", config);
        PostgresJdbcContext jdbcContext = mock(PostgresJdbcContext.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getBoolean(1)).thenReturn(false);
        doAnswer(invocation -> {
            ((ResultSetConsumer) invocation.getArgument(1)).accept(resultSet);
            return null;
        }).when(jdbcContext).queryWithNext(anyString(), any());
        ReflectionTestUtils.setField(postgresConnector, "postgresJdbcContext", jdbcContext);
        ReflectionTestUtils.setField(postgresConnector, "tapLogger", mock(Log.class));

        Assertions.assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(postgresConnector,
                "checkCdcSlaveConnected", (PhysicalWalLogMiner) null));

        ScheduledExecutorService executor = (ScheduledExecutorService) ReflectionTestUtils.getField(postgresConnector,
                "asyncCheckSlaveExecutor");
        Assertions.assertNotNull(executor);
        executor.shutdownNow();
    }

    @Test
    void testPhysicalSlavePreferredSkipsStaleTimelineSlave() throws Exception {
        TimelineProbePostgresConnector postgresConnector = new TimelineProbePostgresConnector();
        PostgresConfig config = physicalMasterSlaveConfig("primary", 5432,
                address("primary", 5432), address("stale-standby", 5433));
        postgresConnector.addProbe("primary", 5432, false, "00000006", 6);
        postgresConnector.addProbe("stale-standby", 5433, true, "00000005", 5);
        PostgresJdbcContext jdbcContext = mock(PostgresJdbcContext.class);

        ReflectionTestUtils.setField(postgresConnector, "postgresConfig", config);
        ReflectionTestUtils.setField(postgresConnector, "postgresJdbcContext", jdbcContext);
        ReflectionTestUtils.setField(postgresConnector, "tapLogger", mock(Log.class));

        Boolean switched = ReflectionTestUtils.invokeMethod(postgresConnector, "switchCdcConnectionToSlave");

        Assertions.assertFalse(Boolean.TRUE.equals(switched));
        Assertions.assertEquals("primary", config.getHost());
        Assertions.assertEquals(5432, config.getPort());
        verify(jdbcContext, never()).refresh();
    }

    @Test
    void testPhysicalSlavePreferredSwitchesOnlyToCurrentTimelineSlave() throws Exception {
        TimelineProbePostgresConnector postgresConnector = new TimelineProbePostgresConnector();
        PostgresConfig config = physicalMasterSlaveConfig("primary", 5432,
                address("primary", 5432), address("stale-standby", 5433), address("healthy-standby", 5434));
        postgresConnector.addProbe("primary", 5432, false, "00000006", 6);
        postgresConnector.addProbe("stale-standby", 5433, true, "00000005", 5);
        postgresConnector.addProbe("healthy-standby", 5434, true, "00000006", 6);
        PostgresJdbcContext jdbcContext = mock(PostgresJdbcContext.class);

        ReflectionTestUtils.setField(postgresConnector, "postgresConfig", config);
        ReflectionTestUtils.setField(postgresConnector, "postgresJdbcContext", jdbcContext);
        ReflectionTestUtils.setField(postgresConnector, "tapLogger", mock(Log.class));

        Boolean switched = ReflectionTestUtils.invokeMethod(postgresConnector, "switchCdcConnectionToSlave");

        Assertions.assertTrue(Boolean.TRUE.equals(switched));
        Assertions.assertEquals("healthy-standby", config.getHost());
        Assertions.assertEquals(5434, config.getPort());
        verify(jdbcContext, times(1)).refresh();
    }

    @Test
    void testPhysicalSlavePreferredChoosesMoreReliableCurrentTimelineSlave() throws Exception {
        TimelineProbePostgresConnector postgresConnector = new TimelineProbePostgresConnector();
        PostgresConfig config = physicalMasterSlaveConfig("primary", 5432,
                address("primary", 5432), address("slower-standby", 5433), address("faster-standby", 5434));
        postgresConnector.addProbe("primary", 5432, false, "00000006", 6);
        postgresConnector.addProbe("slower-standby", 5433, true, "00000006", 6, "0/00001000");
        postgresConnector.addProbe("faster-standby", 5434, true, "00000006", 6, "0/00002000");
        PostgresJdbcContext jdbcContext = mock(PostgresJdbcContext.class);

        ReflectionTestUtils.setField(postgresConnector, "postgresConfig", config);
        ReflectionTestUtils.setField(postgresConnector, "postgresJdbcContext", jdbcContext);
        ReflectionTestUtils.setField(postgresConnector, "tapLogger", mock(Log.class));

        Boolean switched = ReflectionTestUtils.invokeMethod(postgresConnector, "switchCdcConnectionToSlave");

        Assertions.assertTrue(Boolean.TRUE.equals(switched));
        Assertions.assertEquals("faster-standby", config.getHost());
        Assertions.assertEquals(5434, config.getPort());
        verify(jdbcContext, times(1)).refresh();
    }

    private static PostgresConfig physicalMasterSlaveConfig(String host, int port,
                                                           LinkedHashMap<String, Integer>... addresses) {
        PostgresConfig config = new PostgresConfig();
        config.setDeploymentMode("master-slave");
        config.setLogPluginName("physical");
        config.setHost(host);
        config.setPort(port);
        ArrayList<LinkedHashMap<String, Integer>> list = new ArrayList<>();
        Collections.addAll(list, addresses);
        config.setMasterSlaveAddress(list);
        return config;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static LinkedHashMap<String, Integer> address(String host, int port) {
        LinkedHashMap address = new LinkedHashMap();
        address.put("host", host);
        address.put("port", port);
        return address;
    }

    private static class TimelineProbePostgresConnector extends PostgresConnector {
        private final Map<String, PostgresJdbcContext> probes = new HashMap<>();

        private void addProbe(String host, int port, boolean inRecovery, String timelineWalFileHex, int controlTimeline)
                throws SQLException {
            addProbe(host, port, inRecovery, timelineWalFileHex, controlTimeline, timelineWalFileHex);
        }

        private void addProbe(String host, int port, boolean inRecovery, String timelineWalFileHex, int controlTimeline,
                              String readableLsn) throws SQLException {
            PostgresJdbcContext context = mock(PostgresJdbcContext.class);
            doAnswer(invocation -> {
                String sql = invocation.getArgument(0);
                ResultSetConsumer consumer = invocation.getArgument(1);
                ResultSet resultSet = mock(ResultSet.class);
                if (sql.contains("pg_walfile_name")) {
                    when(resultSet.getString(1)).thenReturn(timelineWalFileHex);
                } else if (sql.contains("pg_control_checkpoint()")) {
                    when(resultSet.getInt(1)).thenReturn(controlTimeline);
                } else if (sql.contains("pg_last_wal_replay_lsn()") || sql.contains("pg_current_wal_flush_lsn()")) {
                    when(resultSet.getString(1)).thenReturn(readableLsn);
                } else if (sql.contains("pg_is_in_recovery()")) {
                    when(resultSet.getBoolean(1)).thenReturn(inRecovery);
                }
                consumer.accept(resultSet);
                return null;
            }).when(context).queryWithNext(anyString(), any(ResultSetConsumer.class));
            probes.put(host + ":" + port, context);
        }

        @Override
        protected PostgresJdbcContext newPostgresJdbcContext(PostgresConfig config) {
            return probes.get(config.getHost() + ":" + config.getPort());
        }
    }
}
