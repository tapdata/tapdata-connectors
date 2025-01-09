package io.tapdata.connector.gauss;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.connector.gauss.cdc.CdcOffset;
import io.tapdata.connector.gauss.cdc.GaussDBRunner;
import io.tapdata.connector.gauss.core.GaussColumn;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.gauss.core.GaussDBDDLSqlGenerator;
import io.tapdata.connector.gauss.core.GaussDBExceptionCollector;
import io.tapdata.connector.gauss.core.GaussDBJdbcContext;
import io.tapdata.connector.gauss.core.GaussDBRecordWriter;
import io.tapdata.connector.gauss.core.GaussDBSqlMaker;
import io.tapdata.connector.gauss.entity.TestAccept;
import io.tapdata.connector.gauss.enums.CdcConstant;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OpenGaussDBConnectorTest {
    OpenGaussDBConnector connector;
    TapConnectionContext connectionContext;
    TapConnectorContext connectorContext;
    Log log;

    @Test
    void testParams() {
        Assertions.assertEquals("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='%s' AND active='false'", OpenGaussDBConnector.CLEAN_SLOT_SQL);
        Assertions.assertEquals("SELECT pg_drop_replication_slot('%s')", OpenGaussDBConnector.DROP_SLOT_SQL);
        Assertions.assertEquals("SELECT pg_create_logical_replication_slot('%s','%s')", OpenGaussDBConnector.CREATE_SLOT_SQL);
        Assertions.assertEquals("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='%s'", OpenGaussDBConnector.SELECT_SLOT_COUNT_SQL);
        Assertions.assertEquals("ALTER TABLE \"%s\".\"%s\" REPLICA IDENTITY FULL", OpenGaussDBConnector.OPEN_IDENTITY_SQL);
        Assertions.assertEquals("select relname, relreplident from pg_class\n" +
                "where relnamespace=(select oid from pg_namespace where nspname='%s') and relname in (%s)", OpenGaussDBConnector.PG_REPLICATE_IDENTITY);
    }

    @Test
    void testInstance() {
        try (MockedStatic<GaussDBRunner> gr = mockStatic(GaussDBRunner.class)){
            gr.when(GaussDBRunner::instance).thenCallRealMethod();
            Assertions.assertNotNull(GaussDBRunner.instance());
        }
    }

    @BeforeEach
    void init() {
        connector = mock(OpenGaussDBConnector.class);
        connectionContext = mock(TapConnectionContext.class);
        connectorContext = mock(TapConnectorContext.class);
        log = mock(Log.class);
    }

    @Nested
    class OnStartTest {
        @Test
        void testNormal() {
            doNothing().when(connector).initConnection(connectionContext);
            doCallRealMethod().when(connector).onStart(connectionContext);
            Assertions.assertDoesNotThrow(() -> {
                connector.onStart(connectionContext);
                verify(connector, times(1)).initConnection(connectionContext);
            });
        }
    }

    @Nested
    class MakeTapFieldTest {
        GaussColumn column;
        DataMap dataMap;
        TapField tapField;
        @BeforeEach
        void init() {
            tapField = mock(TapField.class);
            dataMap = mock(DataMap.class);
            column = mock(GaussColumn.class);
            when(column.init(dataMap)).thenReturn(column);
            when(column.getTapField()).thenReturn(tapField);
            doCallRealMethod().when(connector).makeTapField(dataMap);
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<GaussColumn> c = mockStatic(GaussColumn.class)){
                    c.when(GaussColumn::instance).thenReturn(column);
                    TapField field = connector.makeTapField(dataMap);
                    Assertions.assertEquals(tapField, field);
                } finally {
                    verify(column, times(1)).init(dataMap);
                    verify(column, times(1)).getTapField();
                }
            });
        }

    }

    @Nested
    class ConnectionTestTest {
        GaussDBConfig gaussDBConfig;
        DataMap dataMap;
        ConnectionOptions connectionOptions;
        GaussDBTest gaussDBTest;
        Consumer<TestItem> consumer;
        @BeforeEach
        void init() throws Throwable {
            dataMap = mock(DataMap.class);
            gaussDBConfig = mock(GaussDBConfig.class);

            when(connectionContext.getConnectionConfig()).thenReturn(dataMap);
            when(gaussDBConfig.load(dataMap)).thenReturn(gaussDBConfig);
            when(gaussDBConfig.getConnectionString()).thenReturn("gaussDB-connection-str");
            connectionOptions = mock(ConnectionOptions.class);
            when(connectionOptions.connectionString(anyString())).thenReturn(connectionOptions);

            consumer = mock(Consumer.class);
            gaussDBTest = mock(GaussDBTest.class);

            when(connector.connectionTest(connectionContext, consumer)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<GaussDBConfig> gc = mockStatic(GaussDBConfig.class);
                     MockedStatic<ConnectionOptions> co = mockStatic(ConnectionOptions.class);
                     MockedStatic<GaussDBTest> gt = mockStatic(GaussDBTest.class)){
                    gc.when(GaussDBConfig::instance).thenReturn(gaussDBConfig);
                    co.when(ConnectionOptions::create).thenReturn(connectionOptions);
                    gt.when(() -> {
                        GaussDBTest.instance(any(GaussDBConfig.class), any(Consumer.class), any(TestAccept.class));
                    }).thenAnswer(a -> null);
                    ConnectionOptions cop = connector.connectionTest(connectionContext, consumer);
                    Assertions.assertEquals(connectionOptions, cop);
                } finally {
                    verify(gaussDBConfig, times(1)).getConnectionString();
                    verify(gaussDBConfig, times(1)).load(dataMap);
                    verify(connectionContext, times(1)).getConnectionConfig();
                    verify(connectionOptions, times(1)).connectionString(anyString());
                }
            });
        }
    }

    @Nested
    class OnDestroyTest {
        KVMap<Object> stateMap;
        GaussDBRunner cdcRunner;

        @BeforeEach
        void init() throws Throwable {
            cdcRunner = mock(GaussDBRunner.class);
            connector.cdcRunner = cdcRunner;
            doNothing().when(cdcRunner).closeCdcRunner();

            stateMap = mock(KVMap.class);
            when(stateMap.remove(CdcConstant.GAUSS_DB_SLOT_TAG)).thenReturn("");
            when(connectorContext.getStateMap()).thenReturn(stateMap);

            doNothing().when(connector).onStart(connectorContext);
            doNothing().when(connector).clearSlot();
            when(connectorContext.getLog()).thenReturn(log);
            doNothing().when(log).error(anyString(), anyString());
            doNothing().when(connector).onStart(connectorContext);

            doCallRealMethod().when(connector).onDestroy(connectorContext);
        }

        void assertVerify(int onStartTimes, int getStateMapTimes, int removeTimes,
                          int closeCdcRunnerTimes, int clearSlotTimes, int getLogTimes, int onStopTimes) throws Throwable {
            try {
                connector.onDestroy(connectorContext);
            } finally {
                verify(connector, times(onStartTimes)).onStart(connectorContext);
                verify(connectorContext, times(getStateMapTimes)).getStateMap();
                verify(stateMap, times(removeTimes)).remove(CdcConstant.GAUSS_DB_SLOT_TAG);
                verify(cdcRunner, times(closeCdcRunnerTimes)).closeCdcRunner();
                verify(connector, times(clearSlotTimes)).clearSlot();
                verify(connectorContext, times(getLogTimes)).getLog();
                verify(log, times(getLogTimes)).error(anyString(), anyString());
                verify(connector, times(onStopTimes)).onStop(connectorContext);
            }
        }

        @Test
        void testCdcRunnerIsNull() {
            connector.cdcRunner = null;
            connector.slotName = "name";
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1, 1, 1,
                        0, 1, 0, 1);
            });
        }

        @Test
        void testSlotNameIsNull() {
            connector.cdcRunner = cdcRunner;
            connector.slotName = null;
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1, 1, 1,
                        1, 0, 0, 1);
            });
        }

        @Test
        void testExceptionWhenClearSlot() throws Throwable {
            connector.cdcRunner = cdcRunner;
            connector.slotName = "name";
            doAnswer(a -> {
                throw new Exception("failed");
            }).when(connector).clearSlot();
            Assertions.assertThrows(Exception.class, () -> {
                assertVerify(1, 1, 1,
                        1, 1, 1, 1);
            });
        }
    }

    @Nested
    class ClearSlotTest {
        GaussDBJdbcContext gaussJdbcContext;

        @BeforeEach
        void init() throws SQLException {
            gaussJdbcContext = mock(GaussDBJdbcContext.class);
            connector.gaussJdbcContext = gaussJdbcContext;
            connector.slotName = "name";
        }
        @Test
        void testNormal() throws Throwable {
            doNothing().when(gaussJdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));
            doCallRealMethod().when(connector).clearSlot();
            Assertions.assertDoesNotThrow(() -> {
                connector.clearSlot();
            });
            verify(gaussJdbcContext, times(1)).queryWithNext(anyString(), any(ResultSetConsumer.class));
        }

        @Test
        void testResultSetResultSetMoreThanZero() throws SQLException {
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.getInt(1)).thenReturn(1);
            doNothing().when(gaussJdbcContext).execute(anyString());
            doCallRealMethod().when(connector).clearSlot(resultSet);
            connector.clearSlot(resultSet);
            verify(resultSet, times(1)).getInt(1);
            verify(gaussJdbcContext, times(1)).execute(anyString());
        }

        @Test
        void testResultSetResultSetLessThanZero() throws SQLException {
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.getInt(1)).thenReturn(-1);
            doNothing().when(gaussJdbcContext).execute(anyString());
            doCallRealMethod().when(connector).clearSlot(resultSet);
            connector.clearSlot(resultSet);
            verify(resultSet, times(1)).getInt(1);
            verify(gaussJdbcContext, times(0)).execute(anyString());
        }
    }

    @Nested
    class BuildSlotTest {
        DataMap connectionConfig;
        KVMap<Object> stateMap;
        GaussDBJdbcContext gaussJdbcContext;

        @BeforeEach
        void init() throws Throwable {
            connectionConfig = mock(DataMap.class);
            stateMap = mock(KVMap.class);
            gaussJdbcContext = mock(GaussDBJdbcContext.class);

            when(connectorContext.getLog()).thenReturn(log);
            when(connectorContext.getConnectionConfig()).thenReturn(connectionConfig);
            when(connectionConfig.getString("logPluginName")).thenReturn("mppdb_decoding");
            doNothing().when(gaussJdbcContext).execute(anyString());
            when(connectorContext.getStateMap()).thenReturn(stateMap);
            doNothing().when(stateMap).put(anyString(), anyString());
            doNothing().when(log).info(anyString(), anyString());
            doNothing().when(gaussJdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));

            connector.gaussJdbcContext = gaussJdbcContext;
        }

        void assertVerify(boolean nc, int sleepTimes,
                          int getConnectionConfigTimes,
                          int getStringTimes, int executeTimes,
                          int getStateMapTimes, int putTimes,
                          int infoTimes, int queryWithNextTimes) {
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<ConnectorBase> cb = mockStatic(ConnectorBase.class)){
                    cb.when(() -> {ConnectorBase.sleep(3000L);}).thenAnswer(a->{return null;});
                    connector.buildSlot(connectorContext, nc);
                    cb.verify(() -> ConnectorBase.sleep(3000L), times(sleepTimes));
                } finally {
                    verify(connectorContext, times(1)).getLog();
                    verify(connectorContext, times(getConnectionConfigTimes)).getConnectionConfig();
                    verify(connectionConfig, times(getStringTimes)).getString("logPluginName");
                    verify(gaussJdbcContext, times(executeTimes)).execute(anyString());
                    verify(connectorContext, times(getStateMapTimes)).getStateMap();
                    verify(stateMap, times(putTimes)).put(anyString(), anyString());
                    verify(log, times(infoTimes)).info(anyString(), anyString());
                    verify(gaussJdbcContext, times(queryWithNextTimes)).queryWithNext(anyString(), any(ResultSetConsumer.class));
                }
            });
        }

        @Test
        void testSlotNameIsNull() throws Throwable {
            connector.slotName = null;
            doCallRealMethod().when(connector).buildSlot(connectorContext, true);
            assertVerify(true, 1, 1,1,
                    1,1,1,
                    1,0);

        }

        @Test
        void testSlotNameNotNullButNeedCheck() throws Throwable {
            connector.slotName = "name";
            doCallRealMethod().when(connector).buildSlot(connectorContext, true);
            assertVerify(true, 0, 0,0,
                    0,0,0,
                    0,1);
        }

        @Test
        void testSlotNameIsNullAndNotNeedCheck() throws Throwable {
            connector.slotName = "name";
            doCallRealMethod().when(connector).buildSlot(connectorContext, false);
            assertVerify(false, 0, 0,0,
                    0,0,0,
                    0,0);
        }
    }

    @Nested
    class SelectSlotTest {
        ResultSet resultSet;
        @BeforeEach
        void init() throws SQLException {
            resultSet = mock(ResultSet.class);
            doCallRealMethod().when(connector).selectSlot(resultSet, log);
        }
        @Test
        void testLessThanZero() throws SQLException {
            when(resultSet.getInt(1)).thenReturn(0);
            doNothing().when(log).warn(anyString());
            Assertions.assertDoesNotThrow(() -> {
                connector.selectSlot(resultSet, log);
            });
            verify(log, times(1)).warn(anyString());
            verify(resultSet, times(1)).getInt(1);
        }
        @Test
        void testMoreThanZero() throws SQLException {
            connector.slotName = "name";
            when(resultSet.getInt(1)).thenReturn(2);
            doNothing().when(log).info(anyString(),anyString());
            Assertions.assertDoesNotThrow(() -> {
                connector.selectSlot(resultSet, log);
            });
            verify(log, times(1)).info(anyString(), anyString());
            verify(resultSet, times(1)).getInt(1);
        }
    }

    @Nested
    class TestReplicateIdentityTest {
        KVReadOnlyMap<TapTable> tableMap;
        GaussDBConfig gaussDBConfig;
        GaussDBJdbcContext gaussJdbcContext;
        @BeforeEach
        void init() throws SQLException {
            tableMap = mock(KVReadOnlyMap.class);
            gaussDBConfig = mock(GaussDBConfig.class);
            when(gaussDBConfig.getSchema()).thenReturn("schema");

            gaussJdbcContext = mock(GaussDBJdbcContext.class);

            doNothing().when(connector).iteratorTableMap(any(KVReadOnlyMap.class), anyList(), anyList());
            doNothing().when(connector).handleReplicateIdentity(any(ResultSet.class), anyList(), any(Log.class));
            connector.gaussDBConfig = gaussDBConfig;
            connector.gaussJdbcContext = gaussJdbcContext;
        }

        @Test
        void testNullKVReadOnlyMap() throws SQLException {
            doNothing().when(gaussJdbcContext).query(anyString(), any(ResultSetConsumer.class));
            doCallRealMethod().when(connector).testReplicateIdentity(null, log);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.testReplicateIdentity(null, log);
                } finally {
                    verify(connector, times(0)).iteratorTableMap(any(KVReadOnlyMap.class), anyList(), anyList());
                    verify(gaussDBConfig, times(0)).getSchema();
                    verify(gaussJdbcContext, times(0)).query(anyString(), any(ResultSetConsumer.class));
                    verify(log, times(0)).debug(anyString(), anyString());
                }
            });
        }

        @Test
        void testNotNullKVReadOnlyMap() throws SQLException {
            doAnswer(a -> {
                ResultSetConsumer argument = a.getArgument(1, ResultSetConsumer.class);
                argument.accept(mock(ResultSet.class));
                return null;
            }).when(gaussJdbcContext).query(anyString(), any(ResultSetConsumer.class));
            doCallRealMethod().when(connector).testReplicateIdentity(tableMap, log);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.testReplicateIdentity(tableMap, log);
                } finally {
                    verify(connector, times(1)).iteratorTableMap(any(KVReadOnlyMap.class), anyList(), anyList());
                    verify(gaussDBConfig, times(1)).getSchema();
                    verify(gaussJdbcContext, times(1)).query(anyString(), any(ResultSetConsumer.class));
                    verify(log, times(0)).debug(anyString(), anyString());
                    verify(connector, times(1)).handleReplicateIdentity(any(ResultSet.class), anyList(), any(Log.class));
                }
            });
        }

        @Test
        void testSQLException() throws SQLException {
            doAnswer(a -> {
                throw new SQLException("Exception");
            }).when(gaussJdbcContext).query(anyString(), any(ResultSetConsumer.class));
            doCallRealMethod().when(connector).testReplicateIdentity(tableMap, log);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.testReplicateIdentity(tableMap, log);
                } finally {
                    verify(connector, times(1)).iteratorTableMap(any(KVReadOnlyMap.class), anyList(), anyList());
                    verify(gaussDBConfig, times(1)).getSchema();
                    verify(gaussJdbcContext, times(1)).query(anyString(), any(ResultSetConsumer.class));
                    verify(log, times(1)).debug(anyString(), anyString());
                }
            });
        }
    }

    @Nested
    class IteratorTableMapTest {
        KVReadOnlyMap<TapTable> tableMap;
        List<String> tableList;
        List<String> hasPrimary;
        Iterator<Entry<TapTable>> iterator;
        Entry<TapTable> entry;
        TapTable value;
        Collection<String> primaryKeys;

        boolean hasNext;
        @BeforeEach
        void init() {
            primaryKeys = mock(Collection.class);
            value = mock(TapTable.class);
            entry = mock(Entry.class);
            when(entry.getKey()).thenReturn("key");
            hasNext = true;
            iterator = mock(Iterator.class);
            when(iterator.hasNext()).then(a -> {
                if (hasNext) {
                    hasNext = false;
                    return true;
                }
                return false;
            });
            when(iterator.next()).thenReturn(entry);

            tableMap = mock(KVReadOnlyMap.class);
            when(tableMap.iterator()).thenReturn(iterator);

            tableList = mock(List.class);
            when(tableList.add(anyString())).thenReturn(true);

            hasPrimary = mock(List.class);
            when(hasPrimary.add(anyString())).thenReturn(true);

            doCallRealMethod().when(connector).iteratorTableMap(tableMap, tableList, hasPrimary);
        }
        void assertVerify(int primaryKeysTimes, int isEmpty, int add) {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.iteratorTableMap(tableMap, tableList, hasPrimary);
                } finally {
                    verify(tableMap, times(1)).iterator();
                    verify(iterator, times(2)).hasNext();
                    verify(iterator, times(1)).next();
                    verify(entry, times(1)).getKey();
                    verify(tableList, times(1)).add(anyString());
                    verify(entry, times(1)).getValue();

                    verify(value, times(primaryKeysTimes)).primaryKeys();
                    verify(primaryKeys, times(isEmpty)).isEmpty();
                    verify(hasPrimary, times(add)).add(anyString());
                }
            });
        }

        @Test
        void testNormal() {
            when(primaryKeys.isEmpty()).thenReturn(false);
            when(value.primaryKeys()).thenReturn(primaryKeys);
            when(entry.getValue()).thenReturn(value);
            assertVerify(1, 1, 1);
        }

        @Test
        void testEntryValueIsNull() {
            when(primaryKeys.isEmpty()).thenReturn(false);
            when(value.primaryKeys()).thenReturn(primaryKeys);
            when(entry.getValue()).thenReturn(null);
            assertVerify(0, 0, 0);
        }

        @Test
        void testEntryValuePrimaryKeysIsNull() {
            when(primaryKeys.isEmpty()).thenReturn(false);
            when(value.primaryKeys()).thenReturn(null);
            when(entry.getValue()).thenReturn(value);
            assertVerify(1, 0, 0);
        }
        @Test
        void testEntryValuePrimaryKeysIsEmpty() {
            when(primaryKeys.isEmpty()).thenReturn(true);
            when(value.primaryKeys()).thenReturn(primaryKeys);
            when(entry.getValue()).thenReturn(value);
            assertVerify(1, 1, 0);
        }
    }

    @Nested
    class HandleReplicateIdentityTest {
        ResultSet resultSet;
        List<String> hasPrimary;
        boolean hasNext;
        @BeforeEach
        void init() throws SQLException {
            hasNext = true;
            resultSet = mock(ResultSet.class);
            when(resultSet.next()).then(a -> {
                if (hasNext) {
                    hasNext = false;
                    return true;
                }
                return false;
            });
            when(resultSet.getString("relname")).thenReturn("relname");

            hasPrimary = mock(List.class);

            doNothing().when(log).warn(anyString(), anyString());
            doCallRealMethod().when(connector).handleReplicateIdentity(resultSet, hasPrimary, log);
        }

        void assertVerify(int logs) {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.handleReplicateIdentity(resultSet, hasPrimary, log);
                } finally {
                    verify(resultSet, times(2)).next();
                    verify(resultSet, times(1)).getString("relname");
                    verify(resultSet, times(1)).getString("relreplident");
                    verify(hasPrimary, times(1)).contains("relname");
                    verify(log, times(logs)).warn(anyString(), anyString());
                }
            });
        }

        @Test
        void testRelReplNotFAndNotContainsName() throws SQLException {
            when(hasPrimary.contains("relname")).thenReturn(false);
            when(resultSet.getString("relreplident")).thenReturn("g");
            assertVerify(1);
        }

        @Test
        void testRelReplNotFNotDButContainsName() throws SQLException {
            when(hasPrimary.contains("relname")).thenReturn(true);
            when(resultSet.getString("relreplident")).thenReturn("g");
            assertVerify(1);
        }

        @Test
        void testRelReplIsF() throws SQLException {
            when(hasPrimary.contains("relname")).thenReturn(true);
            when(resultSet.getString("relreplident")).thenReturn("f");
            assertVerify(0);
        }
        @Test
        void testRelReplIsD() throws SQLException {
            when(hasPrimary.contains("relname")).thenReturn(true);
            when(resultSet.getString("relreplident")).thenReturn("d");
            assertVerify(0);
        }
        @Test
        void testRelReplNotDAndNotF() throws SQLException {
            when(hasPrimary.contains("relname")).thenReturn(true);
            when(resultSet.getString("relreplident")).thenReturn("Q");
            assertVerify(1);
        }
        @Test
        void testHasPrimaryContainsName() throws SQLException {
            when(hasPrimary.contains("relname")).thenReturn(true);
            when(resultSet.getString("relreplident")).thenReturn("f");
            assertVerify(0);
        }
        @Test
        void testHasPrimaryNotContainsName() throws SQLException {
            when(hasPrimary.contains("relname")).thenReturn(false);
            when(resultSet.getString("relreplident")).thenReturn("f");
            assertVerify(0);
        }
    }

    @Nested
    class OnStopTest {
        GaussDBRunner cdcRunner;
        GaussDBTest gaussDBTest;
        GaussDBJdbcContext gaussJdbcContext;
        @BeforeEach
        void init() {
            cdcRunner = mock(GaussDBRunner.class);
            doNothing().when(cdcRunner).closeCdcRunner();
            gaussDBTest = mock(GaussDBTest.class);
            gaussJdbcContext = mock(GaussDBJdbcContext.class);
            when(connectionContext.getLog()).thenReturn(log);
            doNothing().when(log).debug(anyString(), anyString());
            doCallRealMethod().when(connector).onStop(connectionContext);
        }
        @Test
        void testNormal() {
            connector.cdcRunner = cdcRunner;
            connector.gaussJdbcContext = gaussJdbcContext;
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<EmptyKit> ek = mockStatic(EmptyKit.class)){
                    ek.when(() -> EmptyKit.closeQuietly(gaussDBTest)).then(a->null);
                    ek.when(() -> EmptyKit.closeQuietly(gaussJdbcContext)).then(a->null);
                    connector.onStop(connectionContext);
                } finally {
                    verify(connectionContext, times(0)).getLog();
                    verify(log, times(0)).debug(anyString(), anyString());
                    verify(cdcRunner, times(1)).closeCdcRunner();
                }
            });
        }
        @Test
        void testCdcRunnerIsNull() {
            connector.cdcRunner = null;
            connector.gaussJdbcContext = gaussJdbcContext;
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<EmptyKit> ek = mockStatic(EmptyKit.class)){
                    ek.when(() -> EmptyKit.closeQuietly(gaussDBTest)).then(a->null);
                    ek.when(() -> EmptyKit.closeQuietly(gaussJdbcContext)).then(a->null);
                    connector.onStop(connectionContext);
                } finally {
                    verify(connectionContext, times(0)).getLog();
                    verify(log, times(0)).debug(anyString(), anyString());
                    verify(cdcRunner, times(0)).closeCdcRunner();
                }
            });
        }
        @Test
        void testCdcRunnerException() {
            connector.cdcRunner = cdcRunner;
            connector.gaussJdbcContext = gaussJdbcContext;
            doAnswer(a -> {
                throw new Exception("exception");
            }).when(cdcRunner).closeCdcRunner();
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<EmptyKit> ek = mockStatic(EmptyKit.class)){
                    ek.when(() -> EmptyKit.closeQuietly(gaussDBTest)).then(a->null);
                    ek.when(() -> EmptyKit.closeQuietly(gaussJdbcContext)).then(a->null);
                    connector.onStop(connectionContext);
                } finally {
                    verify(connectionContext, times(1)).getLog();
                    verify(log, times(1)).debug(anyString(), anyString());
                    verify(cdcRunner, times(1)).closeCdcRunner();
                }
            });
        }
    }

    @Nested
    @Disabled
    class InitConnectionTest {
        GaussDBConfig gaussDBConfig;
        GaussDBJdbcContext gaussDBJdbcContext;
        GaussDBSqlMaker gaussDBSqlMaker;
        GaussDBDDLSqlGenerator gaussDBDDLSqlGenerator;
        GaussDBExceptionCollector gaussDBExceptionCollector;
        DataMap connectionConfig;

        void init() {
            connectionConfig = mock(DataMap.class);
            gaussDBExceptionCollector = mock(GaussDBExceptionCollector.class);
            gaussDBDDLSqlGenerator = mock(GaussDBDDLSqlGenerator.class);
            gaussDBSqlMaker = mock(GaussDBSqlMaker.class);
            gaussDBJdbcContext = mock(GaussDBJdbcContext.class);
            gaussDBConfig = mock(GaussDBConfig.class);

            when(connectionContext.getConnectionConfig()).thenReturn(connectionConfig);
            when(gaussDBConfig.load(connectionConfig)).thenReturn(gaussDBConfig);
            when(gaussDBConfig.getCloseNotNull()).thenReturn(true);
            when(gaussDBSqlMaker.closeNotNull(anyBoolean())).thenReturn(gaussDBSqlMaker);
            when(gaussDBJdbcContext.queryVersion()).thenReturn("9.2");
            when(connectionContext.getLog()).thenReturn(log);
        }

        @Test
        void testConnectionContext() {
            init();
            doNothing().when(connector).isConnectorStarted(any(TapConnectionContext.class), any(Consumer.class));
            doCallRealMethod().when(connector).initConnection(connectionContext);
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<GaussDBConfig> gc = mockStatic(GaussDBConfig.class);
                     MockedStatic<GaussDBJdbcContext> gjc = mockStatic(GaussDBJdbcContext.class);
                     MockedStatic<GaussDBSqlMaker> gsm = mockStatic(GaussDBSqlMaker.class);
                     MockedStatic<GaussDBDDLSqlGenerator> gsg = mockStatic(GaussDBDDLSqlGenerator.class);
                     MockedStatic<GaussDBExceptionCollector> gec = mockStatic(GaussDBExceptionCollector.class)) {
                    gc.when(GaussDBConfig::instance).thenReturn(gaussDBConfig);

                    gjc.when(() -> GaussDBJdbcContext.instance(gaussDBConfig)).thenReturn(gaussDBJdbcContext);
                    gsm.when(GaussDBSqlMaker::instance).thenReturn(gaussDBSqlMaker);
                    gsg.when(GaussDBDDLSqlGenerator::instance).thenReturn(gaussDBDDLSqlGenerator);
                    gec.when(GaussDBExceptionCollector::instance).thenReturn(gaussDBExceptionCollector);
                    connector.initConnection(connectionContext);
                    gc.verify(GaussDBConfig::instance, times(1));
                    gjc.verify(() -> GaussDBJdbcContext.instance(gaussDBConfig), times(1));
                    gsm.verify(GaussDBSqlMaker::instance, times(1));
                    gsg.verify(GaussDBDDLSqlGenerator::instance, times(1));
                    gec.verify(GaussDBExceptionCollector::instance, times(1));
                } finally {
                    verify(connectionContext, times(1)).getConnectionConfig();
                    verify(gaussDBConfig, times(1)).load(connectionConfig);
                    verify(gaussDBConfig, times(1)).getCloseNotNull();
                    verify(gaussDBSqlMaker, times(1)).closeNotNull(anyBoolean());
                    verify(gaussDBJdbcContext, times(1)).queryVersion();
                    verify(connectionContext, times(1)).getLog();
                }
            });
        }

        @Test
        void testConnectorContext() {
            KVMap kvMap = mock(KVMap.class);
            connectionContext = mock(TapConnectorContext.class);
            init();
            doCallRealMethod().when(connector).initConnection(connectionContext);
            when(((TapConnectorContext)connectionContext).getStateMap()).thenReturn(kvMap);
            when(kvMap.get(CdcConstant.GAUSS_DB_SLOT_TAG)).thenReturn("name");
            when(connectionContext.getNodeConfig()).thenReturn(connectionConfig);
            doCallRealMethod().when(connector).isConnectorStarted(any(TapConnectionContext.class), any(Consumer.class));
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<GaussDBConfig> gc = mockStatic(GaussDBConfig.class);
                     MockedStatic<GaussDBJdbcContext> gjc = mockStatic(GaussDBJdbcContext.class);
                     MockedStatic<GaussDBSqlMaker> gsm = mockStatic(GaussDBSqlMaker.class);
                     MockedStatic<GaussDBDDLSqlGenerator> gsg = mockStatic(GaussDBDDLSqlGenerator.class);
                     MockedStatic<GaussDBExceptionCollector> gec = mockStatic(GaussDBExceptionCollector.class)) {
                    gc.when(GaussDBConfig::instance).thenReturn(gaussDBConfig);
                    gjc.when(() -> GaussDBJdbcContext.instance(gaussDBConfig)).thenReturn(gaussDBJdbcContext);
                    gsm.when(GaussDBSqlMaker::instance).thenReturn(gaussDBSqlMaker);
                    gsg.when(GaussDBDDLSqlGenerator::instance).thenReturn(gaussDBDDLSqlGenerator);
                    gec.when(GaussDBExceptionCollector::instance).thenReturn(gaussDBExceptionCollector);
                    connector.initConnection(connectionContext);
                    gc.verify(GaussDBConfig::instance, times(1));
                    gjc.verify(() -> GaussDBJdbcContext.instance(gaussDBConfig), times(1));
                    gsm.verify(GaussDBSqlMaker::instance, times(1));
                    gsg.verify(GaussDBDDLSqlGenerator::instance, times(1));
                    gec.verify(GaussDBExceptionCollector::instance, times(1));
                } finally {
                    verify(connectionContext, times(1)).getConnectionConfig();
                    verify(gaussDBConfig, times(2)).load(connectionConfig);
                    verify(gaussDBConfig, times(1)).getCloseNotNull();
                    verify(gaussDBSqlMaker, times(1)).closeNotNull(anyBoolean());
                    verify(gaussDBJdbcContext, times(1)).queryVersion();
                    verify(connectionContext, times(1)).getLog();
                    verify((TapConnectorContext)connectionContext, times(1)).getStateMap();
                    verify(connectionContext, times(1)).getNodeConfig();
                    verify(kvMap, times(1)).get(CdcConstant.GAUSS_DB_SLOT_TAG);
                }
            });
        }
    }

    @Nested
    class IsConnectorStartedTest {
        Consumer<TapConnectorContext> contextConsumer;
        @BeforeEach
        void init() {
            contextConsumer = mock(Consumer.class);
            doNothing().when(contextConsumer).accept(connectorContext);
        }

        @Test
        void testConnectionContext() {
            doCallRealMethod().when(connector).isConnectorStarted(connectionContext, contextConsumer);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.isConnectorStarted(connectionContext, contextConsumer);
                } finally {
                    verify(contextConsumer, times(0)).accept(connectorContext);
                }
            });
        }

        @Test
        void testConnectorContext() {
            doCallRealMethod().when(connector).isConnectorStarted(connectorContext, contextConsumer);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.isConnectorStarted(connectorContext, contextConsumer);
                } finally {
                    verify(contextConsumer, times(1)).accept(connectorContext);
                }
            });
        }

        @Test
        void testContextConsumerIsNull() {
            doCallRealMethod().when(connector).isConnectorStarted(connectionContext, null);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.isConnectorStarted(connectionContext, null);
                } finally {
                    verify(contextConsumer, times(0)).accept(connectorContext);
                }
            });
        }

        @Test
        void testContextConsumerIsNull2() {
            doCallRealMethod().when(connector).isConnectorStarted(connectorContext, null);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.isConnectorStarted(connectorContext, null);
                } finally {
                    verify(contextConsumer, times(0)).accept(connectorContext);
                }
            });
        }
    }

    @Nested
    class OpenIdentityTest {
        TapTable tapTable;
        Collection<String> primaryKeys;
        List<TapIndex> indexList;
        Stream<TapIndex> stream;
        GaussDBJdbcContext gaussJdbcContext;
        GaussDBConfig gaussDBConfig;

        @BeforeEach
        void init() throws SQLException {
            tapTable = mock(TapTable.class);
            when(tapTable.getId()).thenReturn("table");
            primaryKeys = mock(Collection.class);
            indexList = mock(List.class);
            stream = mock(Stream.class);
            when(indexList.stream()).thenReturn(stream);
            gaussJdbcContext = mock(GaussDBJdbcContext.class);
            gaussDBConfig = mock(GaussDBConfig.class);
            connector.gaussJdbcContext = gaussJdbcContext;
            connector.gaussDBConfig = gaussDBConfig;
            when(gaussDBConfig.getSchema()).thenReturn("schema");
            doNothing().when(gaussJdbcContext).execute(anyString());

            doCallRealMethod().when(connector).openIdentity(null);
            doCallRealMethod().when(connector).openIdentity(tapTable);
        }



        void assertVerify(TapTable t,Collection<String> p,List<TapIndex> in,
                          boolean hpk, boolean hsIndex, boolean hsUnique,
                          int pks, int pkIsEmpty, int iList, int inIsEmpty, int streamS, int noneMatch, int schemaS, int getId, int exec) {
            when(tapTable.getIndexList()).thenReturn(in);
            when(tapTable.primaryKeys()).thenReturn(p);
            when(primaryKeys.isEmpty()).thenReturn(hpk);
            when(indexList.isEmpty()).thenReturn(hsIndex);
            when(stream.noneMatch(any())).thenReturn(hsUnique);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.openIdentity(t);
                } finally {
                    verify(tapTable, times(getId)).getId();
                    verify(tapTable, times(pks)).primaryKeys();
                    verify(tapTable, times(iList)).getIndexList();
                    verify(indexList, times(streamS)).stream();
                    verify(gaussDBConfig, times(schemaS)).getSchema();
                    verify(gaussJdbcContext, times(exec)).execute(anyString());
                    verify(primaryKeys, times(pkIsEmpty)).isEmpty();
                    verify(indexList, times(inIsEmpty)).isEmpty();
                    verify(stream, times(noneMatch)).noneMatch(any());
                }
            });
        }

        @Test
        void testTapTableIsNull() {
            assertVerify(null, primaryKeys, indexList, false, false, false,
                    0, 0 , 0 ,0 , 0, 0,
                    0, 0, 0);
        }

        /**
         * NO  primaryKeysIsEmpty  indexListIsEmpty   noneMatchUnique
         * NO1 false               false              false
         * NO2 true                false              false
         * NO3 false               true               false
         * NO4 false               false              true
         * NO5 true                true               false
         * NO6 true                false              true
         * NO7 false               true               true
         * NO8 true                true               true
         * */
        @Test
        void testNO1() {
            assertVerify(tapTable, primaryKeys, indexList,
                    false, false, false,
                    1, 1 , 1 ,1 , 1, 1,
                    0, 0, 0);
        }

        @Test
        void testNO2() {
            assertVerify(tapTable,primaryKeys, indexList,
                    true, false, false,
                    1, 1 , 1 ,1 , 1, 1,
                    0, 0, 0);
        }
        @Test
        void testNO2PKIsNull() {
            assertVerify(tapTable, null, indexList,
                    false, false, false,
                    1, 0 , 1 ,1 , 1, 1,
                    0, 0, 0);
        }

        @Test
        void testNO3() {
            assertVerify(tapTable, primaryKeys, indexList,
                    false, true, false,
                    1, 1 , 1 ,1 , 1, 1,
                    0, 0, 0);
        }
        @Test
        void testNO3IndexIsNull() {
            assertVerify(tapTable, primaryKeys, null,
                    false, true, false,
                    1, 1 , 1 ,0 , 0, 0,
                    1, 1, 1);
        }

        @Test
        void testNO4() {
            assertVerify(tapTable, primaryKeys, indexList,
                    false, false, true,
                    1, 1 , 1 ,1 , 1, 1,
                    1, 1, 1);
        }

        @Test
        void testNO5() {
            assertVerify(tapTable,primaryKeys, indexList,
                    true, true, false,
                    1, 1 , 1 ,1 , 0, 0,
                    1, 1, 1);
        }

        @Test
        void testNO6() {
            assertVerify(tapTable,primaryKeys, indexList,
                    true, false, true,
                    1, 1 , 1 ,1 , 1, 1,
                    1, 1, 1);
        }

        @Test
        void testNO7() {
            assertVerify(tapTable,primaryKeys, indexList,
                    false, true, true,
                    1, 1 , 1 ,1 , 1, 1,
                    1, 1, 1);
        }

        @Test
        void testNO8() {
            assertVerify(tapTable,primaryKeys, indexList,
                    true, true, true,
                    1, 1 , 1 ,1 , 0, 0,
                    1, 1, 1);
        }
    }

    @Nested
    class MakeSureHasUniqueTest {
        GaussDBJdbcContext gaussJdbcContext;
        TapTable tapTable;
        List<DataMap> index;
        @BeforeEach
        void init() throws SQLException {
            index = new ArrayList<>();
            gaussJdbcContext = mock(GaussDBJdbcContext.class);
            tapTable = mock(TapTable.class);
            when(tapTable.getId()).thenReturn("table");
            when(gaussJdbcContext.queryAllIndexes(anyList())).thenReturn(index);
            when(connector.makeSureHasUnique(tapTable)).thenCallRealMethod();
            connector.gaussJdbcContext = gaussJdbcContext;
        }

        @Test
        void testEqualsIsUnique() {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    boolean b = connector.makeSureHasUnique(tapTable);
                    Assertions.assertFalse(b);
                } finally {
                    verify(tapTable, times(1)).getId();
                    verify(gaussJdbcContext, times(1)).queryAllIndexes(anyList());
                }
            });
        }

        @Test
        void testNotEqualsIsUnique() {
            DataMap dataMap = new DataMap();
            dataMap.put("isUnique", "1");
            index.add(dataMap);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    boolean b = connector.makeSureHasUnique(tapTable);
                    Assertions.assertTrue(b);
                } finally {
                    verify(tapTable, times(1)).getId();
                    verify(gaussJdbcContext, times(1)).queryAllIndexes(anyList());
                }
            });
        }
    }

    @Nested
    class WriteRecordTest {
        GaussDBJdbcContext gaussJdbcContext;
        List<TapRecordEvent> tapRecordEvents;
        TapTable tapTable;
        ConnectorCapabilities capabilities;
        Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer;
        GaussDBRecordWriter writer;

        Connection connection;
        String version;
        @BeforeEach
        void init() throws SQLException {
            version = "9.2";
            gaussJdbcContext = mock(GaussDBJdbcContext.class);

            tapRecordEvents = mock(List.class);
            tapTable = mock(TapTable.class);
            capabilities = mock(ConnectorCapabilities.class);
            writeListResultConsumer = mock(Consumer.class);
            writer = mock(GaussDBRecordWriter.class);

            when(connector.hasUniqueIndex(tapTable)).thenReturn(true);
            when(connectorContext.getConnectorCapabilities()).thenReturn(capabilities);
            when(connector.initInsertDmlPolicy(capabilities)).thenReturn("insertDmlPolicy");
            when(connector.initUpdateDmlPolicy(capabilities)).thenReturn("updateDmlPolicy");
            when(connector.getGaussDBVersion(anyBoolean())).thenReturn(version);

            when(connector.initConnectionIsTransaction()).thenReturn(connection);
            when(writer.setInsertPolicy("insertDmlPolicy")).thenReturn(writer);
            when(writer.setUpdatePolicy("updateDmlPolicy")).thenReturn(writer);
            when(connectorContext.getLog()).thenReturn(log);
            when(writer.setTapLogger(log)).thenReturn(writer);
            doNothing().when(writer).write(anyList(), any(Consumer.class), any(Supplier.class));

            doCallRealMethod().when(connector).writeRecord(connectorContext, tapRecordEvents, tapTable, writeListResultConsumer);
            connector.gaussJdbcContext = gaussJdbcContext;
        }

        void assertVerify(int instance1, int instance2) {
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<GaussDBRecordWriter> gw = mockStatic(GaussDBRecordWriter.class)){
                    gw.when(() -> GaussDBRecordWriter.instance(gaussJdbcContext, tapTable, version)).thenReturn(writer);
                    gw.when(() -> GaussDBRecordWriter.instance(gaussJdbcContext, connection, tapTable, version)).thenReturn(writer);
                    connector.writeRecord(connectorContext, tapRecordEvents, tapTable, writeListResultConsumer);
                    gw.verify(() -> GaussDBRecordWriter.instance(gaussJdbcContext, tapTable, version), times(instance1));
                    gw.verify(() -> GaussDBRecordWriter.instance(gaussJdbcContext, connection, tapTable, version), times(instance2));
                } finally {
                    verify(connector, times(1)).hasUniqueIndex(tapTable);
                    verify(connectorContext, times(1)).getConnectorCapabilities();
                    verify(connector, times(1)).initInsertDmlPolicy(capabilities);
                    verify(connector, times(1)).initUpdateDmlPolicy(capabilities);
                    verify(connector, times(1)).getGaussDBVersion(anyBoolean());
                    verify(connector, times(1)).isTransaction();
                    verify(connector, times(instance2)).initConnectionIsTransaction();
                    verify(writer, times(1)).setInsertPolicy("insertDmlPolicy");
                    verify(writer, times(1)).setUpdatePolicy("updateDmlPolicy");
                    verify(connectorContext, times(1)).getLog();
                    verify(writer, times(1)).setTapLogger(log);
                    verify(writer, times(1)).write(anyList(), any(Consumer.class), any(Supplier.class));
                }
            });
        }
        @Test
        void testIsTransaction() {
            when(connector.isTransaction()).thenReturn(true);
            assertVerify(0, 1);
        }
        @Test
        void testNotTransaction() {
            when(connector.isTransaction()).thenReturn(false);
            assertVerify(1, 0);
        }
    }

    @Nested
    class IsTransactionTest {
        @Test
        void testNormal() {
            when(connector.isTransaction()).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> {
                boolean transaction = connector.isTransaction();
                Assertions.assertFalse(transaction);
            });
        }
    }

    @Nested
    class GaussDBVersionTest {
        @BeforeEach
        void init() {
            connector.gaussDBVersion = "9.2";
            when(connector.getGaussDBVersion(anyBoolean())).thenCallRealMethod();
        }
        @Test
        void testHasUniqueIndex() {
            Assertions.assertDoesNotThrow(() -> {
                String s = connector.getGaussDBVersion(true);
                Assertions.assertEquals("9.2", s);
            });
        }
        @Test
        void testNotHasUniqueIndex() {
            Assertions.assertDoesNotThrow(() -> {
                String s = connector.getGaussDBVersion(false);
                Assertions.assertEquals("90500", s);
            });
        }
    }

    @Nested
    class InitConnectionIsTransactionTest {
        Connection connection;
        Map<String, Connection> transactionConnectionMap;
        GaussDBJdbcContext gaussJdbcContext;
        @BeforeEach
        void init() throws SQLException {
            connection = mock(Connection.class);
            transactionConnectionMap = mock(Map.class);
            gaussJdbcContext = mock(GaussDBJdbcContext.class);

            when(transactionConnectionMap.get(anyString())).thenReturn(connection);
            when(transactionConnectionMap.put(anyString(), any(Connection.class))).thenReturn(connection);
            when(connector.transactionConnectionMap()).thenReturn(transactionConnectionMap);
            when(gaussJdbcContext.getConnection()).thenReturn(connection);

            when(connector.initConnectionIsTransaction()).thenCallRealMethod();
            connector.gaussJdbcContext = gaussJdbcContext;
        }
        @Test
        void testContainKey() {
            when(transactionConnectionMap.containsKey(anyString())).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    Connection c = connector.initConnectionIsTransaction();
                    Assertions.assertEquals(connection, c);
                } finally {
                    verify(transactionConnectionMap, times(1)).get(anyString());
                    verify(transactionConnectionMap, times(0)).put(anyString(), any(Connection.class));
                    verify(connector, times(1)).transactionConnectionMap();
                    verify(gaussJdbcContext, times(0)).getConnection();
                    verify(transactionConnectionMap, times(1)).containsKey(anyString());
                }
            });
        }

        @Test
        void testNotContainKey() {
            when(transactionConnectionMap.containsKey(anyString())).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    Connection c = connector.initConnectionIsTransaction();
                    Assertions.assertEquals(connection, c);
                } finally {
                    verify(transactionConnectionMap, times(0)).get(anyString());
                    verify(transactionConnectionMap, times(1)).put(anyString(), any(Connection.class));
                    verify(connector, times(1)).transactionConnectionMap();
                    verify(gaussJdbcContext, times(1)).getConnection();
                    verify(transactionConnectionMap, times(1)).containsKey(anyString());
                }
            });
        }
    }

    @Nested
    class TransactionConnectionMap {
        @Test
        void testNormal() {
            when(connector.transactionConnectionMap()).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> {
                Map<String, Connection> map = connector.transactionConnectionMap();
                Assertions.assertNull(map);
            });
        }
    }

    @Nested
    class InitInsertDmlPolicyTest {
        ConnectorCapabilities capabilities;
        @BeforeEach
        void init() {
            capabilities = mock(ConnectorCapabilities.class);
            when(capabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)).thenReturn("insertDmlPolicy");
            when(connector.initInsertDmlPolicy(capabilities)).thenCallRealMethod();
            when(connector.initInsertDmlPolicy(null)).thenCallRealMethod();
        }
        @Test
        void testConnectorCapabilitiesIsNull() {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    String policy = connector.initInsertDmlPolicy(null);
                    Assertions.assertEquals(ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS, policy);
                } finally {
                    verify(capabilities, times(0)).getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
                }
            });
        }
        @Test
        void testGetCapabilityAlternativeIsNull() {
            when(capabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    String policy = connector.initInsertDmlPolicy(capabilities);
                    Assertions.assertEquals(ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS, policy);
                } finally {
                    verify(capabilities, times(1)).getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
                }
            });
        }
        @Test
        void testGetCapabilityAlternativeNotNull() {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    String policy = connector.initInsertDmlPolicy(capabilities);
                    Assertions.assertEquals("insertDmlPolicy", policy);
                } finally {
                    verify(capabilities, times(1)).getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
                }
            });
        }
    }
    @Nested
    class InitUpdateDmlPolicyTest {
        ConnectorCapabilities capabilities;
        @BeforeEach
        void init() {
            capabilities = mock(ConnectorCapabilities.class);
            when(capabilities.getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY)).thenReturn("updateDmlPolicy");
            when(connector.initUpdateDmlPolicy(capabilities)).thenCallRealMethod();
            when(connector.initUpdateDmlPolicy(null)).thenCallRealMethod();
        }
        @Test
        void testConnectorCapabilitiesIsNull() {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    String policy = connector.initUpdateDmlPolicy(null);
                    Assertions.assertEquals(ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS, policy);
                } finally {
                    verify(capabilities, times(0)).getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
                }
            });
        }
        @Test
        void testGetCapabilityAlternativeIsNull() {
            when(capabilities.getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY)).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    String policy = connector.initUpdateDmlPolicy(capabilities);
                    Assertions.assertEquals(ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS, policy);
                } finally {
                    verify(capabilities, times(1)).getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
                }
            });
        }
        @Test
        void testGetCapabilityAlternativeNotNull() {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    String policy = connector.initUpdateDmlPolicy(capabilities);
                    Assertions.assertEquals("updateDmlPolicy", policy);
                } finally {
                    verify(capabilities, times(1)).getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
                }
            });
        }
    }
    @Nested
    class HasUniqueIndexTest {
        TapTable tapTable;
        Map<String, Boolean> writtenTableMap;
        @BeforeEach
        void init() throws SQLException {
            tapTable = mock(TapTable.class);
            writtenTableMap = mock(Map.class);

            when(tapTable.getId()).thenReturn("tableId");

            doNothing().when(connector).openIdentity(tapTable);
            when(connector.makeSureHasUnique(tapTable)).thenReturn(true);
            when(writtenTableMap.put("tableId", true)).thenReturn(true);
            when(writtenTableMap.get("tableId")).thenReturn(true);
            connector.writtenTableMap = writtenTableMap;

            when(connector.hasUniqueIndex(tapTable)).thenCallRealMethod();
        }
        @Test
        void testWrittenTableMapNotHasTable() {
            when(writtenTableMap.get("tableId")).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    boolean index = connector.hasUniqueIndex(tapTable);
                    Assertions.assertTrue(index);
                } finally {
                    verify(tapTable, times(1)).getId();
                    verify(connector, times(1)).openIdentity(tapTable);
                    verify(connector, times(1)).makeSureHasUnique(tapTable);
                    verify(writtenTableMap, times(1)).put("tableId", true);
                    verify(writtenTableMap, times(1)).get("tableId");
                }
            });
        }
        @Test
        void testWrittenTableMapHasTable() {
            when(writtenTableMap.get("tableId")).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    boolean index = connector.hasUniqueIndex(tapTable);
                    Assertions.assertFalse(index);
                } finally {
                    verify(tapTable, times(1)).getId();
                    verify(connector, times(0)).openIdentity(tapTable);
                    verify(connector, times(0)).makeSureHasUnique(tapTable);
                    verify(writtenTableMap, times(0)).put("tableId", true);
                    verify(writtenTableMap, times(1)).get("tableId");
                }
            });
        }
    }

    @Nested
    class TimestampToStreamOffsetTest {
        GaussDBTest gaussDBTest;
        KVReadOnlyMap<TapTable> map;
        GaussDBConfig gaussDBConfig;
        @BeforeEach
        void init() throws Throwable {
            gaussDBTest = mock(GaussDBTest.class);
            map = mock(KVReadOnlyMap.class);
            when(connectorContext.getLog()).thenReturn(log);
            doNothing().when(log).warn(anyString());
            gaussDBConfig = mock(GaussDBConfig.class);
            connector.gaussDBConfig = gaussDBConfig;

            when(connectorContext.getTableMap()).thenReturn(map);

            doNothing().when(connector).testReplicateIdentity(map, log);
            doNothing().when(connector).buildSlot(connectorContext, false);

            when(connector.timestampToStreamOffset(connectorContext, null)).thenCallRealMethod();
            when(connector.timestampToStreamOffset(connectorContext, 100L)).thenCallRealMethod();
        }

        @Test
        void testOffsetStartTimeNotNull() {
            when(gaussDBTest.testStreamRead()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<GaussDBTest> gt = mockStatic(GaussDBTest.class)) {
                    gt.when(() -> GaussDBTest.instance(any(GaussDBConfig.class), any(Consumer.class), any(TestAccept.class))).then(a -> {
                        Consumer consumer = a.getArgument(1, Consumer.class);
                        consumer.accept(mock(TestItem.class));
                        TestAccept accept = a.getArgument(2, TestAccept.class);
                        accept.accept(gaussDBTest);
                        return gaussDBTest;
                    });
                    Object offset = connector.timestampToStreamOffset(connectorContext, 100L);
                    gt.verify(() -> GaussDBTest.instance(any(GaussDBConfig.class), any(Consumer.class), any(TestAccept.class)), times(1));
                    Assertions.assertNotNull(offset);
                    Assertions.assertEquals(HashMap.class.getName(), offset.getClass().getName());
                } finally {
                    verify(connectorContext, times(1)).getLog();
                    verify(connectorContext, times(1)).getTableMap();
                    verify(log, times(1)).warn(anyString());
                    verify(gaussDBTest, times(1)).testStreamRead();
                    verify(connector, times(1)).testReplicateIdentity(map, log);
                    verify(connector, times(1)).buildSlot(connectorContext, false);
                }
            });
        }

        @Test
        void testCanCdc() {
            when(gaussDBTest.testStreamRead()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<GaussDBTest> gt = mockStatic(GaussDBTest.class)) {
                    gt.when(() -> GaussDBTest.instance(any(GaussDBConfig.class), any(Consumer.class), any(TestAccept.class))).then(a -> {
                        Consumer consumer = a.getArgument(1, Consumer.class);
                        consumer.accept(mock(TestItem.class));
                        TestAccept accept = a.getArgument(2, TestAccept.class);
                        accept.accept(gaussDBTest);
                        return gaussDBTest;
                    });
                    Object offset = connector.timestampToStreamOffset(connectorContext, null);
                    gt.verify(() -> GaussDBTest.instance(any(GaussDBConfig.class), any(Consumer.class), any(TestAccept.class)), times(1));
                    Assertions.assertNotNull(offset);
                    Assertions.assertEquals(HashMap.class.getName(), offset.getClass().getName());
                } finally {
                    verify(connectorContext, times(1)).getLog();
                    verify(connectorContext, times(1)).getTableMap();
                    verify(log, times(0)).warn(anyString());
                    verify(gaussDBTest, times(1)).testStreamRead();
                    verify(connector, times(1)).testReplicateIdentity(map, log);
                    verify(connector, times(1)).buildSlot(connectorContext, false);
                }
            });
        }

        @Test
        void testCanNotCdc() {
            when(gaussDBTest.testStreamRead()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<GaussDBTest> gt = mockStatic(GaussDBTest.class)) {
                    gt.when(() -> GaussDBTest.instance(any(GaussDBConfig.class), any(Consumer.class), any(TestAccept.class))).then(a -> {
                        Consumer consumer = a.getArgument(1, Consumer.class);
                        consumer.accept(mock(TestItem.class));
                        TestAccept accept = a.getArgument(2, TestAccept.class);
                        accept.accept(gaussDBTest);
                        return gaussDBTest;
                    });
                    Object offset = connector.timestampToStreamOffset(connectorContext, null);
                    gt.verify(() -> GaussDBTest.instance(any(GaussDBConfig.class), any(Consumer.class), any(TestAccept.class)), times(1));
                    Assertions.assertNotNull(offset);
                    Assertions.assertEquals(HashMap.class.getName(), offset.getClass().getName());
                } finally {
                    verify(connectorContext, times(1)).getLog();
                    verify(connectorContext, times(0)).getTableMap();
                    verify(log, times(0)).warn(anyString());
                    verify(gaussDBTest, times(1)).testStreamRead();
                    verify(connector, times(0)).testReplicateIdentity(map, log);
                    verify(connector, times(0)).buildSlot(connectorContext, false);
                }
            });
        }
    }

    @Nested
    class GetTableInfo {
        GaussDBJdbcContext gaussJdbcContext;
        DataMap dataMap;
        TableInfo tableInfo;
        @Test
        void testNormal() {
            dataMap = mock(DataMap.class);
            tableInfo = mock(TableInfo.class);
            gaussJdbcContext = mock(GaussDBJdbcContext.class);
            connector.gaussJdbcContext = gaussJdbcContext;
            when(gaussJdbcContext.getTableInfo(anyString())).thenReturn(dataMap);
            when(dataMap.getString("size")).thenReturn("1000");
            when(dataMap.getString("rowcount")).thenReturn("10000");
            doNothing().when(tableInfo).setNumOfRows(anyLong());
            doNothing().when(tableInfo).setStorageSize(anyLong());
            when(connector.getTableInfo(connectorContext, "tableName")).thenCallRealMethod();
            try(MockedStatic<TableInfo> ti = mockStatic(TableInfo.class)) {
                ti.when(TableInfo::create).thenReturn(tableInfo);
                TableInfo tableName = connector.getTableInfo(connectorContext, "tableName");
                Assertions.assertNotNull(tableName);
                ti.verify(TableInfo::create, times(1));
            } finally {
                verify(gaussJdbcContext, times(1)).getTableInfo(anyString());
                verify(dataMap, times(1)).getString("size");
                verify(dataMap, times(1)).getString("rowcount");
                verify(tableInfo, times(1)).setNumOfRows(anyLong());
                verify(tableInfo, times(1)).setStorageSize(anyLong());
            }
        }
    }

    @Nested
    class StreamReadTest {
        List<String> tableList;
        Object offsetState;
        StreamReadConsumer consumer;
        GaussDBConfig gaussDBConfig;
        KVReadOnlyMap<TapTable> map;

        List<String> tables;
        GaussDBRunner cdcRunner;
        DataMap nodeConfig;
        String slotName;
        @BeforeEach
        void init() throws Throwable {
            log = mock(io.tapdata.entity.logger.Log.class);
            tableList = mock(List.class);
            offsetState = mock(Object.class);
            consumer = mock(StreamReadConsumer.class);
            gaussDBConfig = mock(GaussDBConfig.class);
            connector.gaussDBConfig = gaussDBConfig;
            map = mock(KVReadOnlyMap.class);

            tables = mock(List.class);
            cdcRunner = mock(GaussDBRunner.class);
            //connector.cdcRunner = cdcRunner;
            nodeConfig = mock(DataMap.class);
            slotName = "slotName";
            connector.slotName = slotName;

            when(connectorContext.getTableMap()).thenReturn(map);
            when(connector.cdcTables(connectorContext, tableList)).thenReturn(tables);
            when(connectorContext.getLog()).thenReturn(log);
            when(cdcRunner.init(any(GaussDBConfig.class), any(io.tapdata.entity.logger.Log.class))).thenReturn(cdcRunner);
            doNothing().when(connector).testReplicateIdentity(map, log);
            doNothing().when(connector).buildSlot(connectorContext, true);
            when(connectorContext.getNodeConfig()).thenReturn(nodeConfig);
            when(cdcRunner.useSlot(anyString())).thenReturn(cdcRunner);
            when(cdcRunner.watch(anyList())).thenReturn(cdcRunner);
            when(cdcRunner.supplierIsAlive(any(Supplier.class))).thenReturn(cdcRunner);
            when(cdcRunner.offset(any(Object.class))).thenReturn(cdcRunner);
            when(cdcRunner.waitTime(anyLong())).thenReturn(cdcRunner);
            doNothing().when(cdcRunner).registerConsumer(any(StreamReadConsumer.class), anyInt());
            doNothing().when(cdcRunner).startCdcRunner();
            doNothing().when(connector).checkThrowable();
            doCallRealMethod().when(connector).streamRead(connectorContext, tableList, offsetState, 100, consumer);
        }
        void assertVerify() throws Throwable {
            try (MockedStatic<GaussDBRunner> gr = mockStatic(GaussDBRunner.class)){
                gr.when(GaussDBRunner::instance).thenReturn(cdcRunner);
                connector.streamRead(connectorContext, tableList, offsetState, 100, consumer);
                //gr.verify(GaussDBRunner::instance, times(1));
                verify(connectorContext, times(1)).getTableMap();
                verify(connector, times(1)).cdcTables(connectorContext, tableList);
                verify(connectorContext, times(1)).getLog();
                verify(cdcRunner, times(1)).init(any(GaussDBConfig.class), any(io.tapdata.entity.logger.Log.class));
                verify(connector, times(1)).testReplicateIdentity(map, log);
                verify(connector, times(1)).buildSlot(connectorContext, true);
                verify(connectorContext, times(1)).getNodeConfig();
                verify(nodeConfig, times(1)).getInteger("flushLsn");
                verify(cdcRunner, times(1)).useSlot(anyString());
                verify(cdcRunner, times(1)).watch(anyList());
                verify(cdcRunner, times(1)).supplierIsAlive(any(Supplier.class));
                verify(cdcRunner, times(1)).offset(any(Object.class));
                verify(cdcRunner, times(1)).waitTime(anyLong());
                verify(cdcRunner, times(1)).registerConsumer(any(StreamReadConsumer.class), anyInt());
                verify(cdcRunner, times(1)).startCdcRunner();
                verify(connector, times(1)).checkThrowable();
            }
        }

        @Test
        void testFlushLsnIsNull() throws Throwable {
            when(nodeConfig.getInteger("flushLsn")).thenReturn(null);
            assertVerify();
        }

        @Test
        void testFlushLsnNotNull() throws Throwable {
            when(nodeConfig.getInteger("flushLsn")).thenReturn(100);
            assertVerify();
        }
    }

    @Nested
    class CheckThrowableTest {
        GaussDBRunner cdcRunner;
        AtomicReference<Throwable> reference;
        Throwable throwable;
        Throwable last;
        ExceptionCollector exceptionCollector;
        @BeforeEach
        void init() throws Throwable {
            reference = mock(AtomicReference.class);
            cdcRunner = mock(GaussDBRunner.class);
            connector.cdcRunner = cdcRunner;
            throwable = mock(Throwable.class);
            last = mock(Throwable.class);
            exceptionCollector = mock(ExceptionCollector.class);

            when(cdcRunner.getThrowable()).thenReturn(reference);
            when(reference.get()).thenReturn(throwable);

            when(connector.getExceptionCollector()).thenReturn(exceptionCollector);
            doNothing().when(exceptionCollector).collectTerminateByServer(any(Throwable.class));
            doNothing().when(exceptionCollector).collectCdcConfigInvalid(any(Throwable.class));
            doNothing().when(exceptionCollector).revealException(any(Throwable.class));

            doCallRealMethod().when(connector).checkThrowable();
        }

        void assertVerify(Throwable l, int getLastCause, int getThrowable, int get,
                          int getExceptionCollector, int collectTerminateByServer,
                          int collectCdcConfigInvalid, int revealException) throws Throwable {
            try(MockedStatic<ErrorKit> ek = mockStatic(ErrorKit.class)) {
                ek.when(() -> ErrorKit.getLastCause(throwable)).thenReturn(l);
                connector.checkThrowable();
                ek.verify(() -> ErrorKit.getLastCause(throwable), times(getLastCause));
            } finally {
                verify(cdcRunner, times(getThrowable)).getThrowable();
                verify(reference, times(get)).get();
                verify(connector, times(getExceptionCollector)).getExceptionCollector();
                verify(exceptionCollector, times(collectTerminateByServer)).collectTerminateByServer(any(Throwable.class));
                verify(exceptionCollector, times(collectCdcConfigInvalid)).collectCdcConfigInvalid(any(Throwable.class));
                verify(exceptionCollector, times(revealException)).revealException(any(Throwable.class));
            }
        }

        @Test
        void testCdcRunnerIsNull() {
            connector.cdcRunner = null;
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(last,0,0,0,
                        0,0,0,0);
            });
        }

        @Test
        void testGetThrowableIsNull() {
            connector.cdcRunner = cdcRunner;
            when(cdcRunner.getThrowable()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(last, 0,1,0,
                        0,0,0,0);
            });
        }

        @Test
        void testThrowableIsNull() {
            connector.cdcRunner = cdcRunner;
            when(reference.get()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(last, 0,1,1,
                        0,0,0,0);
            });
        }

        @Test
        void testLastThrowableIsNull() {
            connector.cdcRunner = cdcRunner;
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(null, 1,1,1,
                        0,0,0,0);
            });
        }

        @Test
        void testLastThrowableIsSQLException() {
            connector.cdcRunner = cdcRunner;
            last = new SQLException("sql-exception");
            Assertions.assertThrows(SQLException.class, () -> {
                assertVerify(last, 1,1,1,
                        1,1,1,1);
            });
        }

        @Test
        void testLastThrowableNotSQLException() {
            connector.cdcRunner = cdcRunner;
            last = mock(Exception.class);
            Assertions.assertThrows(Exception.class, () -> {
                assertVerify(last, 1,1,1,
                        0,0,0,0);
            });
        }
    }

    @Nested
    class GetExceptionCollectorTest {
        @Test
        void testNormal() {
            when(connector.getExceptionCollector()).thenCallRealMethod();
            Assertions.assertNull(connector.getExceptionCollector());
        }
    }

    @Nested
    class CdcTablesTest {
        List<String> tableList;
        DataMap connectionConfig;
        @BeforeEach
        void init() {
            tableList = new ArrayList<>();
            connectionConfig = mock(DataMap.class);
            when(connectorContext.getConnectionConfig()).thenReturn(connectionConfig);
            when(connectionConfig.get("schema")).thenReturn("schema");

            when(connector.cdcTables(connectorContext, null)).thenCallRealMethod();
            when(connector.cdcTables(connectorContext, tableList)).thenCallRealMethod();
        }
        @Test
        void testNullTableList() {
            List<String> list = connector.cdcTables(connectorContext, null);
            Assertions.assertNotNull(list);
            Assertions.assertEquals(0, list.size());
            verify(connectorContext, times(1)).getConnectionConfig();
            verify(connectionConfig, times(1)).get("schema");
        }
        @Test
        void testEmptyTableList() {
            List<String> list = connector.cdcTables(connectorContext, tableList);
            Assertions.assertNotNull(list);
            Assertions.assertEquals(0, list.size());
            verify(connectorContext, times(1)).getConnectionConfig();
            verify(connectionConfig, times(1)).get("schema");
        }
        @Test
        void testNormalTableList() {
            tableList.add("table");
            List<String> list = connector.cdcTables(connectorContext, tableList);
            Assertions.assertNotNull(list);
            Assertions.assertEquals(1, list.size());
            Assertions.assertEquals("schema.table", list.get(0));
            verify(connectorContext, times(1)).getConnectionConfig();
            verify(connectionConfig, times(1)).get("schema");
        }
    }
}
