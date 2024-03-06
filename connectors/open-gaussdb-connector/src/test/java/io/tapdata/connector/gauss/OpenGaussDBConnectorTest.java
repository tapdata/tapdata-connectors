package io.tapdata.connector.gauss;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.gauss.cdc.GaussDBRunner;
import io.tapdata.connector.gauss.core.GaussColumn;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.gauss.core.GaussDBJdbcContext;
import io.tapdata.connector.gauss.enums.CdcConstant;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
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
        void init() {
            dataMap = mock(DataMap.class);
            gaussDBConfig = mock(GaussDBConfig.class);

            when(connectionContext.getConnectionConfig()).thenReturn(dataMap);
            when(gaussDBConfig.load(dataMap)).thenReturn(gaussDBConfig);
            when(gaussDBConfig.getConnectionString()).thenReturn("gaussDB-connection-str");
            connectionOptions = mock(ConnectionOptions.class);
            when(connectionOptions.connectionString(anyString())).thenReturn(connectionOptions);

            consumer = mock(Consumer.class);
            gaussDBTest = mock(GaussDBTest.class);
            when(gaussDBTest.initContext()).thenReturn(gaussDBTest);
            when(gaussDBTest.testOneByOne()).thenReturn(true);
            doNothing().when(gaussDBTest).close();

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
                        GaussDBTest.instance(gaussDBConfig, consumer);
                    }).thenReturn(gaussDBTest);
                    ConnectionOptions cop = connector.connectionTest(connectionContext, consumer);
                    Assertions.assertEquals(connectionOptions, cop);
                } finally {
                    verify(gaussDBConfig, times(1)).getConnectionString();
                    verify(gaussDBConfig, times(1)).load(dataMap);
                    verify(connectionContext, times(1)).getConnectionConfig();
                    verify(connectionOptions, times(1)).connectionString(anyString());
                    verify(gaussDBTest, times(1)).initContext();
                    verify(gaussDBTest, times(1)).testOneByOne();
                    verify(gaussDBTest, times(1)).close();
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
        void init() {
            tableMap = mock(KVReadOnlyMap.class);
            gaussDBConfig = mock(GaussDBConfig.class);
            when(gaussDBConfig.getSchema()).thenReturn("schema");

            gaussJdbcContext = mock(GaussDBJdbcContext.class);

            doNothing().when(connector).iteratorTableMap(any(KVReadOnlyMap.class), anyList(), anyList());
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
            doNothing().when(gaussJdbcContext).query(anyString(), any(ResultSetConsumer.class));
            doCallRealMethod().when(connector).testReplicateIdentity(tableMap, log);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    connector.testReplicateIdentity(tableMap, log);
                } finally {
                    verify(connector, times(1)).iteratorTableMap(any(KVReadOnlyMap.class), anyList(), anyList());
                    verify(gaussDBConfig, times(1)).getSchema();
                    verify(gaussJdbcContext, times(1)).query(anyString(), any(ResultSetConsumer.class));
                    verify(log, times(0)).debug(anyString(), anyString());
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
            connector.gaussDBTest = gaussDBTest;
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
            connector.gaussDBTest = gaussDBTest;
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
            connector.gaussDBTest = gaussDBTest;
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
    class InitConnectionTest {

    }
}
