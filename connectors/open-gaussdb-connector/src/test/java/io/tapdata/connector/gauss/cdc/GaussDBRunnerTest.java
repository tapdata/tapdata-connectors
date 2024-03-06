package io.tapdata.connector.gauss.cdc;

import com.huawei.opengauss.jdbc.jdbc.PgConnection;
import com.huawei.opengauss.jdbc.replication.LogSequenceNumber;
import com.huawei.opengauss.jdbc.replication.PGReplicationConnection;
import com.huawei.opengauss.jdbc.replication.PGReplicationStream;
import com.huawei.opengauss.jdbc.replication.fluent.ChainedStreamBuilder;
import com.huawei.opengauss.jdbc.replication.fluent.logical.ChainedLogicalStreamBuilder;
import io.debezium.embedded.EmbeddedEngine;
import io.tapdata.connector.gauss.cdc.logic.event.EventFactory;
import io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete.LogicReplicationDiscreteImpl;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
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

public class GaussDBRunnerTest {
    GaussDBRunner runner;
    GaussDBConfig config;
    Log log;

    @BeforeEach
    void init() {
        config = mock(GaussDBConfig.class);
        log = mock(Log.class);
        runner = mock(GaussDBRunner.class);
    }

    @Nested
    class InitTest {
        @BeforeEach
        void init() {
            when(runner.init(config, log)).thenCallRealMethod();
            when(runner.init(null, log)).thenCallRealMethod();
            when(config.getHaPort()).thenReturn(8000);
            when(config.getHaHost()).thenReturn("ha.com");
            when(config.getDatabase()).thenReturn("database");
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    GaussDBRunner init = runner.init(config, log);
                    Assertions.assertNotNull(init);
                } finally {
                    verify(config, times(1)).getHaPort();
                    verify(config, times(1)).getHaHost();
                    verify(config, times(1)).getDatabase();
                }
            });
        }

        @Test
        void testNullConfig() {
            Assertions.assertThrows(CoreException.class, () -> {
                try {
                    GaussDBRunner init = runner.init(null, log);
                } finally {
                    verify(config, times(0)).getHaPort();
                    verify(config, times(0)).getHaHost();
                    verify(config, times(0)).getDatabase();
                }
            });
        }
    }

    @Nested
    class IsRunningTest {
        Supplier<Boolean> supplier;
        @BeforeEach
        void init() {
            supplier = mock(Supplier.class);
            when(runner.isRunning()).thenCallRealMethod();
        }

        @Test
        void testNullSupplier() {
            Assertions.assertFalse(runner.isRunning());
        }
        @Test
        void testSupplierGetIsNull() {
            when(supplier.get()).thenReturn(null);
            runner.supplier = supplier;
            Assertions.assertFalse(runner.isRunning());
        }
        @Test
        void testSupplierGetIsTrue() {
            when(supplier.get()).thenReturn(true);
            runner.supplier = supplier;
            Assertions.assertTrue(runner.isRunning());
        }
        @Test
        void testSupplierGetIsFalse() {
            when(supplier.get()).thenReturn(false);
            runner.supplier = supplier;
            Assertions.assertFalse(runner.isRunning());
        }
    }

    @Nested
    class InitPropertiesTest {
        GaussDBConfig gaussDBConfig;
        Properties dbConfigProperties;
        @BeforeEach
        void init() {
            dbConfigProperties = new Properties();
            gaussDBConfig = mock(GaussDBConfig.class);

            when(gaussDBConfig.getProperties()).thenReturn(dbConfigProperties);
            when(gaussDBConfig.getUser()).thenReturn("user");
            when(gaussDBConfig.getPassword()).thenReturn("pwd");
            when(gaussDBConfig.getSchema()).thenReturn("schema");
            when(runner.initProperties()).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> {
                runner.gaussDBConfig = gaussDBConfig;
                dbConfigProperties.put("key", "value");
                Properties properties = runner.initProperties();
                assertVerify(properties);
            });
        }

        @Test
        void testNullGaussDBConfig() {
            Assertions.assertThrows(CoreException.class, () -> {
                Properties properties = runner.initProperties();
                assertVerify(properties);
            });
        }

        @Test
        void testNullDbConfigProperties() {
            Assertions.assertDoesNotThrow(() -> {
                runner.gaussDBConfig = gaussDBConfig;
                when(gaussDBConfig.getProperties()).thenReturn(null);
                Properties properties = runner.initProperties();
                assertVerify(properties);
            });
        }

        @Test
        void testEmptyDbConfigProperties() {
            Assertions.assertDoesNotThrow(() -> {
                runner.gaussDBConfig = gaussDBConfig;
                Properties properties = runner.initProperties();
                assertVerify(properties);
            });
        }
        @Test
        void testSSLClose() {
            Assertions.assertDoesNotThrow(() -> {
                runner.gaussDBConfig = gaussDBConfig;
                dbConfigProperties.put("ssl", "false");
                when(gaussDBConfig.getProperties()).thenReturn(dbConfigProperties);
                Properties properties = runner.initProperties();
                assertVerify(properties);
                Assertions.assertEquals("false", properties.get("ssl"));
            });
        }
        @Test
        void testSSLOpen() {
            Assertions.assertDoesNotThrow(() -> {
                runner.gaussDBConfig = gaussDBConfig;
                dbConfigProperties.put("ssl", "true");
                when(gaussDBConfig.getProperties()).thenReturn(dbConfigProperties);
                Properties properties = runner.initProperties();
                assertVerify(properties);
                Assertions.assertEquals("true", properties.get("ssl"));
            });
        }

        void assertVerify(Properties properties) {
            Assertions.assertNotNull(properties);
            Assertions.assertEquals("user", properties.get("user"));
            Assertions.assertEquals("pwd", properties.get("password"));
            Assertions.assertEquals("database", properties.get("replication"));
            Assertions.assertEquals("9.4", properties.get("assumeMinServerVersion"));
            Assertions.assertEquals("simple", properties.get("preferQueryMode"));
            Assertions.assertEquals("schema", properties.get("currentSchema"));
        }
    }

    @Nested
    class InitCdcConnectionTest {

        @Test
        void testNullJdbcUrl() throws SQLException {
            when(runner.initCdcConnection(null)).thenCallRealMethod();
            runner.jdbcURL = null;
            Assertions.assertThrows(CoreException.class, () -> {
                assertVerify(null, 0);
            });
        }

        @Test
        void testNullJdbcProperties() throws SQLException {
            when(runner.initCdcConnection(null)).thenCallRealMethod();
            runner.jdbcURL = "mock-url";
            Assertions.assertThrows(CoreException.class, () -> {
                assertVerify(null, 0);
            });
        }

        @Test
        void testEmptyJdbcProperties() throws SQLException {
            when(runner.initCdcConnection(any(Properties.class))).thenCallRealMethod();
            runner.jdbcURL = "mock-url";
            Properties p = mock(Properties.class);
            when(p.isEmpty()).thenReturn(true);
            Assertions.assertThrows(CoreException.class, () -> {
                assertVerify(p, 0);
            });
        }

        @Test
        void testNormal() throws SQLException {
            when(runner.initCdcConnection(any(Properties.class))).thenCallRealMethod();
            runner.jdbcURL = "mock-url";
            Properties p = mock(Properties.class);
            when(p.isEmpty()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(p, 1);
            });
        }

        void assertVerify(Properties properties, int times) throws SQLException {
            try (MockedStatic<DriverManager> manager = mockStatic(DriverManager.class)) {
                manager.when(() -> DriverManager.getConnection(runner.jdbcURL, properties)).thenReturn(mock(PgConnection.class));
                runner.initCdcConnection(properties);
                manager.verify(() -> {
                    DriverManager.getConnection(runner.jdbcURL, properties);
                }, times(times));
            }
        }
    }

    @Nested
    class InitChainedLogicalStreamBuilderTest {
        PgConnection conn;
        PGReplicationConnection api;
        ChainedStreamBuilder streamBuilder;
        ChainedLogicalStreamBuilder logical;
        @BeforeEach
        void init() {
            conn = mock(PgConnection.class);
            api = mock(PGReplicationConnection.class);
            when(conn.getReplicationAPI()).thenReturn(api);
            streamBuilder = mock(ChainedStreamBuilder.class);
            when(api.replicationStream()).thenReturn(streamBuilder);
            logical = mock(ChainedLogicalStreamBuilder.class);
            when(streamBuilder.logical()).thenReturn(logical);
            when(logical.withSlotName(anyString())).thenReturn(logical);
            when(logical.withSlotName(null)).thenReturn(logical);
            when(logical.withSlotOption("include-xids", false)).thenReturn(logical);
            when(logical.withSlotOption("skip-empty-xacts", true)).thenReturn(logical);
            when(logical.withSlotOption("parallel-decode-num", 10)).thenReturn(logical);

            when(runner.initChainedLogicalStreamBuilder()).thenCallRealMethod();
        }

        @Test
        void testNullConnection() {
            runner.conn = null;
            Assertions.assertThrows(CoreException.class, () -> {
                runner.initChainedLogicalStreamBuilder();
            });
        }

        @Test
        void testNormal() {
            runner.conn = conn;
            Assertions.assertDoesNotThrow(() -> {
                runner.initChainedLogicalStreamBuilder();
                verify(conn, times(1)).getReplicationAPI();
                verify(api, times(1)).replicationStream();
                verify(streamBuilder, times(1)).logical();
                verify(logical, times(1)).withSlotName(null);
                verify(logical, times(1)).withSlotOption("include-xids", false);
                verify(logical, times(1)).withSlotOption("skip-empty-xacts", true);
                verify(logical, times(1)).withSlotOption("parallel-decode-num", 10);
            });
        }
    }

    @Nested
    class InitCdcOffsetTest {
        ChainedLogicalStreamBuilder streamBuilder;
        CdcOffset offset;
        @BeforeEach
        void init() {
            streamBuilder = mock(ChainedLogicalStreamBuilder.class);
            when(streamBuilder.withStartPosition(any(LogSequenceNumber.class))).thenReturn(null);

            doCallRealMethod().when(runner).initCdcOffset(streamBuilder);
        }

        @Test
        void testNullChainedLogicalStreamBuilder() {
            doCallRealMethod().when(runner).initCdcOffset(null);
            Assertions.assertDoesNotThrow(() -> {
                runner.initCdcOffset(null);
                verify(streamBuilder, times(0)).withStartPosition(any(LogSequenceNumber.class));
            });
        }

        @Test
        void testNullOffset() {
            Assertions.assertDoesNotThrow(() -> {
                runner.initCdcOffset(streamBuilder);
                verify(streamBuilder, times(0)).withStartPosition(any(LogSequenceNumber.class));
            });
        }

        @Test
        void testNullLsnFromOffset() {
            Assertions.assertDoesNotThrow(() -> {
                offset = mock(CdcOffset.class);
                when(offset.getLsn()).thenReturn(null);
                runner.offset = offset;
                runner.initCdcOffset(streamBuilder);
                verify(streamBuilder, times(0)).withStartPosition(any(LogSequenceNumber.class));
            });
        }

        @Test
        void testLsnFromOffsetIsLogSequenceNumber() {
            Assertions.assertDoesNotThrow(() -> {
                Number lsn = mock(Number.class);
                when(lsn.longValue()).thenReturn(1000L);
                offset = mock(CdcOffset.class);
                when(offset.getLsn()).thenReturn(lsn);
                runner.offset = offset;
                LogSequenceNumber logSequenceNumber = mock(LogSequenceNumber.class);
                try (MockedStatic<LogSequenceNumber> sequence = mockStatic(LogSequenceNumber.class)) {
                    sequence.when(() -> LogSequenceNumber.valueOf(anyLong())).thenReturn(logSequenceNumber);
                    runner.initCdcOffset(streamBuilder);
                    verify(lsn, times(1)).longValue();
                    sequence.verify(() -> {
                        LogSequenceNumber.valueOf(anyLong());
                    }, times(1));
                } finally {
                    verify(streamBuilder, times(1)).withStartPosition(any(LogSequenceNumber.class));
                }
            });
        }

        @Test
        void testLsnFromOffsetIsNumber() {
            Assertions.assertDoesNotThrow(() -> {
                LogSequenceNumber lsn = mock(LogSequenceNumber.class);
                offset = mock(CdcOffset.class);
                when(offset.getLsn()).thenReturn(lsn);
                runner.offset = offset;
                runner.initCdcOffset(streamBuilder);
                verify(streamBuilder, times(1)).withStartPosition(any(LogSequenceNumber.class));
            });
        }

        @Test
        void testLsnFromOffsetNotNumberAndNotLogSequenceNumber() {
            Assertions.assertDoesNotThrow(() -> {
                String lsn = "lsn";
                offset = mock(CdcOffset.class);
                when(offset.getLsn()).thenReturn(lsn);
                runner.offset = offset;
                runner.initCdcOffset(streamBuilder);
                verify(streamBuilder, times(0)).withStartPosition(any(LogSequenceNumber.class));
            });
        }
    }

    @Nested
    class InitWhiteTableListTest {
        String whiteTableList;
        Log log;
        ChainedLogicalStreamBuilder streamBuilder;
        @BeforeEach
        void init() {
            whiteTableList = "public.table";
            log = mock(Log.class);
            streamBuilder = mock(ChainedLogicalStreamBuilder.class);
            when(streamBuilder.withSlotOption("white-table-list", whiteTableList)).thenReturn(streamBuilder);
            doNothing().when(log).info("Tables: {} will be monitored in cdc white table list", whiteTableList);
            runner.log = log;
            doCallRealMethod().when(runner).initWhiteTableList(streamBuilder);
            doCallRealMethod().when(runner).initWhiteTableList(null);
        }

        @Test
        void testNullChainedLogicalStreamBuilder() {
            Assertions.assertDoesNotThrow(() -> {
                runner.initWhiteTableList(null);
                verify(streamBuilder, times(0)).withSlotOption("white-table-list", whiteTableList);
                verify(log, times(0)).info("Tables: {} will be monitored in cdc white table list", whiteTableList);
            });
        }

        @Test
        void testNullWhiteTableList() {
            runner.whiteTableList = null;
            Assertions.assertDoesNotThrow(() -> {
                runner.initWhiteTableList(streamBuilder);
                verify(streamBuilder, times(0)).withSlotOption("white-table-list", whiteTableList);
                verify(log, times(0)).info("Tables: {} will be monitored in cdc white table list", whiteTableList);
            });
        }

        @Test
        void testEmptyWhiteTableList() {
            runner.whiteTableList = "";
            Assertions.assertDoesNotThrow(() -> {
                runner.initWhiteTableList(streamBuilder);
                verify(streamBuilder, times(0)).withSlotOption("white-table-list", whiteTableList);
                verify(log, times(0)).info("Tables: {} will be monitored in cdc white table list", whiteTableList);
            });
        }

        @Test
        void testNormal() {
            runner.whiteTableList = whiteTableList;
            Assertions.assertDoesNotThrow(() -> {
                runner.initWhiteTableList(streamBuilder);
                verify(streamBuilder, times(1)).withSlotOption("white-table-list", whiteTableList);
                verify(log, times(1)).info("Tables: {} will be monitored in cdc white table list", whiteTableList);
            });
        }
    }

    @Nested
    class InitPGReplicationStreamTest {
        ChainedLogicalStreamBuilder streamBuilder;
        PGReplicationStream start;
        @BeforeEach
        void init() throws SQLException {
            streamBuilder = mock(ChainedLogicalStreamBuilder.class);
            start = mock(PGReplicationStream.class);

            when(streamBuilder.withSlotOption("standby-connection", true)).thenReturn(streamBuilder);
            when(streamBuilder.withSlotOption("decode-style", "b")).thenReturn(streamBuilder);
            when(streamBuilder.withSlotOption("sending-batch", 1)).thenReturn(streamBuilder);
            when(streamBuilder.withSlotOption("max-txn-in-memory", 100)).thenReturn(streamBuilder);
            when(streamBuilder.withSlotOption("max-reorderbuffer-in-memory", 50)).thenReturn(streamBuilder);
            when(streamBuilder.withSlotOption("include-user", true)).thenReturn(streamBuilder);
            when(streamBuilder.withSlotOption("enable-heartbeat", true)).thenReturn(streamBuilder);
            when(streamBuilder.withSlotOption("include-xids", true)).thenReturn(streamBuilder);
            when(streamBuilder.withSlotOption("include-timestamp", true)).thenReturn(streamBuilder);
            when(streamBuilder.start()).thenReturn(start);
        }

        void assertVerify(int times, boolean isCloud) throws SQLException {
            verify(streamBuilder, times(times)).withSlotOption("standby-connection", true);
            verify(streamBuilder, times(times)).withSlotOption("decode-style", "b");
            verify(streamBuilder, times(times)).withSlotOption("sending-batch", 1);
            verify(streamBuilder, times(times)).withSlotOption("max-txn-in-memory", 100);
            verify(streamBuilder, times(times)).withSlotOption("max-reorderbuffer-in-memory", 50);
            verify(streamBuilder, times(times)).withSlotOption("include-user", true);
            verify(streamBuilder, times(isCloud ? 0 : times)).withSlotOption("enable-heartbeat", true);
            verify(streamBuilder, times(times)).withSlotOption("include-xids", true);
            verify(streamBuilder, times(times)).withSlotOption("include-timestamp", true);
            verify(streamBuilder, times(times)).start();
        }

        @Test
        void testNullChainedLogicalStreamBuilder() throws SQLException {
            when(runner.initPGReplicationStream(null, false)).thenCallRealMethod();
            Assertions.assertThrows(CoreException.class, () -> {
                try {
                    runner.initPGReplicationStream(null, false);
                } finally {
                    assertVerify(0, false);
                }
            });
        }

        @Test
        void testNormal() throws SQLException {
            when(runner.initPGReplicationStream(streamBuilder, false)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> {
                PGReplicationStream stream = runner.initPGReplicationStream(streamBuilder, false);
                Assertions.assertNotNull(stream);
                assertVerify(1, false);
            });
        }


        @Test
        void testIsCloudGaussDBNormal() throws SQLException {
            when(runner.initPGReplicationStream(streamBuilder, true)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> {
                PGReplicationStream stream = runner.initPGReplicationStream(streamBuilder, true);
                Assertions.assertNotNull(stream);
                assertVerify(1, true);
            });
        }
    }

    @Nested
    class InitEventFactoryTest {
        EventFactory<ByteBuffer> factory;
        StreamReadConsumer consumer;
        CdcOffset offset;
        Supplier<Boolean> supplier;
        @BeforeEach
        void init() {
            factory = mock(EventFactory.class);
            consumer = mock(StreamReadConsumer.class);
            offset = mock(CdcOffset.class);
            supplier = mock(Supplier.class);
            when(runner.initEventFactory()).thenCallRealMethod();
        }
        @Test
        void testNormal() {
            runner.consumer = consumer;
            runner.offset = offset;
            runner.supplier = supplier;
            try (MockedStatic<LogicReplicationDiscreteImpl> logic = mockStatic(LogicReplicationDiscreteImpl.class)) {
                logic.when(() -> {
                    LogicReplicationDiscreteImpl.instance(consumer, 100, offset, supplier);
                }).thenReturn(factory);
                EventFactory<ByteBuffer> factory = runner.initEventFactory();
                Assertions.assertNotNull(factory);
                logic.verify(() -> {
                    LogicReplicationDiscreteImpl.instance(consumer, 100, offset, supplier);
                }, times(1));
            }
        }
    }

    @Nested
    class SupplierIsAliveTest {
        Supplier<Boolean> supplier;
        @BeforeEach
        void init() {
            supplier = mock(Supplier.class);
            when(runner.supplierIsAlive(supplier)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            runner.supplierIsAlive(supplier);
            Assertions.assertNotNull(runner.supplier);
            Assertions.assertEquals(supplier, runner.supplier);
        }
    }

    @Nested
    class UseSlotTest {
        @Test
        void testNormal() {
            when(runner.useSlot(anyString())).thenCallRealMethod();
            String slotName = "name";
            GaussDBRunner gaussDBRunner = runner.useSlot(slotName);
            Assertions.assertNotNull(gaussDBRunner);
            Assertions.assertEquals(runner, gaussDBRunner);
        }
    }

    @Nested
    class WaitTimeTest {
        @BeforeEach
        void init() {
            when(runner.waitTime(anyLong())).thenCallRealMethod();
        }

        @Test
        void testLessThanMINValue() {
            long v = 10 * 60 * 1000 - 1;
            GaussDBRunner gaussDBRunner = runner.waitTime(v);
            Assertions.assertNotNull(gaussDBRunner);
            Assertions.assertEquals(runner, gaussDBRunner);
            Assertions.assertEquals(10 * 60 * 1000, runner.waitTime);
        }

        @Test
        void testMoreThanMAXValue() {
            long v = 100 * 60 * 1000 + 1;
            GaussDBRunner gaussDBRunner = runner.waitTime(v);
            Assertions.assertNotNull(gaussDBRunner);
            Assertions.assertEquals(runner, gaussDBRunner);
            Assertions.assertEquals(10 * 60 * 1000, runner.waitTime);
        }

        @Test
        void betweenMinValueAndMaxValue() {
            long v = 100 * 60 * 1000 - 1;
            GaussDBRunner gaussDBRunner = runner.waitTime(v);
            Assertions.assertNotNull(gaussDBRunner);
            Assertions.assertEquals(runner, gaussDBRunner);
            Assertions.assertEquals(v , runner.waitTime);
        }
    }

    @Nested
    class WatchTest {
        @Test
        void testNullObservedTableList() {
            when(runner.watch(null)).thenCallRealMethod();
            GaussDBRunner watch = runner.watch(null);
            Assertions.assertNotNull(watch);
            Assertions.assertEquals(runner, watch);
            Assertions.assertNull(runner.whiteTableList);
        }

        @Test
        void testEmptyObservedTableList() {
            List<String> observedTableList = mock(ArrayList.class);
            when(observedTableList.isEmpty()).thenReturn(true);
            when(runner.watch(observedTableList)).thenCallRealMethod();
            GaussDBRunner watch = runner.watch(observedTableList);
            Assertions.assertNotNull(watch);
            Assertions.assertEquals(runner, watch);
            Assertions.assertNull(runner.whiteTableList);
        }

        @Test
        void testNormalObservedTableList() {
            List<String> observedTableList = new ArrayList<>();
            observedTableList.add("table1");
            observedTableList.add("table2");
            when(runner.watch(observedTableList)).thenCallRealMethod();
            GaussDBRunner watch = runner.watch(observedTableList);
            Assertions.assertNotNull(watch);
            Assertions.assertEquals(runner, watch);
            Assertions.assertNotNull(runner.whiteTableList);
            Assertions.assertEquals("table1,table2", runner.whiteTableList);
        }
    }

    @Nested
    class OffsetTest {
        @BeforeEach
        void init() {
            when(runner.getRunnerName()).thenReturn("name");
        }

        @Test
        void testNullOffset() {
            when(runner.offset(null)).thenCallRealMethod();
            try {
                GaussDBRunner offset = runner.offset(null);
                Assertions.assertNotNull(offset);
                Assertions.assertEquals(runner, offset);
            } finally {
                verify(runner, times(1)).getRunnerName();
                GaussDBRunner.offsetMap.remove(runner.getRunnerName());
            }
        }

        @Test
        void testCdcOffset() {
            CdcOffset offset = mock(CdcOffset.class);
            when(runner.offset(offset)).thenCallRealMethod();
            try {
                GaussDBRunner state = runner.offset(offset);
                Assertions.assertNotNull(state);
                Assertions.assertEquals(runner, state);
            } finally {
                verify(runner, times(1)).getRunnerName();
                GaussDBRunner.offsetMap.remove(runner.getRunnerName());
            }
        }

        @Test
        void testNotCdcOffsetAndNotNull() {
            long offsetState = 100L;
            when(runner.offset(offsetState)).thenCallRealMethod();
            try {
                GaussDBRunner state = runner.offset(offsetState);
                Assertions.assertNotNull(state);
                Assertions.assertEquals(runner, state);
            } finally {
                verify(runner, times(1)).getRunnerName();
                GaussDBRunner.offsetMap.remove(runner.getRunnerName());
            }
        }
    }

    @Nested
    class GetThrowableTest {
        @Test
        void testNormal() {
            when(runner.getThrowable()).thenCallRealMethod();
            AtomicReference<Throwable> throwable = runner.getThrowable();
            Assertions.assertNull(throwable);
        }
    }

    @Nested
    class RunTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(runner).run();
            doNothing().when(runner).startCdcRunner();
            Assertions.assertDoesNotThrow(() -> {
                runner.run();
            });
        }
    }

    @Nested
    class RegisterConsumerTest {
        @Test
        void testNormal() throws SQLException {
            runner.log = mock(Log.class);
            doNothing().when(runner.log).info(anyString());
            StreamReadConsumer consumer = mock(StreamReadConsumer.class);
            int recordSize = 100;
            PgConnection conn = mock(PgConnection.class);
            EventFactory<ByteBuffer> eventFactory = mock(EventFactory.class);
            when(runner.initEventFactory()).thenReturn(eventFactory);
            doCallRealMethod().when(runner).registerConsumer(consumer, recordSize);
            doNothing().when(runner).setPGReplicationStream(false);
            doNothing().when(runner).setPGReplicationStream(true);
            runner.conn = conn;
            runner.registerConsumer(consumer, recordSize);
            verify(runner, times(1)).setPGReplicationStream(false);
            verify(runner, times(0)).setPGReplicationStream(true);
            verify(conn, times(0)).close();
            verify(runner, times(1)).initEventFactory();
            verify(runner.log, times(2)).info(anyString());
        }

        /**
         * not Cloud gaussdb support: enabled-heartbeat=true
         * */
        @Test
        void testNotCloudGaussDB() throws SQLException {
            runner.log = mock(Log.class);
            doNothing().when(runner.log).info(anyString());
            StreamReadConsumer consumer = mock(StreamReadConsumer.class);
            int recordSize = 100;
            PgConnection conn = mock(PgConnection.class);
            EventFactory<ByteBuffer> eventFactory = mock(EventFactory.class);
            when(runner.initEventFactory()).thenReturn(eventFactory);
            doCallRealMethod().when(runner).registerConsumer(consumer, recordSize);
            doNothing().when(runner).setPGReplicationStream(false);
            doNothing().when(runner).setPGReplicationStream(true);
            runner.conn = conn;
            runner.registerConsumer(consumer, recordSize);
            verify(runner, times(1)).setPGReplicationStream(false);
            verify(runner, times(0)).setPGReplicationStream(true);
            verify(conn, times(0)).close();
            verify(runner, times(1)).initEventFactory();
            verify(runner.log, times(2)).info(anyString());
        }

        /**
         * is Cloud gaussdb not support: enabled-heartbeat=true
         * */
        @Test
        void testCloudGaussDB() throws SQLException {
            runner.log = mock(Log.class);
            doNothing().when(runner.log).info(anyString());
            StreamReadConsumer consumer = mock(StreamReadConsumer.class);
            int recordSize = 100;
            PgConnection conn = mock(PgConnection.class);
            EventFactory<ByteBuffer> eventFactory = mock(EventFactory.class);
            when(runner.initEventFactory()).thenReturn(eventFactory);
            doCallRealMethod().when(runner).registerConsumer(consumer, recordSize);
            doAnswer(w -> {
                throw new SQLException("IS Cloud, Not Support enabled-heartbeat");
            }).when(runner).setPGReplicationStream(false);
            doNothing().when(runner).setPGReplicationStream(true);
            runner.conn = conn;
            runner.registerConsumer(consumer, recordSize);
            verify(runner, times(1)).setPGReplicationStream(false);
            verify(runner, times(1)).setPGReplicationStream(true);
            verify(conn, times(1)).close();
            verify(runner, times(1)).initEventFactory();
            verify(runner.log, times(2)).info(anyString());
        }
        /**
         * is Cloud gaussdb not support: enabled-heartbeat=true
         * */
        @Test
        void testCloudGaussDBButConnectionIsNull() throws SQLException {
            runner.log = mock(Log.class);
            doNothing().when(runner.log).info(anyString());
            StreamReadConsumer consumer = mock(StreamReadConsumer.class);
            int recordSize = 100;
            PgConnection conn = mock(PgConnection.class);
            EventFactory<ByteBuffer> eventFactory = mock(EventFactory.class);
            when(runner.initEventFactory()).thenReturn(eventFactory);
            doCallRealMethod().when(runner).registerConsumer(consumer, recordSize);
            doAnswer(w -> {
                throw new SQLException("IS Cloud, Not Support enabled-heartbeat");
            }).when(runner).setPGReplicationStream(false);
            doNothing().when(runner).setPGReplicationStream(true);
            runner.registerConsumer(consumer, recordSize);
            verify(runner, times(1)).setPGReplicationStream(false);
            verify(runner, times(1)).setPGReplicationStream(true);
            verify(conn, times(0)).close();
            verify(runner, times(1)).initEventFactory();
            verify(runner.log, times(2)).info(anyString());
        }
    }

    @Nested
    class SetPGReplicationStreamTest {
        @Test
        void testNormal() throws SQLException {
            runner.log = mock(Log.class);
            doNothing().when(runner.log).info(anyString());

            Properties properties = mock(Properties.class);
            when(runner.initProperties()).thenReturn(properties);

            PgConnection conn = mock(PgConnection.class);
            when(runner.initCdcConnection(properties)).thenReturn(conn);

            ChainedLogicalStreamBuilder streamBuilder = mock(ChainedLogicalStreamBuilder.class);
            when(runner.initChainedLogicalStreamBuilder()).thenReturn(streamBuilder);

            doNothing().when(runner).initCdcOffset(streamBuilder);

            PGReplicationStream stream = mock(PGReplicationStream.class);
            when(runner.initPGReplicationStream(streamBuilder, true)).thenReturn(stream);

            doCallRealMethod().when(runner).setPGReplicationStream(anyBoolean());

            runner.setPGReplicationStream(true);

            verify(runner, times(1)).initProperties();
            verify(runner, times(1)).initCdcConnection(properties);
            verify(runner, times(1)).initChainedLogicalStreamBuilder();
            verify(runner, times(1)).initCdcOffset(streamBuilder);
            verify(runner, times(1)).initWhiteTableList(streamBuilder);
            verify(runner, times(1)).initPGReplicationStream(streamBuilder, true);
            verify(runner.log, times(1)).info(anyString());
        }
    }

    @Nested
    class FlushLsnTest {
        PGReplicationStream stream;
        Log log;
        long waitTime;
        LogSequenceNumber lastRecv;

        @BeforeEach
        void init() throws SQLException {
            waitTime = 100;
            runner.waitTime = waitTime;

            lastRecv = mock(LogSequenceNumber.class);
            stream = mock(PGReplicationStream.class);
            runner.stream = stream;

            when(lastRecv.asString()).thenReturn("lsn");

            when(stream.getLastReceiveLSN()).thenReturn(lastRecv);
            doNothing().when(stream).setFlushedLSN(lastRecv);
            doNothing().when(stream).forceUpdateStatus();

            log = mock(Log.class);
            runner.log = log;
            doNothing().when(log).debug(anyString(), anyString(), anyLong());

            when(runner.flushLsn(anyLong())).thenCallRealMethod();
        }

        @Test
        void testValueTimeLessThanWaitTime() {
            waitTime = Long.MAX_VALUE;
            runner.waitTime = waitTime;
            Assertions.assertDoesNotThrow(() -> {
                long flushLsn = 0;
                try {
                    flushLsn = runner.flushLsn(0);
                } finally {
                    Assertions.assertEquals(0, flushLsn);
                    verify(stream, times(0)).getLastReceiveLSN();
                    verify(stream, times(0)).setFlushedLSN(lastRecv);
                    verify(stream, times(0)).forceUpdateStatus();
                    verify(log, times(0)).debug(anyString(), anyString(), anyLong());
                }
            });
        }
        @Test
        void testValueTimeMoreThanWaitTime() {
            waitTime = Long.MIN_VALUE;
            runner.waitTime = waitTime;
            Assertions.assertDoesNotThrow(() -> {
                try {
                    long flushLsn = runner.flushLsn(0);
                } finally {
                    verify(stream, times(1)).getLastReceiveLSN();
                    verify(stream, times(1)).setFlushedLSN(lastRecv);
                    verify(stream, times(1)).forceUpdateStatus();
                    verify(log, times(1)).debug(anyString(), anyString(), anyLong());
                }
            });
        }
        @Test
        void testSqlExceptionFromForceUpdateStatus() throws SQLException {
            doAnswer(w -> {
                throw new SQLException("failed");
            }).when(stream).forceUpdateStatus();
            waitTime = Long.MIN_VALUE;
            runner.waitTime = waitTime;
            Assertions.assertThrows(SQLException.class, () -> {
                try {
                    long flushLsn = runner.flushLsn(0);
                } finally {
                    verify(stream, times(1)).getLastReceiveLSN();
                    verify(stream, times(1)).setFlushedLSN(lastRecv);
                    verify(stream, times(1)).forceUpdateStatus();
                    verify(log, times(0)).debug(anyString(), anyString(), anyLong());
                }
            });
        }
    }

    @Nested
    class VerifyByteBufferTest {
        ByteBuffer byteBuffer;
        @BeforeEach
        void init() {
            byteBuffer = mock(ByteBuffer.class);
            when(runner.verifyByteBuffer(null)).thenCallRealMethod();
            when(runner.verifyByteBuffer(byteBuffer)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            boolean bf = runner.verifyByteBuffer(byteBuffer);
            Assertions.assertTrue(bf);
        }

        @Test
        void testNullByteBuffer() throws InterruptedException {
            doNothing().when(runner).sleep(anyLong());
            Assertions.assertDoesNotThrow(() -> {
                boolean bf = false;
                try {
                    bf = runner.verifyByteBuffer(null);
                } finally {
                    Assertions.assertFalse(bf);
                    verify(runner, times(1)).sleep(anyLong());
                }
            });
        }

        @Test
        void testException() throws InterruptedException {
            doAnswer(w -> {
                throw new InterruptedException("");
            }).when(runner).sleep(anyLong());
            Assertions.assertDoesNotThrow(() -> {
                boolean bf = false;
                try {
                    bf = runner.verifyByteBuffer(null);
                } finally {
                    Assertions.assertFalse(bf);
                    verify(runner, times(1)).sleep(anyLong());
                }
            });
        }
    }

    @Nested
    class SleepTest {
        @Test
        void testSleep () throws InterruptedException {
            doCallRealMethod().when(runner).sleep(anyLong());
            Assertions.assertDoesNotThrow(() -> {
                runner.sleep(0L);
            });
        }
    }

    @Nested
    class StartCdcRunnerTest {
        StreamReadConsumer consumer;
        Supplier<Boolean> supplier;
        PGReplicationStream stream;
        EventFactory<ByteBuffer> eventFactory;
        Log log;

        AtomicReference<Throwable> atomicReference;
        ByteBuffer byteBuffer;

        @BeforeEach
        void init() throws SQLException {
            atomicReference = mock(AtomicReference.class);
            when(runner.getThrowable()).thenReturn(atomicReference);
            doNothing().when(atomicReference).set(any(Throwable.class));
            byteBuffer = mock(ByteBuffer.class);
            consumer = mock(StreamReadConsumer.class);
            supplier = mock(Supplier.class);
            stream = mock(PGReplicationStream.class);
            eventFactory = mock(EventFactory.class);
            log = mock(Log.class);

            doNothing().when(consumer).streamReadStarted();
            when(supplier.get()).thenReturn(true);
            when(stream.readPending()).thenReturn(byteBuffer);
            when(runner.verifyByteBuffer(byteBuffer)).thenReturn(true);
            doNothing().when(eventFactory).emit(byteBuffer, log);
            when(runner.flushLsn(anyLong())).thenReturn(0L);
            doNothing().when(log).warn(anyString(), anyString());
            doNothing().when(consumer).streamReadEnded();
            doNothing().when(runner).closeCdcRunner();

            doCallRealMethod().when(runner).startCdcRunner();
        }

        @Test
        void testNullConsumer() {
            runner.consumer = null;
            runner.supplier = supplier;
            runner.stream = stream;
            runner.log = log;
            runner.eventFactory = eventFactory;
            Assertions.assertThrows(CoreException.class, () -> {
                try {
                    runner.startCdcRunner();
                } finally {
                    verify(consumer, times(0)).streamReadStarted();
                    verify(supplier, times(0)).get();
                    verify(stream, times(0)).readPending();
                    verify(runner, times(0)).verifyByteBuffer(byteBuffer);
                    verify(eventFactory, times(0)).emit(byteBuffer, log);
                    verify(runner, times(0)).flushLsn(anyLong());
                    verify(log, times(0)).warn(anyString(), anyString());
                    verify(runner, times(0)).getThrowable();
                    verify(atomicReference, times(0)).set(any(Throwable.class));
                    verify(consumer, times(0)).streamReadEnded();
                    verify(runner, times(0)).closeCdcRunner();
                }
            });
        }

        @Test
        void testNullSupplier() {
            runner.consumer = consumer;
            runner.supplier = null;
            runner.stream = stream;
            runner.log = log;
            runner.eventFactory = eventFactory;
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.startCdcRunner();
                } finally {
                    verify(consumer, times(1)).streamReadStarted();
                    verify(supplier, times(0)).get();
                    verify(stream, times(0)).readPending();
                    verify(runner, times(0)).verifyByteBuffer(byteBuffer);
                    verify(eventFactory, times(0)).emit(byteBuffer, log);
                    verify(runner, times(0)).flushLsn(anyLong());
                    verify(log, times(0)).warn(anyString(), anyString());
                    verify(runner, times(0)).getThrowable();
                    verify(atomicReference, times(0)).set(any(Throwable.class));
                    verify(consumer, times(1)).streamReadEnded();
                    verify(runner, times(1)).closeCdcRunner();
                }
            });
        }

        @Test
        void testSupplierIsFalse() throws SQLException {
            runner.consumer = consumer;
            runner.supplier = supplier;
            runner.stream = stream;
            runner.log = log;
            runner.eventFactory = eventFactory;
            when(supplier.get()).thenReturn(false);
            when(stream.readPending()).thenAnswer(w -> {
                when(supplier.get()).thenReturn(false);
                return byteBuffer;
            });
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.startCdcRunner();
                } finally {
                    verify(consumer, times(1)).streamReadStarted();
                    verify(supplier, times(1)).get();
                    verify(stream, times(0)).readPending();
                    verify(runner, times(0)).verifyByteBuffer(byteBuffer);
                    verify(eventFactory, times(0)).emit(byteBuffer, log);
                    verify(runner, times(0)).flushLsn(anyLong());
                    verify(log, times(0)).warn(anyString(), anyString());
                    verify(runner, times(0)).getThrowable();
                    verify(atomicReference, times(0)).set(any(Throwable.class));
                    verify(consumer, times(1)).streamReadEnded();
                    verify(runner, times(1)).closeCdcRunner();
                }
            });
        }

        @Test
        void testVerifyByteBufferNotSucceed() throws SQLException {
            runner.consumer = consumer;
            runner.supplier = supplier;
            runner.stream = stream;
            runner.log = log;
            runner.eventFactory = eventFactory;
            when(runner.verifyByteBuffer(byteBuffer)).thenReturn(false);
            when(stream.readPending()).thenAnswer(w -> {
                when(supplier.get()).thenReturn(false);
                return byteBuffer;
            });
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.startCdcRunner();
                } finally {
                    verify(consumer, times(1)).streamReadStarted();
                    verify(supplier, times(2)).get();
                    verify(stream, times(1)).readPending();
                    verify(runner, times(1)).verifyByteBuffer(byteBuffer);
                    verify(eventFactory, times(0)).emit(byteBuffer, log);
                    verify(runner, times(0)).flushLsn(anyLong());
                    verify(log, times(0)).warn(anyString(), anyString());
                    verify(runner, times(0)).getThrowable();
                    verify(atomicReference, times(0)).set(any(Throwable.class));
                    verify(consumer, times(1)).streamReadEnded();
                    verify(runner, times(1)).closeCdcRunner();
                }
            });
        }

        @Test
        void testException() throws SQLException {
            runner.consumer = consumer;
            runner.supplier = supplier;
            runner.stream = stream;
            runner.log = log;
            runner.eventFactory = eventFactory;
            when(runner.verifyByteBuffer(byteBuffer)).thenReturn(false);
            when(stream.readPending()).thenAnswer(w -> {
                throw new SQLException("Failed");
            });
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.startCdcRunner();
                } finally {
                    verify(consumer, times(1)).streamReadStarted();
                    verify(supplier, times(1)).get();
                    verify(stream, times(1)).readPending();
                    verify(runner, times(0)).verifyByteBuffer(byteBuffer);
                    verify(eventFactory, times(0)).emit(byteBuffer, log);
                    verify(runner, times(0)).flushLsn(anyLong());
                    verify(log, times(1)).warn(anyString(), anyString());
                    verify(runner, times(1)).getThrowable();
                    verify(atomicReference, times(1)).set(any(Throwable.class));
                    verify(consumer, times(1)).streamReadEnded();
                    verify(runner, times(1)).closeCdcRunner();
                }
            });
        }

        @Test
        void testNormal() throws SQLException {
            runner.consumer = consumer;
            runner.supplier = supplier;
            runner.stream = stream;
            runner.log = log;
            runner.eventFactory = eventFactory;
            when(runner.verifyByteBuffer(byteBuffer)).thenReturn(true);
            when(stream.readPending()).thenAnswer(w -> {
                when(supplier.get()).thenReturn(false);
                return byteBuffer;
            });
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.startCdcRunner();
                } finally {
                    verify(consumer, times(1)).streamReadStarted();
                    verify(supplier, times(2)).get();
                    verify(stream, times(1)).readPending();
                    verify(runner, times(1)).verifyByteBuffer(byteBuffer);
                    verify(eventFactory, times(1)).emit(byteBuffer, log);
                    verify(runner, times(1)).flushLsn(anyLong());
                    verify(log, times(0)).warn(anyString(), anyString());
                    verify(runner, times(0)).getThrowable();
                    verify(atomicReference, times(0)).set(any(Throwable.class));
                    verify(consumer, times(1)).streamReadEnded();
                    verify(runner, times(1)).closeCdcRunner();
                }
            });
        }
    }

    @Nested
    class CloseCdcRunnerTest {
        PGReplicationStream stream;
        PgConnection conn;
        EmbeddedEngine engine;
        Log log;
        @BeforeEach
        void init() throws SQLException, IOException {
            stream = mock(PGReplicationStream.class);
            doNothing().when(stream).close();
            conn = mock(PgConnection.class);
            doNothing().when(conn).close();
            engine = mock(EmbeddedEngine.class);
            doNothing().when(engine).close();
            log = mock(Log.class);
            doNothing().when(log).warn(anyString(), anyString());
            when(runner.getEngine()).thenReturn(engine);
            when(runner.getRunnerName()).thenReturn("name");
            doCallRealMethod().when(runner).closeCdcRunner();

            runner.stream = stream;
            runner.conn = conn;
            runner.log = log;
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.closeCdcRunner();
                } finally {
                    verify(stream, times(1)).close();
                    verify(conn, times(1)).close();
                    verify(engine, times(1)).close();
                    verify(log, times(0)).warn(anyString(), anyString());
                    verify(runner, times(1)).getEngine();
                    verify(runner, times(1)).getRunnerName();
                }
            });
        }

        @Test
        void testExceptionStream() throws SQLException {
            doAnswer(w -> {
                throw new SQLException("");
            }).when(stream).close();
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.closeCdcRunner();
                } finally {
                    verify(stream, times(1)).close();
                    verify(conn, times(1)).close();
                    verify(engine, times(1)).close();
                    verify(log, times(1)).warn(anyString(), anyString());
                    verify(runner, times(1)).getEngine();
                    verify(runner, times(1)).getRunnerName();
                }
            });
        }
        @Test
        void testNullStream() {
            runner.stream = null;
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.closeCdcRunner();
                } finally {
                    verify(stream, times(0)).close();
                    verify(conn, times(1)).close();
                    verify(engine, times(1)).close();
                    verify(log, times(0)).warn(anyString(), anyString());
                    verify(runner, times(1)).getEngine();
                    verify(runner, times(1)).getRunnerName();
                }
            });
        }

        @Test
        void testExceptionConn() throws SQLException {
            doAnswer(w -> {
                throw new SQLException("");
            }).when(conn).close();
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.closeCdcRunner();
                } finally {
                    verify(stream, times(1)).close();
                    verify(conn, times(1)).close();
                    verify(engine, times(1)).close();
                    verify(log, times(1)).warn(anyString(), anyString());
                    verify(runner, times(1)).getEngine();
                    verify(runner, times(1)).getRunnerName();
                }
            });
        }

        @Test
        void testNullConn() {
            runner.conn = null;
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.closeCdcRunner();
                } finally {
                    verify(stream, times(1)).close();
                    verify(conn, times(0)).close();
                    verify(engine, times(1)).close();
                    verify(log, times(0)).warn(anyString(), anyString());
                    verify(runner, times(1)).getEngine();
                    verify(runner, times(1)).getRunnerName();
                }
            });
        }

        @Test
        void testExceptionEngine() throws IOException {
            doAnswer(w -> {
                throw new IOException("");
            }).when(engine).close();
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.closeCdcRunner();
                } finally {
                    verify(stream, times(1)).close();
                    verify(conn, times(1)).close();
                    verify(engine, times(1)).close();
                    verify(log, times(1)).warn(anyString(), anyString());
                    verify(runner, times(1)).getEngine();
                    verify(runner, times(1)).getRunnerName();
                }
            });
        }

        @Test
        void testNullEngine() {
            when(runner.getEngine()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    runner.closeCdcRunner();
                } finally {
                    verify(stream, times(1)).close();
                    verify(conn, times(1)).close();
                    verify(engine, times(0)).close();
                    verify(log, times(0)).warn(anyString(), anyString());
                    verify(runner, times(1)).getEngine();
                    verify(runner, times(1)).getRunnerName();
                }
            });
        }
    }

    @Nested
    class GetEngineTest {
        @Test
        void testGetEngine() {
            doCallRealMethod().when(runner).getEngine();
            Assertions.assertNull(runner.getEngine());
        }
    }
}
