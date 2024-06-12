package io.tapdata.connector.tidb;

import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.tidb.cdc.process.thread.ProcessHandler;
import io.tapdata.connector.tidb.cdc.process.thread.TiCDCShellManager;
import io.tapdata.connector.tidb.cdc.util.ProcessLauncher;
import io.tapdata.connector.tidb.cdc.util.ProcessSearch;
import io.tapdata.connector.tidb.cdc.util.ZipUtils;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.connector.tidb.dml.TidbReader;
import io.tapdata.connector.tidb.util.HttpUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TidbConnectorTest {

    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        TidbConnector tidbConnector = new TidbConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(tidbConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }
//    @Test
//    void testCreate() throws IOException {
//        HttpUtil httpUtil = HttpUtil.of(new TapLog());
//        ChangeFeed changefeed = new ChangeFeed();
//        String changeFeedId = UUID.randomUUID().toString().replaceAll("-", "");
//        if (Pattern.matches("^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$", changeFeedId)) {
//            changefeed.setSinkUri("file:///tidbCDC888?protocol=canal-json");
//            changefeed.setChangefeedId("simple-replication-task");
//            changefeed.setForceReplicate(true);
//            changefeed.setSyncDdl(true);
//            JSONObject jsonObject = new JSONObject();
//            List<String> tableList = new ArrayList();
//            tableList.add("*");
//            List rules = new ArrayList();
//            for(String table:tableList) {
//                String rule = "test"+"."+table;
//                rules.add(rule);
//            }
//            jsonObject.put("rules", rules.toArray());
//            ReplicaConfig replicaConfig = new ReplicaConfig();
//            replicaConfig.setFilter(jsonObject);
//            Sink sink = new Sink();
//            sink.setDateSeparator("none");
//            sink.setProtocol("canal-json");
//            replicaConfig.setSink(sink);
//            changefeed.setReplicaConfig(replicaConfig);
//            httpUtil.createChangeFeed(changefeed, "123.60.132.254:8300");
//        }
//    }

//    @Test
//    void testDelete() throws IOException {
//        HttpUtil   httpUtil = HttpUtil.of(new TapLog());
//        httpUtil.deleteChangeFeed("simple-replication-task9", "127.0.0.1:8300");
//    }

//    @Test
//    void queryChangefeedlist() throws IOException {
//        HttpUtil httpUtil = HttpUtil.of(new TapLog());
//        httpUtil.queryChangeFeedsList("123.60.132.254:8300");
//    }

    @Nested
    class TestCase {
        Log log;
        TidbConnector connector;
        TapConnectorContext nodeContext;
        TapConnectionContext tapConnectionContext;
        TidbConfig tidbConfig;
        TidbJdbcContext tidbJdbcContext;
        TimeZone timezone;
        TidbReader tidbReader;
        AtomicBoolean started;
        @BeforeEach
        void init() {
            started = new AtomicBoolean(false);
            log = mock(Log.class);
            tidbConfig = new TidbConfig();
            tidbJdbcContext = mock(TidbJdbcContext.class);
            timezone = TimeZone.getDefault();
            tidbReader = mock(TidbReader.class);
            connector = mock(TidbConnector.class);
            nodeContext = mock(TapConnectorContext.class);
            tapConnectionContext = mock(TapConnectionContext.class);
            when(nodeContext.getLog()).thenReturn(log);
            when(tapConnectionContext.getLog()).thenReturn(log);

            ReflectionTestUtils.setField(connector, "started", started);
            ReflectionTestUtils.setField(connector, "tidbConfig", tidbConfig);
            ReflectionTestUtils.setField(connector, "tidbJdbcContext", tidbJdbcContext);
            ReflectionTestUtils.setField(connector, "timezone", timezone);
            ReflectionTestUtils.setField(connector, "tidbReader", tidbReader);
        }

        @Nested
        class ErrorHandleTest {
            PDKMethod pdkMethod;
            @BeforeEach
            void init() {
                pdkMethod = mock(PDKMethod.class);
            }

            @Test
            void testNormal() {
                Exception exception = new Exception("");
                when(connector.errorHandle(tapConnectionContext, pdkMethod,exception)).thenCallRealMethod();
                RetryOptions retryOptions = connector.errorHandle(tapConnectionContext, pdkMethod, exception);
                Assertions.assertNotNull(retryOptions);
                Assertions.assertTrue(retryOptions.isNeedRetry());
            }

            @Test
            void testCDC_TOOL_NOT_EXISTS() {
                CoreException e = new CoreException(TiCDCShellManager.CDC_TOOL_NOT_EXISTS, "");
                when(connector.errorHandle(tapConnectionContext, pdkMethod,e)).thenCallRealMethod();
                RetryOptions retryOptions = connector.errorHandle(tapConnectionContext, pdkMethod, e);
                Assertions.assertNotNull(retryOptions);
                Assertions.assertFalse(retryOptions.isNeedRetry());
            }

            @Test
            void testERROR_START_TS_BEFORE_GC() {
                CoreException e = new CoreException(HttpUtil.ERROR_START_TS_BEFORE_GC, "");
                when(connector.errorHandle(tapConnectionContext, pdkMethod,e)).thenCallRealMethod();
                RetryOptions retryOptions = connector.errorHandle(tapConnectionContext, pdkMethod, e);
                Assertions.assertNotNull(retryOptions);
                Assertions.assertFalse(retryOptions.isNeedRetry());
            }
        }

        @Nested
        class StreamReadTest {
            List<String> tableList;
            Object offsetState;
            int recordSize;
            StreamReadConsumer consumer;
            KVMap<Object> kvMap;
            ProcessHandler handler;

            @BeforeEach
            void init() throws Throwable {
                handler = mock(ProcessHandler.class);
                tableList = mock(List.class);
                offsetState = 0L;
                recordSize =1;
                consumer = mock(StreamReadConsumer.class);
                kvMap = mock(KVMap.class);
                doNothing().when(handler).doActivity();
                doNothing().when(handler).close();
                doNothing().when(connector).doWait(handler);

                when(connector.genericFeedId(kvMap)).thenReturn("feed-id");
                when(nodeContext.getStateMap()).thenReturn(kvMap);
                when(kvMap.get(ProcessHandler.CDC_SERVER)).thenReturn("");

                doCallRealMethod().when(connector).streamRead(nodeContext, tableList, offsetState, recordSize, consumer);
            }
            @Test
            void testNormal() throws Exception {
                try(MockedStatic<ProcessHandler> ph = mockStatic(ProcessHandler.class)) {
                    ph.when(() -> ProcessHandler.of(any(ProcessHandler.ProcessInfo.class), any(StreamReadConsumer.class))).thenReturn(handler);
                    Assertions.assertDoesNotThrow(() -> connector.streamRead(nodeContext, tableList, offsetState, recordSize, consumer));
                    verify(connector).genericFeedId(kvMap);
                    verify(kvMap).get(ProcessHandler.CDC_SERVER);
                    verify(nodeContext, times(2)).getStateMap();
                    verify(handler).doActivity();
                    verify(handler).close();
                    verify(connector).doWait(handler);
                }
            }
        }

        @Nested
        class GenericFeedIdTest {
            KVMap<Object> kvMap;
            @BeforeEach
            void init() {
                kvMap = mock(KVMap.class);
                when(connector.genericFeedId(kvMap)).thenCallRealMethod();
                doNothing().when(kvMap).put(anyString(), anyString());
            }

            @Test
            void testNormal() {
                when(kvMap.get(ProcessHandler.FEED_ID)).thenReturn("id");
                Assertions.assertEquals("id", connector.genericFeedId(kvMap));
                verify(kvMap).get(ProcessHandler.FEED_ID);
                verify(kvMap, times(0)).put(anyString(), anyString());
            }

            @Test
            void testNot() {
                when(kvMap.get(ProcessHandler.FEED_ID)).thenReturn(null);
                String s = connector.genericFeedId(kvMap);
                Assertions.assertNotNull(s);
                Assertions.assertNotEquals("id", s);
                verify(kvMap).get(ProcessHandler.FEED_ID);
                verify(kvMap, times(1)).put(anyString(), anyString());
            }
        }

        @Nested
        class DoWaitTest {

        }

        @Nested
        class CheckTiServerAndStopTest {
            HttpUtil httpUtil;
            String cdcServer;
            DataMap config;
            List<String> processes;
            @BeforeEach
            void init() throws IOException {
                processes = new ArrayList<>();
                processes.add("100");
                config = mock(DataMap.class);
                httpUtil = mock(HttpUtil.class);
                cdcServer = "http://127.0.0.1:2000";
                when(httpUtil.checkAlive(cdcServer)).thenReturn(true);
                when(httpUtil.queryChangeFeedsList(cdcServer)).thenReturn(0);
                when(nodeContext.getConnectionConfig()).thenReturn(config);
                when(config.getString("pdServer")).thenReturn("http://127.0.0.1:3000");
                doNothing().when(log).debug("Cdc server is alive, will check change feed list");
                doNothing().when(log).debug("There is not any change feed with cdc server: {}, will stop cdc server", cdcServer);

                doCallRealMethod().when(connector).checkTiServerAndStop(httpUtil, cdcServer, nodeContext);
            }
            @Test
            void testNormal() throws IOException {
                try(MockedStatic<ProcessSearch> ps = mockStatic(ProcessSearch.class);
                    MockedStatic<ProcessLauncher> pl = mockStatic(ProcessLauncher.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    ps.when(() -> ProcessSearch.getProcesses(any(Log.class), anyString())).thenReturn(processes);
                    pl.when(() -> ProcessLauncher.execCmdWaitResult(anyString(), anyString(), any(Log.class))).thenReturn("");
                    zu.when(() -> ZipUtils.deleteFile(anyString(), any(Log.class))).thenAnswer(a -> null);
                    Assertions.assertDoesNotThrow(() -> connector.checkTiServerAndStop(httpUtil, cdcServer, nodeContext));
                    verify(nodeContext).getLog();
                    verify(httpUtil).checkAlive(cdcServer);
                    verify(log).debug("Cdc server is alive, will check change feed list");
                    verify(httpUtil).queryChangeFeedsList(cdcServer);
                    verify(log).debug("There is not any change feed with cdc server: {}, will stop cdc server", cdcServer);
                    verify(nodeContext).getConnectionConfig();
                    verify(config).getString("pdServer");
                }
            }
            @Test
            void testNotAlive() throws IOException {
                when(httpUtil.checkAlive(cdcServer)).thenReturn(false);
                try(MockedStatic<ProcessSearch> ps = mockStatic(ProcessSearch.class);
                    MockedStatic<ProcessLauncher> pl = mockStatic(ProcessLauncher.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    ps.when(() -> ProcessSearch.getProcesses(any(Log.class), anyString())).thenReturn(processes);
                    pl.when(() -> ProcessLauncher.execCmdWaitResult(anyString(), anyString(), any(Log.class))).thenReturn("");
                    zu.when(() -> ZipUtils.deleteFile(anyString(), any(Log.class))).thenAnswer(a -> null);
                    Assertions.assertDoesNotThrow(() -> connector.checkTiServerAndStop(httpUtil, cdcServer, nodeContext));
                    verify(nodeContext).getLog();
                    verify(httpUtil).checkAlive(cdcServer);
                    verify(log, times(0)).debug("Cdc server is alive, will check change feed list");
                    verify(httpUtil, times(0)).queryChangeFeedsList(cdcServer);
                    verify(log, times(0)).debug("There is not any change feed with cdc server: {}, will stop cdc server", cdcServer);
                    verify(nodeContext, times(0)).getConnectionConfig();
                    verify(config, times(0)).getString("pdServer");
                }
            }
            @Test
            void testHasFeed() throws IOException {
                when(httpUtil.queryChangeFeedsList(cdcServer)).thenReturn(1);
                try(MockedStatic<ProcessSearch> ps = mockStatic(ProcessSearch.class);
                    MockedStatic<ProcessLauncher> pl = mockStatic(ProcessLauncher.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    ps.when(() -> ProcessSearch.getProcesses(any(Log.class), anyString())).thenReturn(processes);
                    pl.when(() -> ProcessLauncher.execCmdWaitResult(anyString(), anyString(), any(Log.class))).thenReturn("");
                    zu.when(() -> ZipUtils.deleteFile(anyString(), any(Log.class))).thenAnswer(a -> null);
                    Assertions.assertDoesNotThrow(() -> connector.checkTiServerAndStop(httpUtil, cdcServer, nodeContext));
                    verify(nodeContext).getLog();
                    verify(httpUtil).checkAlive(cdcServer);
                    verify(log, times(1)).debug("Cdc server is alive, will check change feed list");
                    verify(httpUtil, times(1)).queryChangeFeedsList(cdcServer);
                    verify(log, times(0)).debug("There is not any change feed with cdc server: {}, will stop cdc server", cdcServer);
                    verify(nodeContext, times(0)).getConnectionConfig();
                    verify(config, times(0)).getString("pdServer");
                }
            }
            @Test
            void testNotAnyProcess() throws IOException {
                processes.clear();
                try(MockedStatic<ProcessSearch> ps = mockStatic(ProcessSearch.class);
                    MockedStatic<ProcessLauncher> pl = mockStatic(ProcessLauncher.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    ps.when(() -> ProcessSearch.getProcesses(any(Log.class), anyString())).thenReturn(processes);
                    pl.when(() -> ProcessLauncher.execCmdWaitResult(anyString(), anyString(), any(Log.class))).thenReturn("");
                    zu.when(() -> ZipUtils.deleteFile(anyString(), any(Log.class))).thenAnswer(a -> null);
                    Assertions.assertDoesNotThrow(() -> connector.checkTiServerAndStop(httpUtil, cdcServer, nodeContext));
                    verify(nodeContext).getLog();
                    verify(httpUtil).checkAlive(cdcServer);
                    verify(log, times(1)).debug("Cdc server is alive, will check change feed list");
                    verify(httpUtil).queryChangeFeedsList(cdcServer);
                    verify(log).debug("There is not any change feed with cdc server: {}, will stop cdc server", cdcServer);
                    verify(nodeContext).getConnectionConfig();
                    verify(config).getString("pdServer");
                }
            }
        }

        @Nested
        class CleanCDCTest {
            KVMap<Object> kvMap;
            HttpUtil httpUtil;
            @BeforeEach
            void init() throws IOException {
                kvMap = mock(KVMap.class);
                httpUtil = mock(HttpUtil.class);
                when(nodeContext.getStateMap()).thenReturn(kvMap);
                when(kvMap.get(ProcessHandler.FEED_ID)).thenReturn("feed-id");
                when(kvMap.get(ProcessHandler.CDC_SERVER)).thenReturn("cdc-server");
                when(kvMap.get(ProcessHandler.CDC_FILE_PATH)).thenReturn("cdc-file-path");
                doNothing().when(log).debug("Start to delete change feed: {}", "feed-id");
                doNothing().when(log).debug("Start to clean cdc data dir: {}", "cdc-file-path");
                doNothing().when(log).debug("Start to check cdc server heath: {}", "cdc-server");
                doNothing().when(connector).checkTiServerAndStop(httpUtil, "cdc-server", nodeContext);
                when(httpUtil.deleteChangeFeed("feed-id", "cdc-server")).thenReturn(true);
            }

            @Test
            void testNormal() throws IOException {
                doCallRealMethod().when(connector).cleanCDC(nodeContext, true);
                try(MockedStatic<HttpUtil> hu = mockStatic(HttpUtil.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    zu.when(() -> ZipUtils.deleteFile("cdc-file-path", log)).thenAnswer(a -> null);
                    hu.when((() -> HttpUtil.of(log))).thenReturn(httpUtil);
                    Assertions.assertDoesNotThrow(() -> connector.cleanCDC(nodeContext, true));
                    verify(nodeContext).getLog();
                    verify(nodeContext).getStateMap();
                    verify(kvMap).get(ProcessHandler.FEED_ID);
                    verify(kvMap).get(ProcessHandler.CDC_SERVER);
                    verify(kvMap).get(ProcessHandler.CDC_FILE_PATH);
                    verify(log).debug("Start to delete change feed: {}", "feed-id");
                    verify(log).debug("Start to clean cdc data dir: {}", "cdc-file-path");
                    verify(log).debug("Start to check cdc server heath: {}", "cdc-server");
                    verify(connector).checkTiServerAndStop(httpUtil, "cdc-server", nodeContext);
                }
            }
            @Test
            void testNorNodeContest() throws IOException {
                doCallRealMethod().when(connector).cleanCDC(tapConnectionContext, true);
                try(MockedStatic<HttpUtil> hu = mockStatic(HttpUtil.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    zu.when(() -> ZipUtils.deleteFile("cdc-file-path", log)).thenAnswer(a -> null);
                    hu.when((() -> HttpUtil.of(log))).thenReturn(httpUtil);
                    Assertions.assertDoesNotThrow(() -> connector.cleanCDC(tapConnectionContext, true));
                    verify(nodeContext, times(0)).getLog();
                    verify(nodeContext, times(0)).getStateMap();
                    verify(kvMap, times(0)).get(ProcessHandler.FEED_ID);
                    verify(kvMap, times(0)).get(ProcessHandler.CDC_SERVER);
                    verify(kvMap, times(0)).get(ProcessHandler.CDC_FILE_PATH);
                    verify(log, times(0)).debug("Start to delete change feed: {}", "feed-id");
                    verify(log, times(0)).debug("Start to clean cdc data dir: {}", "cdc-file-path");
                    verify(log, times(0)).debug("Start to check cdc server heath: {}", "cdc-server");
                    verify(connector, times(0)).checkTiServerAndStop(httpUtil, "cdc-server", nodeContext);
                }
            }
            @Test
            void testFeedIdIsEmpty() throws IOException {
                when(kvMap.get(ProcessHandler.FEED_ID)).thenReturn(null);
                doNothing().when(log).debug("Start to delete change feed: {}", "");
                when(httpUtil.deleteChangeFeed("", "cdc-server")).thenReturn(true);
                doCallRealMethod().when(connector).cleanCDC(nodeContext, true);
                try(MockedStatic<HttpUtil> hu = mockStatic(HttpUtil.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    zu.when(() -> ZipUtils.deleteFile("cdc-file-path", log)).thenAnswer(a -> null);
                    hu.when((() -> HttpUtil.of(log))).thenReturn(httpUtil);
                    Assertions.assertDoesNotThrow(() -> connector.cleanCDC(nodeContext, true));
                    verify(nodeContext).getLog();
                    verify(nodeContext).getStateMap();
                    verify(kvMap).get(ProcessHandler.FEED_ID);
                    verify(kvMap).get(ProcessHandler.CDC_SERVER);
                    verify(kvMap).get(ProcessHandler.CDC_FILE_PATH);
                    verify(log, times(0)).debug("Start to delete change feed: {}", "");
                    verify(log, times(0)).debug("Start to clean cdc data dir: {}", "cdc-file-path");
                    verify(log, times(0)).debug("Start to check cdc server heath: {}", "cdc-server");
                    verify(connector, times(0)).checkTiServerAndStop(httpUtil, "cdc-server", nodeContext);
                }
            }
            @Test
            void testCdcServerIsEmpty() throws IOException {
                when(kvMap.get(ProcessHandler.CDC_SERVER)).thenReturn("");
                doNothing().when(log).debug("Start to check cdc server heath: {}", "");
                when(httpUtil.deleteChangeFeed("feed-id", "")).thenReturn(true);
                doCallRealMethod().when(connector).cleanCDC(nodeContext, true);
                try(MockedStatic<HttpUtil> hu = mockStatic(HttpUtil.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    zu.when(() -> ZipUtils.deleteFile("cdc-file-path", log)).thenAnswer(a -> null);
                    hu.when((() -> HttpUtil.of(log))).thenReturn(httpUtil);
                    Assertions.assertDoesNotThrow(() -> connector.cleanCDC(nodeContext, true));
                    verify(nodeContext).getLog();
                    verify(nodeContext).getStateMap();
                    verify(kvMap).get(ProcessHandler.FEED_ID);
                    verify(kvMap).get(ProcessHandler.CDC_SERVER);
                    verify(kvMap).get(ProcessHandler.CDC_FILE_PATH);
                    verify(log, times(0)).debug("Start to delete change feed: {}", "feed-id");
                    verify(log, times(0)).debug("Start to clean cdc data dir: {}", "cdc-file-path");
                    verify(log, times(0)).debug("Start to check cdc server heath: {}", "");
                    verify(connector, times(0)).checkTiServerAndStop(httpUtil, "", nodeContext);
                }
            }
            @Test
            void testFilePathIsEmpty() throws IOException {
                when(kvMap.get(ProcessHandler.CDC_FILE_PATH)).thenReturn("");
                doNothing().when(log).debug("Start to clean cdc data dir: {}", "");
                doCallRealMethod().when(connector).cleanCDC(nodeContext, true);
                try(MockedStatic<HttpUtil> hu = mockStatic(HttpUtil.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    zu.when(() -> ZipUtils.deleteFile("cdc-file-path", log)).thenAnswer(a -> null);
                    hu.when((() -> HttpUtil.of(log))).thenReturn(httpUtil);
                    Assertions.assertDoesNotThrow(() -> connector.cleanCDC(nodeContext, true));
                    verify(nodeContext).getLog();
                    verify(nodeContext).getStateMap();
                    verify(kvMap).get(ProcessHandler.FEED_ID);
                    verify(kvMap).get(ProcessHandler.CDC_SERVER);
                    verify(kvMap).get(ProcessHandler.CDC_FILE_PATH);
                    verify(log).debug("Start to delete change feed: {}", "feed-id");
                    verify(log, times(0)).debug("Start to clean cdc data dir: {}", "");
                    verify(log).debug("Start to check cdc server heath: {}", "cdc-server");
                    verify(connector).checkTiServerAndStop(httpUtil, "cdc-server", nodeContext);
                }
            }
            @Test
            void testNotStopServer() throws IOException {
                doCallRealMethod().when(connector).cleanCDC(nodeContext, false);
                try(MockedStatic<HttpUtil> hu = mockStatic(HttpUtil.class);
                    MockedStatic<ZipUtils> zu = mockStatic(ZipUtils.class)) {
                    zu.when(() -> ZipUtils.deleteFile("cdc-file-path", log)).thenAnswer(a -> null);
                    hu.when((() -> HttpUtil.of(log))).thenReturn(httpUtil);
                    Assertions.assertDoesNotThrow(() -> connector.cleanCDC(nodeContext, false));
                    verify(nodeContext).getLog();
                    verify(nodeContext).getStateMap();
                    verify(kvMap).get(ProcessHandler.FEED_ID);
                    verify(kvMap).get(ProcessHandler.CDC_SERVER);
                    verify(kvMap).get(ProcessHandler.CDC_FILE_PATH);
                    verify(log).debug("Start to delete change feed: {}", "feed-id");
                    verify(log).debug("Start to clean cdc data dir: {}", "cdc-file-path");
                    verify(log, times(0)).debug("Start to check cdc server heath: {}", "cdc-server");
                    verify(connector, times(0)).checkTiServerAndStop(httpUtil, "cdc-server", nodeContext);
                }
            }
        }

        @Nested
        class onStopTest {
            @Test
            void testNormal() throws Exception {
                doCallRealMethod().when(connector).onStop(tapConnectionContext);
                doNothing().when(connector).cleanCDC(tapConnectionContext, false);
                try(MockedStatic<EmptyKit> ek = mockStatic(EmptyKit.class)) {
                    ek.when(() -> EmptyKit.closeQuietly(tidbJdbcContext)).thenAnswer(a -> null);
                    Assertions.assertDoesNotThrow(() -> connector.onStop(tapConnectionContext));
                    verify(connector).cleanCDC(tapConnectionContext, false);
                }
            }
        }

        @Nested
        class OnDestroyTest {
            @Test
            void testNormal() {
                doNothing().when(connector).cleanCDC(nodeContext, true);
                doCallRealMethod().when(connector).onDestroy(nodeContext);
                Assertions.assertDoesNotThrow(() -> connector.onDestroy(nodeContext));
                verify(connector).cleanCDC(nodeContext, true);
            }
        }

        @Nested
        class TimestampToStreamOffsetTest {
            MysqlBinlogPosition mysqlBinlogPosition;
            @BeforeEach
            void init() throws Throwable {
                mysqlBinlogPosition = mock(MysqlBinlogPosition.class);

                when(tidbJdbcContext.readBinlogPosition()).thenReturn(mysqlBinlogPosition);
                when(tidbJdbcContext.querySafeGcPoint()).thenReturn(0L);

                when(connector.timestampToStreamOffset(nodeContext, null)).thenCallRealMethod();
                when(connector.timestampToStreamOffset(nodeContext, -10L)).thenCallRealMethod();
                when(connector.timestampToStreamOffset(nodeContext, 10L)).thenCallRealMethod();
                doNothing().when(log).warn(anyString(), anyLong(), anyLong());
            }
            @Test
            void testReadFromBinlog() {
                Assertions.assertDoesNotThrow(() -> connector.timestampToStreamOffset(nodeContext, null));
            }
            @Test
            void testMore() throws Throwable {
                Object offset = connector.timestampToStreamOffset(nodeContext, -10L);
                Assertions.assertNotNull(offset);
            }
            @Test
            void testLess() throws Throwable {
                Object offset = connector.timestampToStreamOffset(nodeContext, 10L);
                Assertions.assertNotNull(offset);
                Assertions.assertEquals(10L << 18, offset);
            }
        }

        @Nested
        class processDataMap {
            DataMap dataMap;
            TapTable tapTable;
            @BeforeEach
            void init() {
                dataMap = new DataMap();
                tapTable = mock(TapTable.class);

                doCallRealMethod().when(connector).processDataMap(dataMap, tapTable);
            }

            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> connector.processDataMap(dataMap, tapTable));
            }
            @Test
            void test1() {
                dataMap.put("t", new Timestamp(System.currentTimeMillis()));
                dataMap.put("t2", 2);
                Assertions.assertDoesNotThrow(() -> connector.processDataMap(dataMap, tapTable));
            }
            @Test
            void test2() {
                ReflectionTestUtils.setField(connector, "timezone", null);
                dataMap.put("t", new Timestamp(System.currentTimeMillis()));
                Assertions.assertDoesNotThrow(() -> connector.processDataMap(dataMap, tapTable));
            }
        }

        @Nested
        class initTimeZone {
            @Test
            void testNormal() {
                doCallRealMethod().when(connector).initTimeZone();
                Assertions.assertDoesNotThrow(connector::initTimeZone);
            }
            @Test
            void test1() {
                tidbConfig.setTimezone("+08:00");
                doCallRealMethod().when(connector).initTimeZone();
                Assertions.assertDoesNotThrow(connector::initTimeZone);
            }
        }
    }
}
