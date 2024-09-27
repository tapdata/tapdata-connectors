package io.tapdata.connector.tidb.cdc.process.thread;


import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.tidb.cdc.process.analyse.NormalFileReader;
import io.tapdata.connector.tidb.cdc.process.analyse.ReadConsumer;
import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLObject;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DMLManagerTest {
    DMLManager manager;
    NormalFileReader reader;
    ProcessHandler handler;
    TapEventManager eventManager;
    ConcurrentHashMap<String, DDLManager.VersionInfo> tableVersionMap;
    BooleanSupplier alive;
    Log log;
    List<String> table;
    @BeforeEach
    void init() {
        table = new ArrayList<>();
        manager = mock(DMLManager.class);
        reader = mock(NormalFileReader.class);
        handler = mock(ProcessHandler.class);
        eventManager = mock(TapEventManager.class);
        tableVersionMap = new ConcurrentHashMap<String, DDLManager.VersionInfo>();
        alive = () -> true;
        log = mock(Log.class);
        ReflectionTestUtils.setField(manager, "reader", reader);
        ReflectionTestUtils.setField(manager, "handler", handler);
        ReflectionTestUtils.setField(manager, "alive", alive);
        ReflectionTestUtils.setField(manager, "log", log);
        ReflectionTestUtils.setField(manager, "table", table);

        ReflectionTestUtils.setField(handler, "tableVersionMap", tableVersionMap);
        when(handler.getTapEventManager()).thenReturn(eventManager);
    }

    @Nested
    class withBasePathTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(manager).withBasePath(anyString());
            doCallRealMethod().when(manager).withBasePath(null);
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> manager.withBasePath(""));
            Assertions.assertDoesNotThrow(() -> manager.withBasePath(""));
            Assertions.assertDoesNotThrow(() -> manager.withBasePath("xx"+ File.separator));
        }
    }

    @Test
    void testInit() {
        doCallRealMethod().when(manager).init();
        Assertions.assertDoesNotThrow(() -> manager.init());
    }

    @Test
    void testGetFullBasePath() {
        when(manager.getFullBasePath()).thenCallRealMethod();
        ReflectionTestUtils.setField(manager, "basePath", "basePath");
        ReflectionTestUtils.setField(manager, "database", "database");
        Assertions.assertEquals(FileUtil.paths("basePath", "database"), manager.getFullBasePath());
    }


    @Nested
    class readJsonAndCommit {
        File file;
        @BeforeEach
        void init() {
            file = mock(File.class);
            when(file.getPath()).thenReturn("");
            doNothing().when(eventManager).emit(any(DMLObject.class));
            doNothing().when(log).debug(anyString(), anyString());
            doNothing().when(log).error(anyString(), anyString(), anyString());
            doAnswer(a -> {
                ReadConsumer argument = a.getArgument(1, ReadConsumer.class);
                argument.accept("{\"table\":\"table\"}");
                return null;
            }).when(reader).readLineByLine(any(File.class), any(ReadConsumer.class), any(BooleanSupplier.class));
            doCallRealMethod().when(manager).readJsonAndCommit(file);
        }

        @Test
        void testNormal() {
            try(MockedStatic<FileUtils> fu = mockStatic(FileUtils.class)) {
                fu.when(() -> FileUtils.delete(file)).thenAnswer(a->null);
                manager.readJsonAndCommit(file);
                verify(reader, times(1)).readLineByLine(any(File.class), any(ReadConsumer.class), any(BooleanSupplier.class));
                verify(log, times(1)).debug(anyString(), anyString());
                verify(handler, times(1)).getTapEventManager();
                verify(eventManager, times(1)).emit(any(DMLObject.class));
                verify(log, times(0)).error(anyString(), anyString(), anyString());
            }
        }
        @Test
        void testException() {
            try(MockedStatic<FileUtils> fu = mockStatic(FileUtils.class)) {
                fu.when(() -> FileUtils.delete(file)).thenAnswer(a->{
                    throw new IOException("");
                });
                manager.readJsonAndCommit(file);
                verify(reader, times(1)).readLineByLine(any(File.class), any(ReadConsumer.class), any(BooleanSupplier.class));
                verify(log, times(1)).debug(anyString(), anyString());
                verify(handler, times(1)).getTapEventManager();
                verify(eventManager, times(1)).emit(any(DMLObject.class));
                verify(log, times(1)).error(anyString(), anyString(), anyString());
            }
        }

        @Test
        void testNotNull() {
            DDLManager.VersionInfo versionInfo = new DDLManager.VersionInfo("v", new ArrayList<>(), TimeZone.getDefault(), TimeZone.getDefault());
            tableVersionMap.put("table", versionInfo);
            try(MockedStatic<FileUtils> fu = mockStatic(FileUtils.class)) {
                fu.when(() -> FileUtils.delete(file)).thenAnswer(a->null);
                manager.readJsonAndCommit(file);
                verify(reader, times(1)).readLineByLine(any(File.class), any(ReadConsumer.class), any(BooleanSupplier.class));
                verify(log, times(0)).debug(anyString(), anyString());
                verify(handler, times(1)).getTapEventManager();
                verify(eventManager, times(1)).emit(any(DMLObject.class));
                verify(log, times(0)).error(anyString(), anyString(), anyString());
            } finally {
                tableVersionMap.remove("table");
            }
        }
    }

    @Nested
    class scanAllTableDir {
        File databaseDir;
        File[] files;
        @BeforeEach
        void init() {
            databaseDir = mock(File.class);

            File f1 = mock(File.class);
            when(f1.exists()).thenReturn(false);
            when(f1.isDirectory()).thenReturn(false);
            when(f1.getName()).thenReturn("table");

            File f2 = mock(File.class);
            when(f2.exists()).thenReturn(true);
            when(f2.isDirectory()).thenReturn(false);
            when(f2.getName()).thenReturn("table1");

            File f3 = mock(File.class);
            when(f3.exists()).thenReturn(true);
            when(f3.isDirectory()).thenReturn(false);
            when(f3.getName()).thenReturn("table2");

            File f4 = mock(File.class);
            when(f4.exists()).thenReturn(true);
            when(f4.isDirectory()).thenReturn(false);
            when(f4.getName()).thenReturn("table3");

            files = new File[]{f1, f2,f3,f4};

            when(databaseDir.listFiles(any(FileFilter.class))).thenAnswer(a -> {
                FileFilter argument = a.getArgument(0, FileFilter.class);
                for (File file : files) {
                    argument.accept(file);
                }
                return new File[]{f2,f3,f4};
            });
            when(manager.scanAllTableDir(databaseDir)).thenCallRealMethod();
        }
        @Test
        void testNormal() {
            List<File> files = manager.scanAllTableDir(databaseDir);
            Assertions.assertNotNull(files);
            Assertions.assertEquals(3, files.size());
        }
        @Test
        void testNotContains() {
            table.add("table2");
            try {
                List<File> files = manager.scanAllTableDir(databaseDir);
                Assertions.assertNotNull(files);
                Assertions.assertEquals(1, files.size());
            } finally {
                table.clear();
            }
        }
    }


























    class Context extends TapConnectorContext {
        public Context(TapNodeSpecification specification, DataMap connectionConfig, DataMap nodeConfig, Log log) {
            super(specification, connectionConfig, nodeConfig, log);
        }

    }

    class Consumer extends StreamReadConsumer {
        @Override
        public void accept(List<TapEvent> events, Object offset) {
            StringBuilder builder = new StringBuilder();
            builder.append("\n=====\n");
            builder.append(TapSimplify.toJson(events));
            builder.append("\n=====\n\n");
            System.out.println(builder.toString());
        }
    }


    void testDMLRead() throws Exception {
        Context context = new Context(null, new DataMap(), new DataMap(), new Log() {
            @Override
            public void debug(String s, Object... objects) {

            }

            @Override
            public void info(String s, Object... objects) {

            }

            @Override
            public void warn(String s, Object... objects) {

            }

            @Override
            public void error(String s, Object... objects) {

            }

            @Override
            public void error(String s, Throwable throwable) {

            }

            @Override
            public void fatal(String s, Object... objects) {

            }
        });
        Consumer consumer = new Consumer();
        ProcessHandler.ProcessInfo info = new ProcessHandler.ProcessInfo()
                .withCdcServer("127.0.0.1:8300")
                .withFeedId(UUID.randomUUID().toString().replaceAll("-", ""))
                .withAlive(() -> true)
                .withTapConnectorContext(context)
                .withCdcTable(null)
                .withThrowableCollector(new AtomicReference<>())
                .withCdcOffset(new Object())
                .withDatabase("test");
        ProcessHandler handler = new ProcessHandler(info, consumer);
        handler.init();
        try {
            handler.doActivity();
            int times = 10000;
            while (times > 0) {
                Thread.sleep(100);
                times--;
            }
        } finally {
            handler.close();
        }
    }


    void test() {
        Assertions.assertTrue("schema_450137783885627416_0206100104.json".matches("(schema_)([\\d]{18})(_)([\\d]{10})(\\.json)"));
    }
}