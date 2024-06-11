package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.connector.tidb.cdc.process.analyse.NormalFileReader;
import io.tapdata.connector.tidb.cdc.process.ddl.convert.Convert;
import io.tapdata.connector.tidb.cdc.process.ddl.entity.DDLObject;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DDLManagerTest {
    DDLManager ddlManager;

    ProcessHandler handler;
    ProcessHandler.ProcessInfo processInfo;
    TapConnectorContext nodeContext;
    Log log;

    @BeforeEach
    void setUp() {
        log = mock(Log.class);
        nodeContext = mock(TapConnectorContext.class);
        when(nodeContext.getLog()).thenReturn(log);
        processInfo = new ProcessHandler.ProcessInfo()
                .withCdcTable(new ArrayList<>())
                .withDatabase("db")
                .withTapConnectorContext(nodeContext)
                .withAlive(()->true)
                .withZone(TimeZone.getDefault())
                .withThrowableCollector(new AtomicReference<>());
        handler = mock(ProcessHandler.class);

        ReflectionTestUtils.setField(handler, "processInfo", processInfo);
        ddlManager = new DDLManager(handler, "");
    }

    @Test
    void testInit() {
        Assertions.assertDoesNotThrow(() -> ddlManager.init());
    }

    @Test
    void testDoActivity() {
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        when(scheduledExecutorService.scheduleWithFixedDelay(
            any(Runnable.class),
            anyInt(),
            anyInt(),
            any(TimeUnit.class)
        )).thenReturn((ScheduledFuture)scheduledFuture);
        when(handler.getScheduledExecutorService()).thenReturn(scheduledExecutorService);
        Assertions.assertDoesNotThrow(() -> ddlManager.doActivity());
    }

    @Nested
    class testScanDDL {
        DDLManager manager;
        List<File> tableDirs;
        @BeforeEach
        void init() {
            manager = mock(DDLManager.class);
            ReflectionTestUtils.setField(manager, "throwableCollector", new AtomicReference<Throwable>());
            tableDirs = new ArrayList<>();
            when(manager.getTableDirs()).thenReturn(tableDirs);
            doNothing().when(manager).scanTableDDL(any(File.class));
            doCallRealMethod().when(manager).scanDDL();
        }
        @Test
        void testNormal() {
            File f = new File("");
            tableDirs.add(f);
            Assertions.assertDoesNotThrow(manager::scanDDL);
        }
        @Test
        void testEmpty(){
            Assertions.assertDoesNotThrow(manager::scanDDL);
        }
        @Test
        void testException() {
            File f = new File("");
            tableDirs.add(f);
            doAnswer(a -> {
                throw new IOException("");
            }).when(manager).scanTableDDL(any(File.class));
            Assertions.assertDoesNotThrow(manager::scanDDL);
        }
    }

    @Nested
    class GetSchemaJsonFileTest {
        File metaDir;
        File[] schemaJson;
        File f1;
        File f2;
        File f3;
        NormalFileReader reader;
        @BeforeEach
        void init() {
            metaDir = mock(File.class);
            f1 = mock(File.class);
            when(f1.getName()).thenReturn("schema_123456789012345678_1234567890.json");
            when(f1.isFile()).thenReturn(true);

            f2 = mock(File.class);
            when(f2.getName()).thenReturn("schema_123456789012345678_1234567890.json");
            when(f2.isFile()).thenReturn(false);

            f3 = mock(File.class);
            when(f3.getName()).thenReturn("schema.json");
            when(f3.isFile()).thenReturn(true);
            schemaJson = new File[]{f1, f2, f3};

            when(metaDir.listFiles(any(FileFilter.class))).thenAnswer(a -> {
                FileFilter argument = a.getArgument(0, FileFilter.class);
                for (File file : schemaJson) {
                    argument.accept(file);
                }
                return new File[]{f1};
            });

            reader = mock(NormalFileReader.class);
            ReflectionTestUtils.setField(ddlManager, "reader", reader);
            when(reader.readAll(any(File.class), any(BooleanSupplier.class))).thenReturn("{\"TableVersion\":1}");
            doNothing().when(log).warn(anyString(), anyString());
        }

        @Test
        void testNormal() {
            List<DDLObject> schemaJsonFile = ddlManager.getSchemaJsonFile(metaDir);
            Assertions.assertNotNull(schemaJsonFile);
            Assertions.assertEquals(1, schemaJsonFile.size());
        }
        @Test
        void testEmptySchema() {
            when(metaDir.listFiles(any(FileFilter.class))).thenAnswer(a -> {
                FileFilter argument = a.getArgument(0, FileFilter.class);
                for (File file : schemaJson) {
                    argument.accept(file);
                }
                return new File[]{};
            });
            List<DDLObject> schemaJsonFile = ddlManager.getSchemaJsonFile(metaDir);
            Assertions.assertNotNull(schemaJsonFile);
            Assertions.assertEquals(0, schemaJsonFile.size());
        }
        @Test
        void testNullSchema() {
            when(metaDir.listFiles(any(FileFilter.class))).thenAnswer(a -> {
                FileFilter argument = a.getArgument(0, FileFilter.class);
                for (File file : schemaJson) {
                    argument.accept(file);
                }
                return null;
            });
            List<DDLObject> schemaJsonFile = ddlManager.getSchemaJsonFile(metaDir);
            Assertions.assertNotNull(schemaJsonFile);
            Assertions.assertEquals(0, schemaJsonFile.size());
        }
        @Test
        void testNotVersion() {
            when(reader.readAll(any(File.class), any(BooleanSupplier.class))).thenReturn("{}");
            List<DDLObject> schemaJsonFile = ddlManager.getSchemaJsonFile(metaDir);
            Assertions.assertNotNull(schemaJsonFile);
            Assertions.assertEquals(0, schemaJsonFile.size());
        }
        @Test
        void testException() {
            when(reader.readAll(any(File.class), any(BooleanSupplier.class))).thenReturn("{]");
            List<DDLObject> schemaJsonFile = ddlManager.getSchemaJsonFile(metaDir);
            Assertions.assertNotNull(schemaJsonFile);
            Assertions.assertEquals(0, schemaJsonFile.size());
        }
    }

    @Nested
    class ScanTableDDLTest {
        DDLManager manager;
        ProcessHandler handler;
        TapEventManager tapEventManager;

        File tableDir;
        List<DDLObject> schemaJsonFile;
        @BeforeEach
        void init() {
            handler = mock(ProcessHandler.class);
            manager = mock(DDLManager.class);
            tapEventManager = mock(TapEventManager.class);
            ReflectionTestUtils.setField(manager, "handler", handler);
            ReflectionTestUtils.setField(handler, "tableVersionLock", new Object());
            ReflectionTestUtils.setField(handler, "processInfo", processInfo);
            ReflectionTestUtils.setField(handler, "tableVersionMap", new ConcurrentHashMap<String, DDLManager.VersionInfo>());

            schemaJsonFile = new ArrayList<>();
            DDLObject d1 = new DDLObject();
            d1.setTableVersion(1L);
            d1.setTableColumns(new ArrayList<>());
            DDLObject d2 = new DDLObject();
            d2.setTableVersion(5L);
            d2.setTableColumns(new ArrayList<>());
            DDLObject d3 = new DDLObject();
            d3.setTableVersion(2L);
            d3.setTableColumns(new ArrayList<>());
            DDLObject d4 = new DDLObject();
            d4.setTableVersion(3L);
            d4.setTableColumns(new ArrayList<>());
            schemaJsonFile.add(d1);
            schemaJsonFile.add(d2);
            schemaJsonFile.add(d3);
            schemaJsonFile.add(d4);

            tableDir = mock(File.class);
            when(tableDir.exists()).thenReturn(true);
            when(tableDir.isDirectory()).thenReturn(true);
            when(tableDir.getName()).thenReturn("name");
            when(tableDir.getAbsolutePath()).thenReturn("path");
            when(manager.getSchemaJsonFile(any(File.class))).thenReturn(schemaJsonFile);
            when(handler.judgeTableVersion("name")).thenReturn("1");
            when(handler.judgeTableVersionHasDMlData(any(File.class), anyString())).thenReturn(false);
            when(handler.getTapEventManager()).thenReturn(tapEventManager);
            doNothing().when(tapEventManager).emit(any(DDLObject.class));

            doCallRealMethod().when(manager).scanTableDDL(tableDir);
        }

        @Test
        void testNormal() {
            manager.scanTableDDL(tableDir);
        }
        @Test
        void testTableDirNotExists() {
            when(tableDir.exists()).thenReturn(false);
            manager.scanTableDDL(tableDir);
        }
        @Test
        void testNotDirectory() {
            when(tableDir.isDirectory()).thenReturn(false);
            manager.scanTableDDL(tableDir);
        }
        @Test
        void testSchemaJsonFileIsEmpty() {
            schemaJsonFile.clear();
            manager.scanTableDDL(tableDir);
        }
        @Test
        void testOldDDLTableVersionIsNull() {
            when(handler.judgeTableVersion("name")).thenReturn(null);
            manager.scanTableDDL(tableDir);
        }
        @Test
        void testNotChange() {
            when(handler.judgeTableVersion("name")).thenReturn("56");
            manager.scanTableDDL(tableDir);
        }
        @Test
        void testNotJudgeTableVersionHasDMlData() {
            when(handler.judgeTableVersionHasDMlData(any(File.class), anyString())).thenReturn(true);
            manager.scanTableDDL(tableDir);
        }
    }

    @Nested
    class GetTableDirsTest {
        List<String> cdcTable;
        @BeforeEach
        void init() {
            cdcTable = new ArrayList<>();
            cdcTable.add("t");
        }
        @Test
        void testNormal() {
            List<File> tableDirs = ddlManager.getTableDirs();
            Assertions.assertNotNull(tableDirs);
        }
    }

    @Nested
    class CloseTest {
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(ddlManager::close);
        }
    }

    @Nested
    class VersionInfoTest {
        @Test
        void testNormal() {
            List<Map<String, Object>> info = new ArrayList<>();
            Map<String, Object> convertInfo = new HashMap<>();
            convertInfo.put(Convert.COLUMN_NAME, "name");
            info.add(convertInfo);
            new DDLManager.VersionInfo("v", info, TimeZone.getDefault());
        }
    }
}