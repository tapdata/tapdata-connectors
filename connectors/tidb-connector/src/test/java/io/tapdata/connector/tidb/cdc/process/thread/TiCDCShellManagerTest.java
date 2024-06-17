package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TiCDCShellManagerTest {
    TiCDCShellManager manager;
    TiCDCShellManager.ShellConfig shellConfig;
    Log log;
    @BeforeEach
    void init() {
        log = mock(Log.class);
        shellConfig = new TiCDCShellManager.ShellConfig()
                .withTapConnectionContext(mock(TapConnectorContext.class))
                .withTiDBConfig(mock(TidbConfig.class))
                .withGcTtl(1)
                .withPdIpPorts("127.0.0.1:3333")
                .withCdcServerIpPort("127.0.0.1:3334")
                .withClusterId("id")
                .withLocalStrongPath("log")
                .withLogLevel(TiCDCShellManager.LogLevel.INFO)
                .withLogDir("dir");
        shellConfig.getPdIpPorts();
        manager = mock(TiCDCShellManager.class);
        ReflectionTestUtils.setField(manager, "shellConfig", shellConfig);
        ReflectionTestUtils.setField(manager, "log", log);
    }
    @BeforeEach
    void setUp() {
    }

    @Test
    void testConfig() {
        Assertions.assertThrows(CoreException.class, () -> shellConfig.withPdIpPorts(null));
        Assertions.assertDoesNotThrow(() -> shellConfig.withPdIpPorts("http://127.0.0.1:3333"));
    }

    @Test
    void testGetCdcPsGrepFilter() {
        String cdcPsGrepFilter = TiCDCShellManager.getCdcPsGrepFilter("http://127.0.0.1:2222", "127.0.0.1:2223");
        Assertions.assertNotNull(cdcPsGrepFilter);
    }

    @Test
    void testGetPdServerGrepFilter() {
        String cdcPsGrepFilter = TiCDCShellManager.getPdServerGrepFilter("http://127.0.0.1:2222");
        Assertions.assertNotNull(cdcPsGrepFilter);
    }

    @Test
    void testInit() {
        doCallRealMethod().when(manager).init();
        Assertions.assertDoesNotThrow(manager::init);
    }
    @Test
    void testGetCmdAfterSetProperties() {
        doNothing().when(log).info(anyString());
        when(manager.getCmdAfterSetProperties()).thenCallRealMethod();
        Assertions.assertNotNull(manager.getCmdAfterSetProperties());
    }

    @Test
    void testGetCdcToolPath() {
        when(manager.getCdcToolPath()).thenCallRealMethod();
        Assertions.assertNotNull(manager.getCdcToolPath());
    }
    @Test
    void testClose() throws Exception {
        doCallRealMethod().when(manager).close();
        Assertions.assertDoesNotThrow(manager::close);
    }

    @Test
    void testGetInfoFromCmdLine() {
        when(manager.getInfoFromCmdLine(anyString(), anyString(), anyString())).thenCallRealMethod();
        String infoFromCmdLine = manager.getInfoFromCmdLine("sssdkkk", "sss", "kkk");
        Assertions.assertEquals("d", infoFromCmdLine);
    }
    @Test
    void testGetCdcServerPort() {
        when(manager.getCdcServerPort(anyString())).thenCallRealMethod();
        Assertions.assertEquals(8300, manager.getCdcServerPort("xxx"));
        Assertions.assertEquals(8301, manager.getCdcServerPort("127.0.0.1:8301"));
        Assertions.assertEquals(8300, manager.getCdcServerPort("127.0:0.1:8300"));
        Assertions.assertEquals(8300, manager.getCdcServerPort("127.0.0.1:8r00"));
    }
    @Nested
    class cdcFileName {
        @BeforeEach
        void init() {
            when(manager.cdcFileName()).thenCallRealMethod();
        }
        void testOne(String name, String r) {
            String a = System.getProperty("os.arch");
            try {
                System.setProperty("os.arch", name);
                Assertions.assertEquals(r, manager.cdcFileName());
            } finally {
                System.setProperty("os.arch", a);
                Assertions.assertEquals(a, System.getProperty("os.arch"));
            }
        }
        @Test
        void testArm() {
            testOne("arm", "cdc_arm");
        }
        @Test
        void testAmd64() {
            testOne("amd64", "cdc_amd");
        }
        @Test
        void testX86_64() {
            testOne("x86_64", "cdc_x86_64");
        }
        @Test
        void testX86() {
            testOne("x86", "cdc_x86");
        }
        @Test
        void testOther() {
            testOne("ddd", "cdc");
        }
    }
    @Nested
    class CheckFile {
        @BeforeEach
        void init() {
            doNothing().when(log).debug(anyString(), anyString());
            doCallRealMethod().when(manager).checkDir(anyString());
        }
        @Test
        void notExists() {
            try(MockedStatic<java.nio.file.Files> fs = mockStatic(java.nio.file.Files.class)) {
                fs.when(() -> java.nio.file.Files.exists(any(Path.class))).thenReturn(false);
                fs.when(() -> java.nio.file.Files.createDirectories(any(Path.class))).thenReturn(mock(Path.class));
                fs.when(() -> java.nio.file.Files.isDirectory(any(Path.class))).thenReturn(false);
                fs.when(() -> java.nio.file.Files.delete(any(Path.class))).thenAnswer(a -> null);
                Assertions.assertDoesNotThrow(() -> manager.checkDir("txt"));
            }
        }
        @Test
        void notDirectory() {
            try(MockedStatic<java.nio.file.Files> fs = mockStatic(java.nio.file.Files.class)) {
                fs.when(() -> java.nio.file.Files.exists(any(Path.class))).thenReturn(true);
                fs.when(() -> java.nio.file.Files.createDirectories(any(Path.class))).thenReturn(mock(Path.class));
                fs.when(() -> java.nio.file.Files.isDirectory(any(Path.class))).thenReturn(false);
                fs.when(() -> java.nio.file.Files.delete(any(Path.class))).thenAnswer(a -> null);
                Assertions.assertDoesNotThrow(() -> manager.checkDir("txt"));
            }
        }
        @Test
        void isDirectory() {
            try(MockedStatic<java.nio.file.Files> fs = mockStatic(java.nio.file.Files.class)) {
                fs.when(() -> java.nio.file.Files.exists(any(Path.class))).thenReturn(true);
                fs.when(() -> java.nio.file.Files.createDirectories(any(Path.class))).thenReturn(mock(Path.class));
                fs.when(() -> java.nio.file.Files.isDirectory(any(Path.class))).thenReturn(true);
                fs.when(() -> java.nio.file.Files.delete(any(Path.class))).thenAnswer(a -> null);
                Assertions.assertDoesNotThrow(() -> manager.checkDir("txt"));
            }
        }
        @Test
        void isDeleteFailed() {
            try(MockedStatic<java.nio.file.Files> fs = mockStatic(java.nio.file.Files.class)) {
                fs.when(() -> java.nio.file.Files.exists(any(Path.class))).thenReturn(true);
                fs.when(() -> java.nio.file.Files.createDirectories(any(Path.class))).thenReturn(mock(Path.class));
                fs.when(() -> java.nio.file.Files.isDirectory(any(Path.class))).thenReturn(false);
                fs.when(() -> java.nio.file.Files.delete(any(Path.class))).thenAnswer(a -> {
                    throw new IOException("");
                });
                Assertions.assertDoesNotThrow(() -> manager.checkDir("txt"));
            }
        }

    }
}