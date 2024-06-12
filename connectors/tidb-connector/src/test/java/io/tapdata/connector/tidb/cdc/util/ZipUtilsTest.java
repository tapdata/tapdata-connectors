package io.tapdata.connector.tidb.cdc.util;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ZipUtilsTest {

    @Nested
    class unzipTest {
        @Test
        void testNullZipFile() {
            Assertions.assertThrows(CoreException.class, () -> {
                try{
                    ZipUtils.unzip(null, "");
                } catch (CoreException e) {
                    Assertions.assertEquals(ZipUtils.COMMON_ILLEGAL_PARAMETERS, e.getCode());
                    throw e;
                }
            });
        }
        @Test
        void testNullOutputPath() {
            Assertions.assertThrows(CoreException.class, () -> {
                try{
                    ZipUtils.unzip("", (String) null);
                } catch (CoreException e) {
                    Assertions.assertEquals(ZipUtils.COMMON_ILLEGAL_PARAMETERS, e.getCode());
                    throw e;
                }
            });
        }
        @Test
        void testIsFile() {
            try(MockedStatic<ZipUtils> zu = Mockito.mockStatic(ZipUtils.class); MockedConstruction<File> f = Mockito.mockConstruction(File.class, (mf, v) -> {
                when(mf.isFile()).thenReturn(true);
            })) {
                zu.when(() -> ZipUtils.unzip(anyString(), anyString())).thenCallRealMethod();
                zu.when(() -> ZipUtils.unzip(anyString(), any(File.class))).thenAnswer(a->null);
                zu.when(() -> ZipUtils.unTarZip(anyString(), anyString())).thenAnswer(a->null);
                Assertions.assertThrows(CoreException.class, () -> {
                    try{
                        ZipUtils.unzip("", "");
                    } catch (CoreException e) {
                        Assertions.assertEquals(ZipUtils.CLI_UNZIP_DIR_IS_FILE, e.getCode());
                        throw e;
                    }
                });

            }
        }

        @Test
        void testIsTar() {
            try(MockedStatic<ZipUtils> zu = Mockito.mockStatic(ZipUtils.class);
                MockedConstruction<File> f = Mockito.mockConstruction(File.class, (mf, v) -> {
                when(mf.isFile()).thenReturn(false);
            })) {
                zu.when(() -> ZipUtils.unzip(anyString(), any(File.class))).thenAnswer(a->null);
                zu.when(() -> ZipUtils.unzip(anyString(), anyString())).thenCallRealMethod();
                zu.when(() -> ZipUtils.unTarZip(anyString(), anyString())).thenAnswer(a->null);
                Assertions.assertDoesNotThrow(() -> ZipUtils.unzip("xx.tar.gz", "x"));
            }
        }
        @Test
        void testIsGz() {
            try(MockedStatic<ZipUtils> zu = Mockito.mockStatic(ZipUtils.class); MockedConstruction<File> f = Mockito.mockConstruction(File.class, (mf, v) -> {
                when(mf.isFile()).thenReturn(false);
            })) {
                zu.when(() -> ZipUtils.unzip(anyString(), anyString())).thenCallRealMethod();
                zu.when(() -> ZipUtils.unzip(anyString(), any(File.class))).thenAnswer(a->null);
                zu.when(() -> ZipUtils.unTarZip(anyString(), anyString())).thenAnswer(a->null);
                Assertions.assertDoesNotThrow(() -> ZipUtils.unzip("xx.gz", ""));
            }
        }
        @Test
        void testIsZip() {
            try(MockedStatic<ZipUtils> zu = Mockito.mockStatic(ZipUtils.class); MockedConstruction<File> f = Mockito.mockConstruction(File.class, (mf, v) -> {
                when(mf.isFile()).thenReturn(false);
            })) {
                zu.when(() -> ZipUtils.unzip(anyString(), anyString())).thenCallRealMethod();
                zu.when(() -> ZipUtils.unzip(anyString(), any(File.class))).thenAnswer(a->null);
                zu.when(() -> ZipUtils.unTarZip(anyString(), anyString())).thenAnswer(a->null);
                Assertions.assertDoesNotThrow(() -> ZipUtils.unzip("xx.zip", ""));
            }
        }
    }

    @Nested
    class deleteFileTest {
        Log log;
        Path directory;
        @BeforeEach
        void init() {
            directory = mock(Path.class);
            log = mock(Log.class);
            doNothing().when(log).debug(anyString(), anyString(), any(IOException.class));
        }

        @Test
        void testNormal() {
            try(MockedStatic<Paths> ps = mockStatic(Paths.class);
                MockedStatic<Files> fs = mockStatic(Files.class)) {
                ps.when(() -> Paths.get(anyString())).thenReturn(directory);
                ps.when(() -> Files.delete(any(Path.class))).thenAnswer(a->null);
                fs.when(() -> Files.walkFileTree(any(Path.class), any(SimpleFileVisitor.class))).thenAnswer(a -> {
                    SimpleFileVisitor argument = a.getArgument(1, SimpleFileVisitor.class);
                    FileVisitResult fileVisitResult = argument.visitFile(directory, mock(BasicFileAttributes.class));
                    Assertions.assertEquals(FileVisitResult.CONTINUE, fileVisitResult);
                    FileVisitResult r = argument.postVisitDirectory(directory, new IOException(""));
                    Assertions.assertEquals(FileVisitResult.CONTINUE, r);
                    return null;
                });
                Assertions.assertDoesNotThrow(() -> ZipUtils.deleteFile("", log));
                verify(log, times(0)).debug(anyString(), anyString(), any(IOException.class));
            }
        }
        @Test
        void testIOException() {
            try(MockedStatic<Paths> ps = mockStatic(Paths.class);
                MockedStatic<Files> fs = mockStatic(Files.class)) {
                ps.when(() -> Paths.get(anyString())).thenReturn(directory);
                ps.when(() -> Files.delete(any(Path.class))).thenAnswer(a->null);
                fs.when(() -> Files.walkFileTree(any(Path.class), any(SimpleFileVisitor.class))).thenAnswer(a -> {
                    throw new IOException("xxx");
                });
                Assertions.assertDoesNotThrow(() -> ZipUtils.deleteFile("", log));
                verify(log, times(1)).debug(anyString(), anyString(), any(IOException.class));
            }
        }
    }
}