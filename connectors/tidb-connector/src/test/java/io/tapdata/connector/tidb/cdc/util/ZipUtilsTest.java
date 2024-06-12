package io.tapdata.connector.tidb.cdc.util;

import io.tapdata.entity.error.CoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
}