package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ActivityTest {
    Activity activity;
    @BeforeEach
    void setUp() {
        activity = new Activity() {
            @Override
            public void init() {

            }

            @Override
            public void doActivity() {

            }

            @Override
            public void close() throws Exception {

            }
        };

    }

    @Nested
    class CancelScheduleTest {
        ScheduledFuture<?> future;
        Log log;
        @BeforeEach
        void init() {
            future = mock(ScheduledFuture.class);
            log = mock(Log.class);
            doNothing().when(log).warn(anyString(), anyString());
        }
        @Test
        void testNormal() {
            when(future.cancel(true)).thenReturn(true);
            activity.cancelSchedule(future, log);
            verify(future).cancel(true);
            verify(log, times(0)).warn(anyString(), anyString());
        }
        @Test
        void testException() {
            when(future.cancel(true)).thenAnswer(a -> {
                throw new Exception("");
            });
            activity.cancelSchedule(future, log);
            verify(future).cancel(true);
            verify(log, times(1)).warn(anyString(), anyString());
        }

        @Test
        void testNull() {
            Assertions.assertDoesNotThrow(() -> activity.cancelSchedule(null, null));
            verify(log, times(0)).warn(anyString(), anyString());
        }
    }

    @Nested
    class ScanAllCdcTableDirTest {
        BooleanSupplier alive;
        List<String> cdcTable;
        File[] tableFiles;
        File f;
        File databaseDir;
        @BeforeEach
        void init() {
            alive = () -> true;
            cdcTable = new ArrayList<>();
            f = mock(File.class);
            tableFiles = new File[]{f};
            databaseDir = mock(File.class);
            when(databaseDir.getAbsolutePath()).thenReturn("path");
        }
        @Nested
        class CdcTableIsEmpty {
            @BeforeEach
            void init() {
                cdcTable.clear();
            }
            @Test
            void testTableFileIsNull() {
                when(databaseDir.listFiles(any(FileFilter.class))).thenReturn(null);
                List<File> files = activity.scanAllCdcTableDir(cdcTable, databaseDir, alive);
                Assertions.assertNotNull(files);
                Assertions.assertEquals(0, files.size());
            }
            @Test
            void testTableFileIsEmpty() {
                when(databaseDir.listFiles(any(FileFilter.class))).thenReturn(new File[]{});
                List<File> files = activity.scanAllCdcTableDir(cdcTable, databaseDir, alive);
                Assertions.assertNotNull(files);
                Assertions.assertEquals(0, files.size());
            }
            @Test
            void testTableFileNotEmpty() {
                when(databaseDir.listFiles(any(FileFilter.class))).thenReturn(tableFiles);
                List<File> files = activity.scanAllCdcTableDir(cdcTable, databaseDir, alive);
                Assertions.assertNotNull(files);
                Assertions.assertEquals(1, files.size());
            }
        }
        @Nested
        class CdcTableNotEmpty {
            @BeforeEach
            void init() {
                cdcTable.clear();
                cdcTable.add("t1");
            }
            @Test
            void testNotAlive() {
                alive = () -> false;
                List<File> files = activity.scanAllCdcTableDir(cdcTable, databaseDir, alive);
                Assertions.assertNotNull(files);
                Assertions.assertEquals(0, files.size());
            }
            @Test
            void testNormal() {
                List<File> files = activity.scanAllCdcTableDir(cdcTable, databaseDir, alive);
                Assertions.assertNotNull(files);
                Assertions.assertEquals(0, files.size());
            }
//            @Test
//            void testFileNotExists() {
//                try(MockedConstruction<File> fe = mockConstruction(File.class, (fei, c) -> {
//                    when(fei.exists()).thenReturn(false);
//                    when(fei.isDirectory()).thenReturn(true);
//                })) {
//                    List<File> files = activity.scanAllCdcTableDir(cdcTable, databaseDir, alive);
//                    Assertions.assertNotNull(files);
//                    Assertions.assertEquals(0, files.size());
//                }
//            }
//            @Test
//            void testFileNotDirectory() {
//                try(MockedConstruction<File> fe = mockConstruction(File.class, (fei, c) -> {
//                    when(fei.exists()).thenReturn(true);
//                    when(fei.isDirectory()).thenReturn(false);
//                })) {
//                    List<File> files = activity.scanAllCdcTableDir(cdcTable, databaseDir, alive);
//                    Assertions.assertNotNull(files);
//                    Assertions.assertEquals(0, files.size());
//                }
//            }
        }
    }

    @Test
    void testTOSTime() {
        Assertions.assertNotNull(Activity.getTOSTime(null));
        Assertions.assertEquals(1L << 18, Activity.getTOSTime(1L));
    }
}