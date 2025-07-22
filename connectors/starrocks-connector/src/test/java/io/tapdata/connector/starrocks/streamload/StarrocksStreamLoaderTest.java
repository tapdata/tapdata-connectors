package io.tapdata.connector.starrocks.streamload;

import io.tapdata.connector.starrocks.bean.StarrocksConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class StarrocksStreamLoaderTest {

    @Nested
    class NeedFlushTest {

        StarrocksStreamLoader starrocksStreamLoader;
        RecordStream recordStream;
        StarrocksConfig starrocksConfig;
        Map<String, Long> lastFlushTimeByTable;
        Map<String, Long> currentBatchSizeByTable;
        Map<String, Set<String>> dataColumnsByTable;

        @BeforeEach
        void setup() {
            starrocksStreamLoader = mock(StarrocksStreamLoader.class);
            doCallRealMethod().when(starrocksStreamLoader).needFlush(any(), anyInt(), anyBoolean(), anyString());
            recordStream = mock(RecordStream.class);
            starrocksConfig = mock(StarrocksConfig.class);
            lastFlushTimeByTable = new ConcurrentHashMap<>();
            currentBatchSizeByTable = new ConcurrentHashMap<>();
            dataColumnsByTable = new ConcurrentHashMap<>();
            // 设置默认的配置值
            when(starrocksConfig.getFlushTimeoutSeconds()).thenReturn(30);
            when(starrocksConfig.getFlushSizeMB()).thenReturn(10);
        }

        @Test
        void testNoDataInTable() {
            // Given: 表中没有数据（不在 pendingFlushTables 中）
            Set<String> pendingFlushTables = new HashSet<>();
            ReflectionTestUtils.setField(starrocksStreamLoader, "pendingFlushTables", pendingFlushTables);
            ReflectionTestUtils.setField(starrocksStreamLoader, "StarrocksConfig", starrocksConfig);
            ReflectionTestUtils.setField(starrocksStreamLoader, "recordStream", recordStream);
            ReflectionTestUtils.setField(starrocksStreamLoader, "lastFlushTimeByTable", lastFlushTimeByTable);
            ReflectionTestUtils.setField(starrocksStreamLoader, "currentBatchSizeByTable", currentBatchSizeByTable);
            ReflectionTestUtils.setField(starrocksStreamLoader, "dataColumnsByTable", dataColumnsByTable);
            when(recordStream.canWrite(anyInt())).thenReturn(true);

            // When & Then: 没有数据时不需要刷新
            Assertions.assertFalse(starrocksStreamLoader.needFlush(
                new TapInsertRecordEvent().init().after(Collections.emptyMap()),
                1, false, "testTable"));
        }

        @Test
        void testBufferFull() {
            // Given: 缓冲区已满
            Set<String> pendingFlushTables = new HashSet<>();
            pendingFlushTables.add("testTable");
            ReflectionTestUtils.setField(starrocksStreamLoader, "pendingFlushTables", pendingFlushTables);
            ReflectionTestUtils.setField(starrocksStreamLoader, "StarrocksConfig", starrocksConfig);
            ReflectionTestUtils.setField(starrocksStreamLoader, "recordStream", recordStream);
            ReflectionTestUtils.setField(starrocksStreamLoader, "lastFlushTimeByTable", lastFlushTimeByTable);
            ReflectionTestUtils.setField(starrocksStreamLoader, "currentBatchSizeByTable", currentBatchSizeByTable);
            ReflectionTestUtils.setField(starrocksStreamLoader, "dataColumnsByTable", dataColumnsByTable);
            when(recordStream.canWrite(anyInt())).thenReturn(false);

            // When & Then: 缓冲区满时需要刷新
            Assertions.assertTrue(starrocksStreamLoader.needFlush(
                new TapInsertRecordEvent().init().after(Collections.emptyMap()),
                1, false, "testTable"));
        }

        @Test
        void testDataColumnsChanged() {
            // Given: 数据列发生变化（通过事件的 after 数据体现）
            Set<String> pendingFlushTables = new HashSet<>();
            pendingFlushTables.add("testTable");
            Map<String, Set<String>> dataColumnsByTable = new ConcurrentHashMap<>();
            dataColumnsByTable.put("testTable", Collections.singleton("oldColumn"));

            ReflectionTestUtils.setField(starrocksStreamLoader, "pendingFlushTables", pendingFlushTables);
            ReflectionTestUtils.setField(starrocksStreamLoader, "dataColumnsByTable", dataColumnsByTable);
            ReflectionTestUtils.setField(starrocksStreamLoader, "StarrocksConfig", starrocksConfig);
            ReflectionTestUtils.setField(starrocksStreamLoader, "recordStream", recordStream);
            ReflectionTestUtils.setField(starrocksStreamLoader, "lastFlushTimeByTable", lastFlushTimeByTable);
            ReflectionTestUtils.setField(starrocksStreamLoader, "currentBatchSizeByTable", currentBatchSizeByTable);
            when(recordStream.canWrite(anyInt())).thenReturn(true);

            // When & Then: 数据列变化时需要刷新（新列 "newColumn" 与已存储的 "oldColumn" 不同）
            TapInsertRecordEvent event = new TapInsertRecordEvent().init().after(Collections.singletonMap("newColumn", "value"));
            Assertions.assertTrue(starrocksStreamLoader.needFlush(event, 1, false, "testTable"));
        }

        @Test
        void testNoFlushNeeded() {
            // Given: 正常情况，不需要刷新
            Set<String> pendingFlushTables = new HashSet<>();
            pendingFlushTables.add("testTable");
            Map<String, Set<String>> dataColumnsByTable = new ConcurrentHashMap<>();
            dataColumnsByTable.put("testTable", Collections.singleton("column1"));
            Map<String, Long> currentBatchSizeByTable = new ConcurrentHashMap<>();
            currentBatchSizeByTable.put("testTable", 1000L); // 小于阈值
            Map<String, Long> lastFlushTimeByTable = new ConcurrentHashMap<>();
            lastFlushTimeByTable.put("testTable", System.currentTimeMillis()); // 刚刚刷新过

            ReflectionTestUtils.setField(starrocksStreamLoader, "pendingFlushTables", pendingFlushTables);
            ReflectionTestUtils.setField(starrocksStreamLoader, "dataColumnsByTable", dataColumnsByTable);
            ReflectionTestUtils.setField(starrocksStreamLoader, "currentBatchSizeByTable", currentBatchSizeByTable);
            ReflectionTestUtils.setField(starrocksStreamLoader, "lastFlushTimeByTable", lastFlushTimeByTable);
            ReflectionTestUtils.setField(starrocksStreamLoader, "StarrocksConfig", starrocksConfig);
            ReflectionTestUtils.setField(starrocksStreamLoader, "recordStream", recordStream);
            when(recordStream.canWrite(anyInt())).thenReturn(true);

            // When & Then: 正常情况下不需要刷新（相同的列）
            TapInsertRecordEvent event = new TapInsertRecordEvent().init().after(Collections.singletonMap("column1", "value"));
            Assertions.assertFalse(starrocksStreamLoader.needFlush(event, 1, false, "testTable"));
        }
    }
    @Nested
    class TestBuildLoadUrl{
        StarrocksStreamLoader starrocksStreamLoader;

        @BeforeEach
        void setup() {
            starrocksStreamLoader = mock(StarrocksStreamLoader.class);
        }

        @DisplayName("test Stream load build url when useHTTPS is true")
        @Test
        void test1(){
            StarrocksConfig starrocksConfig = mock(StarrocksConfig.class);
            when(starrocksConfig.getUseHTTPS()).thenReturn(Boolean.TRUE);
            ReflectionTestUtils.setField(starrocksStreamLoader, "StarrocksConfig", starrocksConfig);
            doCallRealMethod().when(starrocksStreamLoader).buildLoadUrl(anyString(), anyString(), anyString());
            String url = starrocksStreamLoader.buildLoadUrl("localhost:8080", "testDataBase", "testTable");
            Assertions.assertEquals("https://localhost:8080/api/testDataBase/testTable/_stream_load", url);
        }

        @DisplayName("test Stream load build url when useHTTPS is false")
        @Test
        void test2(){
            StarrocksConfig starrocksConfig = mock(StarrocksConfig.class);
            when(starrocksConfig.getUseHTTPS()).thenReturn(Boolean.FALSE);
            ReflectionTestUtils.setField(starrocksStreamLoader, "StarrocksConfig", starrocksConfig);
            doCallRealMethod().when(starrocksStreamLoader).buildLoadUrl(anyString(), anyString(), anyString());
            String url = starrocksStreamLoader.buildLoadUrl("localhost:8080", "testDataBase", "testTable");
            Assertions.assertEquals("http://localhost:8080/api/testDataBase/testTable/_stream_load", url);
        }
    }
}
