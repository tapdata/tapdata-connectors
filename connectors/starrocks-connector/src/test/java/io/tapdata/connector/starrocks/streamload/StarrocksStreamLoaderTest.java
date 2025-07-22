//package io.tapdata.connector.starrocks.streamload;
//
//import io.tapdata.connector.starrocks.bean.StarrocksConfig;
//import io.tapdata.entity.event.dml.TapInsertRecordEvent;
//import org.junit.jupiter.api.*;
//import org.springframework.test.util.ReflectionTestUtils;
//
//import java.util.Collections;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicReference;
//
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.*;
//
//public class StarrocksStreamLoaderTest {
//
//    @Nested
//    class NeedFlushTest {
//
//        StarrocksStreamLoader StarrocksStreamLoader;
//        RecordStream recordStream;
//
//        @BeforeEach
//        void setup() {
//            StarrocksStreamLoader = mock(StarrocksStreamLoader.class);
//            doCallRealMethod().when(StarrocksStreamLoader).needFlush(any(), anyInt(), anyBoolean());
//            recordStream = mock(RecordStream.class);
//        }
//
//        @Test
//        void testReachLastEvent() {
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "lastEventFlag", new AtomicInteger(0));
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "recordStream", recordStream);
//            when(recordStream.canWrite(anyInt())).thenReturn(true);
//            Assertions.assertFalse(StarrocksStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, false));
//        }
//
//        @Test
//        void testCannotWrite() {
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "lastEventFlag", new AtomicInteger(0));
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "recordStream", recordStream);
//            when(recordStream.canWrite(anyInt())).thenReturn(false);
//            Assertions.assertTrue(StarrocksStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, false));
//        }
//
//        @Test
//        void testNoAgg1() {
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "lastEventFlag", new AtomicInteger(1));
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "recordStream", recordStream);
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "dataColumns", new AtomicReference<>(Collections.emptySet()));
//            when(recordStream.canWrite(anyInt())).thenReturn(true);
//            Assertions.assertFalse(StarrocksStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, false));
//        }
//
//        @Test
//        void testNoAgg2() {
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "lastEventFlag", new AtomicInteger(1));
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "recordStream", recordStream);
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "dataColumns", new AtomicReference<>(Collections.singleton("id")));
//            when(recordStream.canWrite(anyInt())).thenReturn(true);
//            Assertions.assertTrue(StarrocksStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, false));
//        }
//
//        @Test
//        void testIsAgg() {
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "lastEventFlag", new AtomicInteger(1));
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "recordStream", recordStream);
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "dataColumns", new AtomicReference<>(Collections.singleton("id")));
//            when(recordStream.canWrite(anyInt())).thenReturn(true);
//            Assertions.assertFalse(StarrocksStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, true));
//        }
//    }
//    @Nested
//    class TestBuildLoadUrl{
//        StarrocksStreamLoader StarrocksStreamLoader;
//        @BeforeEach
//        void setup() {
//            StarrocksStreamLoader = mock(StarrocksStreamLoader.class);
//        }
//        @DisplayName("test Stream load build url when useHTTPS is true")
//        @Test
//        void test1(){
//            StarrocksConfig StarrocksConfig = mock(StarrocksConfig.class);
//            when(StarrocksConfig.getUseHTTPS()).thenReturn(Boolean.TRUE);
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "StarrocksConfig", StarrocksConfig);
//            doCallRealMethod().when(StarrocksStreamLoader).buildLoadUrl(anyString(),anyString(),anyString());
//            String url = StarrocksStreamLoader.buildLoadUrl("localhost:8080","testDataBase","testTable");
//            Assertions.assertEquals("https://localhost:8080/api/testDataBase/testTable/_stream_load", url);
//        }
//        @DisplayName("test Stream load build url when useHTTPS is false")
//        @Test
//        void test2(){
//            StarrocksConfig StarrocksConfig = mock(StarrocksConfig.class);
//            when(StarrocksConfig.getUseHTTPS()).thenReturn(Boolean.FALSE);
//            ReflectionTestUtils.setField(StarrocksStreamLoader, "StarrocksConfig", StarrocksConfig);
//            doCallRealMethod().when(StarrocksStreamLoader).buildLoadUrl(anyString(),anyString(),anyString());
//            String url = StarrocksStreamLoader.buildLoadUrl("localhost:8080","testDataBase","testTable");
//            Assertions.assertEquals("http://localhost:8080/api/testDataBase/testTable/_stream_load", url);
//        }
//    }
//}
