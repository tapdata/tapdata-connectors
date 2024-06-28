package io.tapdata.connector.doris.streamload;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DorisStreamLoaderTest {

    @Nested
    class NeedFlushTest {

        DorisStreamLoader dorisStreamLoader;
        RecordStream recordStream;

        @BeforeEach
        void setup() {
            dorisStreamLoader = mock(DorisStreamLoader.class);
            doCallRealMethod().when(dorisStreamLoader).needFlush(any(), anyInt(), anyBoolean());
            recordStream = mock(RecordStream.class);
        }

        @Test
        void testReachLastEvent() {
            ReflectionTestUtils.setField(dorisStreamLoader, "lastEventFlag", new AtomicInteger(0));
            ReflectionTestUtils.setField(dorisStreamLoader, "recordStream", recordStream);
            when(recordStream.canWrite(anyInt())).thenReturn(true);
            Assertions.assertFalse(dorisStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, false));
        }

        @Test
        void testCannotWrite() {
            ReflectionTestUtils.setField(dorisStreamLoader, "lastEventFlag", new AtomicInteger(0));
            ReflectionTestUtils.setField(dorisStreamLoader, "recordStream", recordStream);
            when(recordStream.canWrite(anyInt())).thenReturn(false);
            Assertions.assertTrue(dorisStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, false));
        }

        @Test
        void testNoAgg1() {
            ReflectionTestUtils.setField(dorisStreamLoader, "lastEventFlag", new AtomicInteger(1));
            ReflectionTestUtils.setField(dorisStreamLoader, "recordStream", recordStream);
            ReflectionTestUtils.setField(dorisStreamLoader, "dataColumns", new AtomicReference<>(Collections.emptySet()));
            when(recordStream.canWrite(anyInt())).thenReturn(true);
            Assertions.assertFalse(dorisStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, false));
        }

        @Test
        void testNoAgg2() {
            ReflectionTestUtils.setField(dorisStreamLoader, "lastEventFlag", new AtomicInteger(1));
            ReflectionTestUtils.setField(dorisStreamLoader, "recordStream", recordStream);
            ReflectionTestUtils.setField(dorisStreamLoader, "dataColumns", new AtomicReference<>(Collections.singleton("id")));
            when(recordStream.canWrite(anyInt())).thenReturn(true);
            Assertions.assertTrue(dorisStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, false));
        }

        @Test
        void testIsAgg() {
            ReflectionTestUtils.setField(dorisStreamLoader, "lastEventFlag", new AtomicInteger(1));
            ReflectionTestUtils.setField(dorisStreamLoader, "recordStream", recordStream);
            ReflectionTestUtils.setField(dorisStreamLoader, "dataColumns", new AtomicReference<>(Collections.singleton("id")));
            when(recordStream.canWrite(anyInt())).thenReturn(true);
            Assertions.assertFalse(dorisStreamLoader.needFlush(new TapInsertRecordEvent().init().after(Collections.emptyMap()), 1, true));
        }
    }
}
