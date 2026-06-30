package io.tapdata.common.dml;

import io.tapdata.pdk.apis.entity.WriteListResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class NormalWriteRecorderTest {

    @Nested
    class AddUpdateBatchTest {

        NormalWriteRecorder recorder;
        PreparedStatement preparedStatement;

        @BeforeEach
        void beforeEach() throws SQLException {
            recorder = mock(NormalWriteRecorder.class);
            preparedStatement = mock(PreparedStatement.class);
            ReflectionTestUtils.setField(recorder, "preparedStatement", preparedStatement);
            ReflectionTestUtils.setField(recorder, "allColumn", Arrays.asList("id", "name"));
            ReflectionTestUtils.setField(recorder, "uniqueCondition", Collections.singletonList("id"));
            doAnswer(invocationOnMock -> null).when(preparedStatement).addBatch();
        }

        @Test
        void testNormal() throws SQLException {
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            ReflectionTestUtils.setField(recorder, "updatePolicy", WritePolicyEnum.IGNORE_ON_NONEXISTS);
            Map<String, Object> after = map(entry("id", 1), entry("name", "name"));
            Map<String, Object> before = map(entry("id", 1));
            doCallRealMethod().when(recorder).addUpdateBatch(any(), any(), any());
            recorder.addUpdateBatch(after, before, new WriteListResult<>());
            verify(recorder, times(0)).insertUpdate(any(), any(), any());
            verify(recorder, times(1)).justUpdate(any(), any(), any());
        }

        @Test
        void testInsertUpdate() throws SQLException {
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            ReflectionTestUtils.setField(recorder, "updatePolicy", WritePolicyEnum.INSERT_ON_NONEXISTS);
            Map<String, Object> after = map(entry("id", 1), entry("name", "name"));
            Map<String, Object> before = map(entry("id", 1));
            doCallRealMethod().when(recorder).addUpdateBatch(any(), any(), any());
            recorder.addUpdateBatch(after, before, new WriteListResult<>());
            verify(recorder, times(1)).insertUpdate(any(), any(), any());
            verify(recorder, times(0)).justUpdate(any(), any(), any());
        }

        @Test
        void testEmptyAfter() throws SQLException {
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            ReflectionTestUtils.setField(recorder, "updatePolicy", WritePolicyEnum.INSERT_ON_NONEXISTS);
            Map<String, Object> after = Collections.emptyMap();
            Map<String, Object> before = map(entry("id", 1));
            doCallRealMethod().when(recorder).addUpdateBatch(any(), any(), any());
            recorder.addUpdateBatch(after, before, new WriteListResult<>());
            verify(recorder, times(0)).insertUpdate(any(), any(), any());
            verify(recorder, times(0)).justUpdate(any(), any(), any());
        }
    }

    @Nested
    class Object2StringTest {

        @Test
        void testConcurrentDateFormat() throws Exception {
            NormalWriteRecorder recorder = mock(NormalWriteRecorder.class);
            when(recorder.object2String(any())).thenCallRealMethod();
            java.sql.Date date = java.sql.Date.valueOf("2026-06-23");
            String expected = "'2026-06-23 00:00:00.000000'";
            Pattern expectedPattern = Pattern.compile("'\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}'");
            int threads = 16;
            int iterationsPerThread = 2000;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger malformed = new AtomicInteger();
            AtomicInteger mismatched = new AtomicInteger();
            AtomicInteger errors = new AtomicInteger();
            try {
                for (int t = 0; t < threads; t++) {
                    pool.submit(() -> {
                        ready.countDown();
                        try {
                            start.await();
                            for (int i = 0; i < iterationsPerThread; i++) {
                                try {
                                    String out = recorder.object2String(date);
                                    if (!expectedPattern.matcher(out).matches()) {
                                        malformed.incrementAndGet();
                                    } else if (!expected.equals(out)) {
                                        mismatched.incrementAndGet();
                                    }
                                } catch (Throwable t1) {
                                    errors.incrementAndGet();
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
                ready.await();
                start.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            } finally {
                pool.shutdownNow();
            }
            assertEquals(0, errors.get(), "object2String threw under concurrency");
            assertEquals(0, malformed.get(), "object2String produced malformed date strings under concurrency");
            assertEquals(0, mismatched.get(), "object2String produced unexpected date strings under concurrency");
        }
    }
}
