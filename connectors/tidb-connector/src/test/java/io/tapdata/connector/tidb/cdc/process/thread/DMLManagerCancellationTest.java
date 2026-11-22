package io.tapdata.connector.tidb.cdc.process.thread;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;
import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DMLManagerCancellationTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void workerWaitDistinguishesCancellationFromExecutionFailure(boolean canceled) throws Exception {
        DMLManager manager = mock(DMLManager.class, CALLS_REAL_METHODS);
        AtomicReference<Throwable> failures = new AtomicReference<>();
        ReflectionTestUtils.setField(manager, "throwableCollector", failures);
        ReflectionTestUtils.setField(manager, "alive", (BooleanSupplier) () -> true);
        ReflectionTestUtils.setField(manager, "threadCount", 1);
        doReturn(directory.toString()).when(manager).getFullBasePath();
        doReturn(Collections.singletonList(new File("CDC1.json"))).when(manager).scanAllTableDir(any());
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch workerFinished = new CountDownLatch(1);
        IllegalStateException decodeFailure = new IllegalStateException("invalid change record");
        doAnswer(call -> {
            try {
                if (canceled) {
                    releaseWorker.await();
                } else {
                    throw decodeFailure;
                }
                return null;
            } finally {
                workerFinished.countDown();
            }
        }).when(manager).doOnce(any());
        try {
            if (canceled) Thread.currentThread().interrupt();
            manager.readOnce();
            assertEquals(canceled, Thread.currentThread().isInterrupted());
            if (canceled) {
                assertInstanceOf(InterruptedException.class, failures.get());
            } else {
                assertInstanceOf(ExecutionException.class, failures.get());
                assertSame(decodeFailure, failures.get().getCause());
            }
        } finally {
            Thread.interrupted();
            releaseWorker.countDown();
            assertTrue(workerFinished.await(5, TimeUnit.SECONDS), "Worker must exit after the test");
        }
    }
}
