package io.tapdata.oceanbase.cdc;

import io.tapdata.entity.logger.Log;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OceanbaseNativeReaderStopTest {
    @Test
    void stoppingBeforeLaunchIsSafe() {
        assertDoesNotThrow(reader()::stop);
    }

    @Test
    void gracefulStopReleasesProcessOnlyOnce() throws Exception {
        OceanbaseNativeReader reader = reader();
        Process process = process(reader);
        when(process.waitFor(30, TimeUnit.SECONDS)).thenReturn(true);
        reader.stop();
        reader.stop();
        verify(process).destroy();
        verify(process, never()).destroyForcibly();
    }

    @Test
    void unresponsiveHelperIsKilledAfterGracePeriod() {
        OceanbaseNativeReader reader = reader();
        Process process = process(reader);
        reader.stop();
        verify(process).destroy();
        verify(process).destroyForcibly();
    }

    @Test
    void interruptedStopPreservesCancellationAndKillsHelper() throws Exception {
        OceanbaseNativeReader reader = reader();
        Process process = process(reader);
        when(process.waitFor(30, TimeUnit.SECONDS)).thenThrow(new InterruptedException("stop canceled"));
        try {
            reader.stop();
            assertTrue(Thread.currentThread().isInterrupted());
            verify(process).destroyForcibly();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void simultaneousStopsCannotDestroyTheSameHelperTwice() throws Exception {
        OceanbaseNativeReader reader = reader();
        Process process = process(reader);
        when(process.waitFor(30, TimeUnit.SECONDS)).thenReturn(true);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Callable<Void> stop = () -> { start.await(); reader.stop(); return null; };
            Future<Void> first = pool.submit(stop);
            Future<Void> second = pool.submit(stop);
            start.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            verify(process).destroy();
            verify(process).waitFor(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void unexpectedExitStillReportsTheOwnedHelperExitCode() throws Exception {
        OceanbaseNativeReader reader = reader();
        Process process = process(reader);
        when(process.waitFor(3, TimeUnit.SECONDS)).thenReturn(true);
        when(process.exitValue()).thenReturn(7);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> ReflectionTestUtils.invokeMethod(reader, "checkUnexpectedExit",
                        (io.netty.util.BooleanSupplier) () -> true));
        assertTrue(failure.getMessage().contains("exit code 7"));
    }

    private OceanbaseNativeReader reader() {
        return new OceanbaseNativeReader(new OceanbaseConfig(), "sonar-regression", mock(Log.class));
    }

    @SuppressWarnings("unchecked")
    private Process process(OceanbaseNativeReader reader) {
        Process process = mock(Process.class);
        ((AtomicReference<Process>) ReflectionTestUtils.getField(reader, "process")).set(process);
        return process;
    }
}
