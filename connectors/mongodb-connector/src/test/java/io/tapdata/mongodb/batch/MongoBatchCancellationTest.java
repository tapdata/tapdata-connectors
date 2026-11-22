package io.tapdata.mongodb.batch;

import io.tapdata.mongodb.MongodbExceptionCollector;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MongoBatchCancellationTest {
    @Test
    void cancellationIsRestoredBeforeTheOriginalFailureReachesHandlers() {
        MongoBatchReader reader = reader();
        InterruptedException failure = new InterruptedException("batch canceled");
        reader.errorHandler = cause -> {
            assertSame(failure, cause);
            assertTrue(Thread.currentThread().isInterrupted());
        };
        try {
            reader.doException(failure);
            assertTrue(Thread.currentThread().isInterrupted());
            verify(reader.exceptionCollector).collectTerminateByServer(failure);
            verify(reader.exceptionCollector).revealException(failure);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void ordinaryFailuresDoNotMarkTheThreadCanceled() {
        MongoBatchReader reader = reader();
        ErrorHandler handler = mock(ErrorHandler.class);
        reader.errorHandler = handler;
        IllegalStateException failure = new IllegalStateException("decode failed");
        reader.doException(failure);
        assertFalse(Thread.currentThread().isInterrupted());
        verify(handler).doHandle(failure);
    }

    private MongoBatchReader reader() {
        MongoBatchReader reader = mock(MongoBatchReader.class, CALLS_REAL_METHODS);
        reader.checkAlive = () -> true;
        reader.exceptionCollector = mock(MongodbExceptionCollector.class);
        return reader;
    }
}
