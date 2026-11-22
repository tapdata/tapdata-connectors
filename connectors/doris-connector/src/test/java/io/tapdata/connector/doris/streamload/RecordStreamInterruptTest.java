package io.tapdata.connector.doris.streamload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

class RecordStreamInterruptTest {
    @ParameterizedTest
    @ValueSource(strings = {"read", "readArray", "readRange", "write"})
    void interruptedIoRetainsCancellationSignal(String operation) {
        RecordStream stream = new RecordStream(8, 2);
        Thread.currentThread().interrupt();
        try {
            RuntimeException failure = assertThrows(RuntimeException.class, () -> {
                switch (operation) {
                    case "read": stream.read(); break;
                    case "readArray": stream.read(new byte[1]); break;
                    case "readRange": stream.read(new byte[1], 0, 1); break;
                    case "write": stream.write(new byte[1]); break;
                    default: fail("Unknown operation");
                }
            });
            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void interruptedEndInputRetainsCancellationSignal() {
        RecordStream stream = new RecordStream(8, 2);
        Thread.currentThread().interrupt();
        try {
            IOException failure = assertThrows(IOException.class, stream::endInput);
            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
