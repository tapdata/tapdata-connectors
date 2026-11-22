package io.tapdata.connector.tidb.cdc.util;

import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProcessLauncherCancellationTest {
    @Test
    @EnabledOnOs(OS.LINUX)
    void canceledCommandWaitRetainsInterrupt() {
        Thread.currentThread().interrupt();
        try {
            assertEquals("", ProcessLauncher.execCmdWaitResult("/bin/sleep 0.2", "test command failed: {}", mock(Log.class)));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void launchFailureDoesNotCreateAnInterrupt() {
        assertEquals("", ProcessLauncher.execCmdWaitResult("/nonexistent-tapdata-test-executable", "test command failed: {}", mock(Log.class)));
        assertFalse(Thread.currentThread().isInterrupted());
    }
}
