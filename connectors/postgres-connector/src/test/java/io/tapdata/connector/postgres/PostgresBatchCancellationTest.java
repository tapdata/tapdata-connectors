package io.tapdata.connector.postgres;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.entity.HashReadOffset;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresBatchCancellationTest {
    static class TestConnector extends PostgresConnector {
        @Override
        protected String getBatchReadSelectSql(TapTable table) { return "select 1"; }
    }

    @Test
    void cancelingPartitionWaitPreservesInterruptAndReleasesExecutor() {
        TestConnector connector = mock(TestConnector.class, CALLS_REAL_METHODS);
        CommonDbConfig config = mock(CommonDbConfig.class);
        when(config.getBatchReadThreadSize()).thenReturn(1);
        ReflectionTestUtils.setField(connector, "commonDbConfig", config);
        doReturn(mock(HashReadOffset.class)).when(connector).resolveHashReadOffset(null);
        ExecutorService pool = mock(ExecutorService.class);
        try (MockedStatic<Executors> executors = mockStatic(Executors.class)) {
            executors.when(() -> Executors.newFixedThreadPool(1)).thenReturn(pool);
            Thread.currentThread().interrupt();
            RuntimeException failure = assertThrows(RuntimeException.class, () -> connector.batchReadWithHashSplit(
                    mock(TapConnectorContext.class), mock(TapTable.class), null, 10, (events, offset) -> {}));
            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            verify(pool).shutdown();
        } finally {
            Thread.interrupted();
        }
    }
}
