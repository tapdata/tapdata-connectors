package io.tapdata.connector.mysql;

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

class MysqlBatchCancellationTest {
    @Test
    void cancelingPartitionWaitPreservesInterruptAndReleasesExecutor() {
        MysqlConnector connector = mock(MysqlConnector.class, CALLS_REAL_METHODS);
        CommonDbConfig config = mock(CommonDbConfig.class);
        when(config.getBatchReadThreadSize()).thenReturn(1);
        ReflectionTestUtils.setField(connector, "commonDbConfig", config);
        doReturn("select 1").when(connector).getBatchReadSelectSql(any());
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
