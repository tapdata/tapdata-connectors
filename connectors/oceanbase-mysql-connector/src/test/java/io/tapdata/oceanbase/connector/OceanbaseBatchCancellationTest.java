package io.tapdata.oceanbase.connector;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import java.sql.ResultSet;
import io.tapdata.common.entity.HashReadOffset;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OceanbaseBatchCancellationTest {
    static class TestConnector extends OceanbaseConnector {
        @Override
        protected String getBatchReadSelectSql(TapTable table) { return "select 1"; }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void interruptedWaitKeepsCancellationForBothPartitionStrategies(boolean partition) throws Exception {
        TestConnector connector = mock(TestConnector.class, CALLS_REAL_METHODS);
        CommonDbConfig config = mock(CommonDbConfig.class);
        when(config.getBatchReadThreadSize()).thenReturn(1);
        ReflectionTestUtils.setField(connector, "commonDbConfig", config);
        doReturn(mock(HashReadOffset.class)).when(connector).resolveHashReadOffset(null);
        MysqlJdbcContextV2 jdbc = mock(MysqlJdbcContextV2.class);
        ReflectionTestUtils.setField(connector, "mysqlJdbcContext", jdbc);
        doAnswer(call -> {
            ResultSet result = mock(ResultSet.class);
            when(result.next()).thenReturn(true, false);
            when(result.getString(2)).thenReturn("p0");
            ((ResultSetConsumer) call.getArgument(1)).accept(result);
            return null;
        }).when(jdbc).queryWithTimeout(anyString(), any(), any());
        ExecutorService pool = mock(ExecutorService.class);
        try (MockedStatic<Executors> executors = mockStatic(Executors.class)) {
            executors.when(() -> Executors.newFixedThreadPool(1)).thenReturn(pool);
            Thread.currentThread().interrupt();
            if (partition) {
                assertDoesNotThrow(() -> connector.batchReadWithPartition(mock(TapConnectorContext.class),
                        mock(TapTable.class), null, 10, (events, offset) -> {}));
            } else {
                RuntimeException failure = assertThrows(RuntimeException.class, () -> connector.batchReadWithHashSplit(
                        mock(TapConnectorContext.class), mock(TapTable.class), null, 10, (events, offset) -> {}));
                assertInstanceOf(InterruptedException.class, failure.getCause());
            }
            assertTrue(Thread.currentThread().isInterrupted());
            verify(pool).shutdown();
        } finally {
            Thread.interrupted();
        }
    }
}
