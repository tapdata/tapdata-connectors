package io.tapdata.oceanbase.connector;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.entity.logger.Log;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import io.tapdata.common.entity.HashReadOffset;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Timeout(15)
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
            RuntimeException failure = assertThrows(RuntimeException.class, () -> {
                if (partition) {
                    connector.batchReadWithPartition(mock(TapConnectorContext.class),
                            mock(TapTable.class), null, 10, (events, offset) -> {});
                } else {
                    connector.batchReadWithHashSplit(mock(TapConnectorContext.class),
                            mock(TapTable.class), null, 10, (events, offset) -> {});
                }
            });
            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            verify(pool).shutdown();
        } finally {
            Thread.interrupted();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void partitionWorkerFailurePropagatesThroughBothEntryPoints(boolean hashSplit) throws Exception {
        SQLException failure = new SQLException("partition read failed");
        verifyPartitionCompletion(hashSplit, failure);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void successfulPartitionReadClosesExecutorThroughBothEntryPoints(boolean hashSplit) throws Exception {
        verifyPartitionCompletion(hashSplit, null);
    }

    private void verifyPartitionCompletion(boolean hashSplit, SQLException workerFailure) throws Exception {
        TestConnector connector = mock(TestConnector.class, CALLS_REAL_METHODS);
        CommonDbConfig config = mock(CommonDbConfig.class);
        when(config.getBatchReadThreadSize()).thenReturn(1);
        ReflectionTestUtils.setField(connector, "commonDbConfig", config);
        ReflectionTestUtils.setField(connector, "tapLogger", mock(Log.class));
        ReflectionTestUtils.setField(connector, "exceptionCollector", mock(ExceptionCollector.class));
        doReturn(true).when(connector).isAlive();
        MysqlJdbcContextV2 jdbc = mock(MysqlJdbcContextV2.class);
        ReflectionTestUtils.setField(connector, "mysqlJdbcContext", jdbc);
        doAnswer(call -> {
            ResultSet result = mock(ResultSet.class);
            when(result.next()).thenReturn(true, true, true, true, false);
            when(result.getString(2)).thenReturn("p0", "p1", "p2", "p3");
            ((ResultSetConsumer) call.getArgument(1)).accept(result);
            return null;
        }).when(jdbc).queryWithTimeout(anyString(), any(), any());
        doAnswer(call -> {
            if (workerFailure != null) {
                throw workerFailure;
            }
            ResultSet result = mock(ResultSet.class);
            when(result.getMetaData()).thenReturn(mock(ResultSetMetaData.class));
            ((ResultSetConsumer) call.getArgument(1)).accept(result);
            return null;
        }).when(jdbc).streamQueryWithTimeout(anyString(), any(), any(), anyInt());
        ExecutorService pool = mock(ExecutorService.class);
        doAnswer(call -> {
            FutureTask<Void> task = new FutureTask<>(() -> {
                call.getArgument(0, Runnable.class).run();
                return null;
            });
            task.run();
            return task;
        }).when(pool).submit(any(Runnable.class));
        try (MockedStatic<Executors> executors = mockStatic(Executors.class)) {
            executors.when(() -> Executors.newFixedThreadPool(1)).thenReturn(pool);
            org.junit.jupiter.api.function.Executable read = () -> {
                if (hashSplit) {
                    connector.batchReadWithHashSplit(mock(TapConnectorContext.class),
                            mock(TapTable.class), null, 10, (events, offset) -> {});
                } else {
                    connector.batchReadWithPartition(mock(TapConnectorContext.class),
                            mock(TapTable.class), null, 10, (events, offset) -> {});
                }
            };
            if (workerFailure != null) {
                assertSame(workerFailure, assertThrows(SQLException.class, read));
            } else {
                assertDoesNotThrow(read);
            }
            verify(jdbc, times(workerFailure == null ? 4 : 1))
                    .streamQueryWithTimeout(anyString(), any(), any(), anyInt());
            verify(pool).shutdown();
        }
    }

}
