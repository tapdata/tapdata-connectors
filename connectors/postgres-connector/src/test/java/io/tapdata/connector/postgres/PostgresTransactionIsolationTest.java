package io.tapdata.connector.postgres;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresTransactionIsolationTest {
    private PostgresConnector connector;
    private TapConnectorContext context;
    private Connection connection;
    private PostgresJdbcContext jdbc;
    private final TapTable table = new TapTable("business");
    private final TapTable cacheTable = new TapTable("exactly_once_cache");

    @BeforeEach
    void setUp() throws Exception {
        connector = spy(new PostgresConnector());
        context = mock(TapConnectorContext.class);
        when(context.getConnectorCapabilities()).thenReturn(mock(ConnectorCapabilities.class));
        jdbc = mock(PostgresJdbcContext.class);
        connection = mock(Connection.class);
        when(jdbc.getConnection()).thenReturn(connection);
        ReflectionTestUtils.setField(connector, "postgresJdbcContext", jdbc);
        PostgresConfig config = mock(PostgresConfig.class);
        when(config.getCreateAutoInc()).thenReturn(false);
        ReflectionTestUtils.setField(connector, "postgresConfig", config);
        Map<String, DataMap> written = new HashMap<>();
        written.put(table.getId(), DataMap.create());
        written.put(cacheTable.getId(), DataMap.create());
        ReflectionTestUtils.setField(connector, "writtenTableMap", written);
        doNothing().when(connector).beforeWriteRecord(any());
        ReflectionTestUtils.setField(connector, "postgresVersion", "150000");
    }

    @Test
    void anotherWorkersCommitMustNotEndCurrentTransaction() throws Throwable {
        assertOtherWorkerCannotEndTransaction(true);
    }

    @Test
    void anotherWorkersRollbackMustNotEndCurrentTransaction() throws Throwable {
        assertOtherWorkerCannotEndTransaction(false);
    }

    private void assertOtherWorkerCannotEndTransaction(boolean commit) throws Throwable {
        transaction("beginTransaction");
        ExecutorService other = Executors.newSingleThreadExecutor();
        List<List<?>> constructors = new ArrayList<>();
        try (MockedConstruction<PostgresRecordWriter> ignored = writers(constructors)) {
            writeOnce(table);
            other.submit(() -> {
                try {
                    transaction("beginTransaction");
                    transaction(commit ? "commitTransaction" : "rollbackTransaction");
                } catch (Throwable error) {
                    throw new AssertionError(error);
                }
            }).get(5, TimeUnit.SECONDS);
            writeOnce(cacheTable);
            assertEquals(2, constructors.size());
            for (List<?> args : constructors) {
                assertEquals(4, args.size(), "Business and cache writes must share the worker transaction");
                assertSame(connection, args.get(1));
            }
            verify(jdbc, times(1)).getConnection();
            transaction("commitTransaction");
            verify(connection).commit();
            verify(connection).close();
        } finally {
            other.shutdownNow();
        }
    }

    @Test
    void closeFailureMustStillClearCurrentTransaction() throws Throwable {
        transaction("beginTransaction");
        List<List<?>> constructors = new ArrayList<>();
        try (MockedConstruction<PostgresRecordWriter> ignored = writers(constructors)) {
            writeOnce(table);
            doThrow(new SQLException("close failed")).when(connection).close();
            assertThrows(SQLException.class, () -> transaction("commitTransaction"));
            writeOnce(table);
            assertEquals(4 - 1, constructors.get(1).size());
        }
    }

    @Test
    void commitFailureMustPreserveCloseFailureAndClearTransaction() throws Throwable {
        transaction("beginTransaction");
        List<List<?>> constructors = new ArrayList<>();
        try (MockedConstruction<PostgresRecordWriter> ignored = writers(constructors)) {
            writeOnce(table);
            SQLException commitError = new SQLException("commit failed");
            SQLException closeError = new SQLException("close failed");
            doThrow(commitError).when(connection).commit();
            doThrow(closeError).when(connection).close();
            SQLException actual = assertThrows(SQLException.class, () -> transaction("commitTransaction"));
            assertSame(commitError, actual);
            assertArrayEquals(new Throwable[]{closeError}, actual.getSuppressed());
            writeOnce(table);
            assertEquals(4 - 1, constructors.get(1).size());
        }
    }

    @Test
    void rollbackFailureMustReleaseConnectionAndClearTransaction() throws Throwable {
        transaction("beginTransaction");
        List<List<?>> constructors = new ArrayList<>();
        try (MockedConstruction<PostgresRecordWriter> ignored = writers(constructors)) {
            writeOnce(table);
            SQLException rollbackError = new SQLException("rollback failed");
            doThrow(rollbackError).when(connection).rollback();
            assertSame(rollbackError, assertThrows(SQLException.class, () -> transaction("rollbackTransaction")));
            verify(connection).close();
            writeOnce(table);
            assertEquals(4 - 1, constructors.get(1).size());
        }
    }

    @Test
    void emptyTransactionMustNotBorrowConnection() throws Throwable {
        transaction("beginTransaction");
        transaction("commitTransaction");
        transaction("beginTransaction");
        transaction("rollbackTransaction");
        verifyNoInteractions(jdbc, connection);
        List<List<?>> constructors = new ArrayList<>();
        try (MockedConstruction<PostgresRecordWriter> ignored = writers(constructors)) {
            writeOnce(table);
            assertEquals(4 - 1, constructors.get(0).size());
        }
    }

    @Test
    void legacySubclassFlagMustKeepItsExistingLifecycle() throws Throwable {
        transaction("beginTransaction");
        assertEquals(true, ReflectionTestUtils.getField(connector, "isTransaction"));
        transaction("commitTransaction");
        assertEquals(false, ReflectionTestUtils.getField(connector, "isTransaction"));
        transaction("beginTransaction");
        assertEquals(true, ReflectionTestUtils.getField(connector, "isTransaction"));
        transaction("rollbackTransaction");
        assertEquals(false, ReflectionTestUtils.getField(connector, "isTransaction"));
    }

    @Test
    void rollbackMustReleaseOwnConnectionAndLeaveOrdinaryWrites() throws Throwable {
        transaction("beginTransaction");
        List<List<?>> constructors = new ArrayList<>();
        try (MockedConstruction<PostgresRecordWriter> ignored = writers(constructors)) {
            writeOnce(table);
            transaction("rollbackTransaction");
            verify(connection).rollback();
            verify(connection).close();
            assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(connector, "transactionConnectionMap")).isEmpty());
            writeOnce(table);
            assertEquals(4 - 1, constructors.get(1).size());
        }
    }

    private MockedConstruction<PostgresRecordWriter> writers(List<List<?>> constructors) {
        return mockConstruction(PostgresRecordWriter.class, withSettings().defaultAnswer(RETURNS_SELF),
                (writer, creation) -> constructors.add(creation.arguments()));
    }

    private void transaction(String action) throws Throwable {
        Method method = CommonDbConnector.class.getDeclaredMethod(action, TapConnectorContext.class);
        invoke(method, context);
    }

    private void writeOnce(TapTable target) throws Throwable {
        Method method = PostgresConnector.class.getDeclaredMethod("writeRecord", TapConnectorContext.class, List.class, TapTable.class, Consumer.class);
        invoke(method, context, Collections.singletonList(new TapInsertRecordEvent()), target, (Consumer<Object>) result -> { });
    }

    private void invoke(Method method, Object... args) throws Throwable {
        method.setAccessible(true);
        try {
            method.invoke(connector, args);
        } catch (InvocationTargetException error) {
            throw error.getCause();
        }
    }
}
