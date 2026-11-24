package io.tapdata.common;

import io.tapdata.pdk.apis.functions.TapSupplier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Owns and closes the supplied connection, committing only non-autocommit connections on success.
 * Callers retain their driver-specific fetch and row conversion. */
public final class JdbcQueryExecutor {
    private JdbcQueryExecutor() { }

    @FunctionalInterface
    public interface RowReader {
        Map<String, Object> read() throws SQLException;
    }

    @FunctionalInterface
    public interface RowReaderFactory {
        RowReader create(ResultSet resultSet) throws SQLException;
    }

    public static void execute(String sql, TapSupplier<Connection> connections, Consumer<Object> consumer,
                               Supplier<Boolean> alive, int batchSize, int timeoutSeconds, Integer fetchSize,
                               RowReaderFactory readers) throws Throwable {
        try (Connection connection = connections.get()) {
            boolean inspect = InspectQueryScope.active();
            if (inspect) {
                connection.setReadOnly(true);
                if (connection.getAutoCommit()) connection.setAutoCommit(false);
            }
            try (Statement statement = connection.createStatement()) {
                if (timeoutSeconds > 0) {
                    try { statement.setQueryTimeout(timeoutSeconds); }
                    catch (SQLFeatureNotSupportedException unsupported) {
                        throw new SQLFeatureNotSupportedException(
                                "JDBC driver cannot enforce queryTimeoutSeconds; omit it or set 0 to explicitly run without a timeout", unsupported);
                    }
                }
                if (fetchSize != null) { statement.setFetchSize(fetchSize); }
                InspectQueryScope.register(statement, connection);
                boolean hasResult = statement.execute(sql);
                if (InspectQueryScope.active() && !hasResult) throw new SQLException("Inspection requires one result set, not an update count");
                int resultSets = 0;
                if (!hasResult) {
                    consumer.accept((long) statement.getUpdateCount());
                } else {
                    while (alive.get() && hasResult) {
                        try (ResultSet resultSet = statement.getResultSet()) {
                            if (InspectQueryScope.active() && ++resultSets > 1) throw new SQLException("Inspection requires exactly one result set");
                            InspectQueryScope.columns(resultSet);
                            RowReader reader;
                            if (InspectQueryScope.active()) {
                                List<String> columns = InspectQueryScope.rowColumns(resultSet);
                                reader = () -> InspectJdbcValues.read(resultSet, columns);
                            } else {
                                reader = readers.create(resultSet);
                            }
                            List<Map<String, Object>> batch = new ArrayList<>();
                            while (alive.get() && resultSet.next()) {
                                batch.add(reader.read());
                                if (batch.size() == batchSize) {
                                    consumer.accept(batch);
                                    batch = new ArrayList<>();
                                }
                            }
                            if (!batch.isEmpty()) { consumer.accept(batch); }
                        }
                        hasResult = statement.getMoreResults();
                        if (InspectQueryScope.active() && !hasResult && statement.getUpdateCount() != -1) throw new SQLException("Unexpected update count after query");
                    }
                }
                if (InspectQueryScope.active() && !alive.get()) throw new SQLException("Inspection query was cancelled before completion");
                if (!inspect && !connection.getAutoCommit()) connection.commit();
            } finally {
                if (inspect && !connection.getAutoCommit()) connection.rollback();
            }
        }
    }
}
