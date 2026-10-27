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
        try (Connection connection = connections.get(); Statement statement = connection.createStatement()) {
            if (timeoutSeconds > 0) {
                try { statement.setQueryTimeout(timeoutSeconds); }
                catch (SQLFeatureNotSupportedException unsupported) {
                    throw new SQLFeatureNotSupportedException(
                            "JDBC driver cannot enforce queryTimeoutSeconds; omit it or set 0 to explicitly run without a timeout", unsupported);
                }
            }
            if (fetchSize != null) { statement.setFetchSize(fetchSize); }
            boolean hasResult = statement.execute(sql);
            if (!hasResult) {
                consumer.accept((long) statement.getUpdateCount());
            } else {
                while (alive.get() && hasResult) {
                    try (ResultSet resultSet = statement.getResultSet()) {
                        RowReader reader = readers.create(resultSet);
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
                }
            }
            if (!connection.getAutoCommit()) connection.commit();
        }
    }
}
