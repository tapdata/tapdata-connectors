package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.util.Clock;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MariaDbSnapshotOffsetTest {
    @Test
    void mariaDbVersionsKeepMasterStatus() throws Exception {
        assertQuery("MySQL", "12.0.2-MariaDB", 12, 0, "SHOW MASTER STATUS");
        assertQuery("MariaDB", "12.0.2", 12, 0, "SHOW MASTER STATUS");
        assertQuery("MySQL", "10.11.14-MariaDB", 10, 11, "SHOW MASTER STATUS");
    }

    @Test
    void overwrittenClassPreservesSharedMysqlVersionSelection() throws Exception {
        assertQuery("MySQL", "8.0.42", 8, 0, "SHOW MASTER STATUS");
        assertQuery("MySQL", "8.3.0", 8, 3, "SHOW MASTER STATUS");
        assertQuery("MySQL", "8.4.0", 8, 4, "SHOW BINARY LOG STATUS");
        assertQuery("MySQL", "9.0.0", 9, 0, "SHOW BINARY LOG STATUS");
    }

    @Test
    void sharedSnapshotSourceInitializesWithoutPreviousOffsetUsingOverwrittenClass() throws Exception {
        MySqlConnectorConfig config = new MySqlConnectorConfig(Configuration.create()
                .with("database.server.name", "mariadb-snapshot-regression")
                .with("snapshot.locking.mode", "none").build());
        MySqlConnection connection = mock(MySqlConnection.class);
        Connection jdbc = mock(Connection.class);
        when(connection.connection()).thenReturn(jdbc);
        DatabaseMetaData metadata = metadata("MySQL", "12.0.2-MariaDB", 12, 0);
        when(jdbc.getMetaData()).thenReturn(metadata);
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData columns = mock(ResultSetMetaData.class);
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn("mariadb-bin.000007");
        when(rs.getLong(2)).thenReturn(1234L);
        when(rs.getMetaData()).thenReturn(columns);
        when(columns.getColumnCount()).thenReturn(4);
        doAnswer(invocation -> {
            invocation.<JdbcConnection.ResultSetConsumer>getArgument(1).accept(rs);
            return connection;
        }).when(connection).query(eq("SHOW MASTER STATUS"), any(JdbcConnection.ResultSetConsumer.class));

        MySqlSnapshotChangeEventSource source = new MySqlSnapshotChangeEventSource(
                config, null, connection, null, null, Clock.SYSTEM, null, null);
        RelationalSnapshotContext context = new RelationalSnapshotContext("");
        // Executes the shared bytecode's invokestatic against MariaDB's overwritten class.
        source.determineSnapshotOffset(context);

        verify(connection).query(eq("SHOW MASTER STATUS"), any(JdbcConnection.ResultSetConsumer.class));
        assertEquals("mariadb-bin.000007", context.offset.getOffset().get("file"));
        assertEquals(1234L, context.offset.getOffset().get("pos"));
        assertTrue(context.offset.isSnapshotRunning());
    }

    private static void assertQuery(String product, String version, int major, int minor, String sql) throws Exception {
        assertEquals(sql, MySqlConnection.binlogStatusQuery(metadata(product, version, major, minor)));
    }

    private static DatabaseMetaData metadata(String product, String version, int major, int minor) throws Exception {
        DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        when(metadata.getDatabaseProductName()).thenReturn(product);
        when(metadata.getDatabaseProductVersion()).thenReturn(version);
        when(metadata.getDatabaseMajorVersion()).thenReturn(major);
        when(metadata.getDatabaseMinorVersion()).thenReturn(minor);
        return metadata;
    }
}
