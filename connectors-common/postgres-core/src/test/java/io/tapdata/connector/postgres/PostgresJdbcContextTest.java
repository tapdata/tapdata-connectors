package io.tapdata.connector.postgres;

import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.postgres.config.PostgresConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.*;

public class PostgresJdbcContextTest {
    PostgresJdbcContext postgresJdbcContext;
    @BeforeEach
    void init(){
        postgresJdbcContext = mock(PostgresJdbcContext.class);
    }
    @Test
    void test_queryTimestamp() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        when(resultSet.getTimestamp(1)).thenReturn(timestamp);
        doAnswer(invocation -> {
            ResultSetConsumer resultSetConsumer = invocation.getArgument(1);
            resultSetConsumer.accept(resultSet);
            return null;
        }).when(postgresJdbcContext).queryWithNext(any(),any());
        doCallRealMethod().when(postgresJdbcContext).queryTimestamp();
        Assertions.assertEquals(timestamp.getTime(),postgresJdbcContext.queryTimestamp());
    }

    @Test
    void test_queryAllForeignKeysSql_readsColumnNamesFromPgAttribute() {
        // TAP-13224: information_schema.columns hides columns of tables the account cannot select,
        // so foreign key column names must come from pg_attribute.
        PostgresConfig config = new PostgresConfig();
        config.setSchema("sys_o'rule");
        when(postgresJdbcContext.getConfig()).thenReturn(config);
        when(postgresJdbcContext.queryAllForeignKeysSql(any(), any())).thenCallRealMethod();

        String sql = postgresJdbcContext.queryAllForeignKeysSql("ignored", Arrays.asList("child", "o'child"));

        Assertions.assertFalse(sql.contains("information_schema"));
        Assertions.assertTrue(sql.contains("(select attname from pg_attribute where attrelid=con.conrelid and attnum=con.fk) fk"));
        Assertions.assertTrue(sql.contains("(select attname from pg_attribute where attrelid=con.confrelid and attnum=con.rfk) rfk"));
        Assertions.assertTrue(sql.contains("c.conrelid,c.confrelid"));
        Assertions.assertTrue(sql.contains("pn.nspname='sys_o''rule'"));
        Assertions.assertTrue(sql.contains("and pc.relname in ('child','o''child')"));

        String allTables = postgresJdbcContext.queryAllForeignKeysSql("ignored", Collections.emptyList());
        Assertions.assertFalse(allTables.contains("pc.relname in"));
    }
}
