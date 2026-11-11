package io.tapdata.connector.postgres;

import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.postgres.config.PostgresConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import io.tapdata.connector.postgres.config.PostgresConfig;
import org.junit.jupiter.api.DisplayName;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.*;

public class PostgresJdbcContextTest {
    @Test
    @DisplayName("普通视图显式启用，两个版本分支保留默认查询且正确隔离schema和注释")
    void shouldOptInToViewsWithoutChangingDefaultQueries() {
        PostgresConfig config = new PostgresConfig();
        config.setDatabase("test'db");
        PostgresJdbcContext context = mock(PostgresJdbcContext.class, CALLS_REAL_METHODS);
        doReturn(config).when(context).getConfig();
        for (String version : List.of("90624", "140000")) {
            context.withPostgresVersion(version).withDiscoverViews(false);
            String original = context.queryAllTablesSql("schema'one", List.of("v'one"));
            Assertions.assertFalse(original.contains("'BASE TABLE', 'VIEW'"));
            context.withDiscoverViews(true);
            String sql = context.queryAllTablesSql("schema'one", List.of("v'one"));
            Assertions.assertTrue(sql.contains("'BASE TABLE', 'VIEW'"));
            Assertions.assertTrue(sql.contains("schema''one"));
            Assertions.assertTrue(sql.contains("v''one"));
            Assertions.assertTrue(sql.contains("obj_description(c.oid"));
            Assertions.assertTrue(sql.contains("nspname = t.table_schema"));
            Assertions.assertFalse(sql.contains("relfilenode"));
            Assertions.assertTrue(context.queryAllColumnsSql("schema'one", List.of("v'one"))
                    .contains("nspname = col.table_schema"));
            context.withDiscoverViews(false);
            Assertions.assertEquals(original, context.queryAllTablesSql("schema'one", List.of("v'one")));
        }
    }

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
