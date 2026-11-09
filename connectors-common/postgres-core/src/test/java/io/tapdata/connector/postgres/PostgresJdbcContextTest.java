package io.tapdata.connector.postgres;

import io.tapdata.common.ResultSetConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import io.tapdata.connector.postgres.config.PostgresConfig;
import org.junit.jupiter.api.DisplayName;

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
}
