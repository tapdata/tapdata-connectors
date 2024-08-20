package io.tapdata.connector.postgres;

import io.tapdata.common.ResultSetConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

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
}
