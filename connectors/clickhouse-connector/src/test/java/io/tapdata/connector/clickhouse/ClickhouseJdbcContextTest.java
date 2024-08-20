package io.tapdata.connector.clickhouse;

import io.tapdata.common.ResultSetConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doCallRealMethod;

public class ClickhouseJdbcContextTest {
    ClickhouseJdbcContext clickhouseJdbcContext;
    @BeforeEach
    void init(){
        clickhouseJdbcContext = mock(ClickhouseJdbcContext.class);
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
        }).when(clickhouseJdbcContext).queryWithNext(any(),any());
        doCallRealMethod().when(clickhouseJdbcContext).queryTimestamp();
        Assertions.assertEquals(timestamp.getTime(),clickhouseJdbcContext.queryTimestamp());
    }
}
