package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MysqlReaderV2Test {

    private MysqlReaderV2 mysqlReader;
    private KVReadOnlyMap<TapTable> tableMap;

    @BeforeEach
    void setUp() throws Throwable {
        MysqlJdbcContextV2 jdbcContext = mock(MysqlJdbcContextV2.class);
        MysqlConfig mysqlConfig = mock(MysqlConfig.class);
        when(jdbcContext.getConfig()).thenReturn(mysqlConfig);
        mysqlReader = new MysqlReaderV2(jdbcContext, mock(Log.class), TimeZone.getDefault());
        tableMap = mock(KVReadOnlyMap.class);
        mysqlReader.init(null, tableMap, null, 1, null);
    }

    @Test
    void shouldSkipMetadataRefreshWhenTableIsUnavailable() {
        when(tableMap.get("missing_table")).thenReturn(null);

        assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(mysqlReader, "ddlFlush", "missing_table"));
    }

    @Test
    void shouldSkipMetadataRefreshWhenFieldsAreUnavailable() {
        TapTable tapTable = mock(TapTable.class);
        when(tableMap.get("empty_table")).thenReturn(tapTable);
        when(tapTable.getNameFieldMap()).thenReturn(null);

        assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(mysqlReader, "ddlFlush", "empty_table"));
    }
}
