package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.TimeZone;
import java.util.LinkedHashMap;
import java.util.Map;

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

    @Test
    void shouldInsertAddedFieldAtItsPhysicalColumnPosition() {
        @SuppressWarnings("unchecked")
        Map<String, LinkedHashMap<String, String>> types = (Map<String, LinkedHashMap<String, String>>) ReflectionTestUtils.getField(mysqlReader, "dataTypeMap");
        LinkedHashMap<String, String> initial = new LinkedHashMap<>();
        initial.put("id", "int");
        initial.put("before_col", "varchar");
        initial.put("after_col", "varchar");
        types.put("orders", initial);

        TapNewFieldEvent ddl = new TapNewFieldEvent();
        ddl.setTableId("orders");
        ddl.setOriginDDL("ALTER TABLE orders ADD COLUMN inserted_col varchar AFTER id");
        ddl.field(new TapField("inserted_col", "varchar").pos(2));
        ReflectionTestUtils.invokeMethod(mysqlReader, "applyDdlToCachedSchema", ddl, "test_db");

        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("id", "inserted_col", "before_col", "after_col"),
                new java.util.ArrayList<>(types.get("orders").keySet()));
    }

    @Test
    void shouldUpdateSchemaQualifiedCacheForDdlWithUnqualifiedTableId() {
        ReflectionTestUtils.setField(mysqlReader, "withSchema", true);
        @SuppressWarnings("unchecked")
        Map<String, LinkedHashMap<String, String>> types = (Map<String, LinkedHashMap<String, String>>) ReflectionTestUtils.getField(mysqlReader, "dataTypeMap");
        LinkedHashMap<String, String> initial = new LinkedHashMap<>();
        initial.put("id", "int");
        initial.put("tail_col", "varchar");
        types.put("test_db.orders", initial);

        TapNewFieldEvent ddl = new TapNewFieldEvent();
        ddl.setTableId("orders");
        ddl.setOriginDDL("ALTER TABLE orders ADD COLUMN first_col varchar FIRST");
        ddl.field(new TapField("first_col", "varchar").pos(1));
        ReflectionTestUtils.invokeMethod(mysqlReader, "applyDdlToCachedSchema", ddl, "test_db");

        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("first_col", "id", "tail_col"),
                new java.util.ArrayList<>(types.get("test_db.orders").keySet()));
        org.junit.jupiter.api.Assertions.assertEquals(1, types.size());
        org.junit.jupiter.api.Assertions.assertNull(types.get("orders"));
    }
}
