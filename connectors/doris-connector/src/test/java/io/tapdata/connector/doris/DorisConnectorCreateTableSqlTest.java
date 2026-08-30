package io.tapdata.connector.doris;

import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DorisConnector#buildCreateTableSql(TapTable)}.
 * DorisConfig is a plain POJO, TapTable is built with TapField directly,
 * no mock or real Doris instance needed.
 */
public class DorisConnectorCreateTableSqlTest {

    private DorisConnector connector;
    private DorisConfig config;

    @BeforeEach
    void setUp() throws Exception {
        connector = new DorisConnector();
        config = new DorisConfig();
        config.setBucket(2);
        config.setUniqueKeyType("Unique");
        config.setDuplicateKey(new ArrayList<>());
        config.setDistributedKey(new ArrayList<>());
        config.setTableProperties(new ArrayList<>());
        config.setSchema("test_db");
        setField(connector, "dorisConfig", config);
        setField(connector, "commonDbConfig", config);
        setField(connector, "commonSqlMaker", new DorisSqlMaker());
    }

    @DisplayName("empty tableProperties should omit the whole PROPERTIES() clause")
    @Test
    void shouldOmitPropertiesClauseWhenTablePropertiesIsEmpty() {
        config.setTableProperties(new ArrayList<>());

        String sql = connector.buildCreateTableSql(simpleTable("test_table"));

        assertTrue(sql.endsWith("BUCKETS 2"), "sql should end with BUCKETS clause, but was: " + sql);
        assertFalse(sql.contains("PROPERTIES("), "sql should not contain PROPERTIES(), but was: " + sql);
    }

    @DisplayName("non-empty tableProperties should keep the PROPERTIES() clause as before")
    @Test
    void shouldKeepPropertiesClauseWhenTablePropertiesIsNotEmpty() {
        config.setTableProperties(properties(
                property("replication_num", "3")));

        String sql = connector.buildCreateTableSql(simpleTable("test_table"));

        assertTrue(sql.contains("PROPERTIES(\"replication_num\"=\"3\")"), "sql should contain PROPERTIES clause, but was: " + sql);
    }

    @DisplayName("multiple tableProperties should be joined with comma as before")
    @Test
    void shouldJoinMultipleTablePropertiesWithComma() {
        config.setTableProperties(properties(
                property("replication_num", "3"),
                property("storage_medium", "SSD")));

        String sql = connector.buildCreateTableSql(simpleTable("test_table"));

        assertTrue(sql.contains("PROPERTIES(\"replication_num\"=\"3\", \"storage_medium\"=\"SSD\")"), "sql should contain joined PROPERTIES clause, but was: " + sql);
    }

    private TapTable simpleTable(String tableName) {
        TapTable tapTable = new TapTable(tableName);
        TapField field = new TapField("id", "VARCHAR");
        field.setDataType("VARCHAR");
        field.setPrimaryKeyPos(1);
        tapTable.add(field);
        return tapTable;
    }

    private LinkedHashMap<String, String> property(String key, String value) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("propKey", key);
        map.put("propValue", value);
        return map;
    }

    private List<LinkedHashMap<String, String>> properties(LinkedHashMap<String, String>... entries) {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        java.util.Collections.addAll(list, entries);
        return list;
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException("field not found: " + fieldName);
    }
}
