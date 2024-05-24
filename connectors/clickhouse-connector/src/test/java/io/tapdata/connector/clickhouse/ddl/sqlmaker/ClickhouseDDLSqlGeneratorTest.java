package io.tapdata.connector.clickhouse.ddl.sqlmaker;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class ClickhouseDDLSqlGeneratorTest {
    private ClickhouseDDLSqlGenerator generator;
    private CommonDbConfig config;
    private TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent;
    private TapAlterFieldNameEvent tapAlterFieldNameEvent;
    private TapDropFieldEvent tapDropFieldEvent;

    @BeforeEach
    void beforeEach() {
        generator = new ClickhouseDDLSqlGenerator();
        config = Mockito.mock(CommonDbConfig.class);
        tapAlterFieldAttributesEvent = Mockito.mock(TapAlterFieldAttributesEvent.class);
        tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        tapDropFieldEvent = new TapDropFieldEvent();
    }

    @Test
     void testAlterColumnAttr() {
        String database = "test_db";
        String tableId = "test_table";
        String fieldName = "test_field";
        String dataType = "Int32";
        boolean nullable = true;
        String comment = "Test column";
        int primary = 1;

        Mockito.when(config.getDatabase()).thenReturn(database);
        Mockito.when(tapAlterFieldAttributesEvent.getTableId()).thenReturn(tableId);
        Mockito.when(tapAlterFieldAttributesEvent.getFieldName()).thenReturn(fieldName);
        ValueChange<String> dataTypeChange = new ValueChange<>("Int32", "Int32");
        Mockito.when(tapAlterFieldAttributesEvent.getDataTypeChange()).thenReturn(dataTypeChange);
        ValueChange<Boolean> nullableChange = new ValueChange<>(nullable, nullable);
        Mockito.when(tapAlterFieldAttributesEvent.getNullableChange()).thenReturn(nullableChange);
        ValueChange<String> commentChange = new ValueChange<>("", comment);
        Mockito.when(tapAlterFieldAttributesEvent.getCommentChange()).thenReturn(commentChange);
        ValueChange<Integer> primaryChange = new ValueChange<>(null, primary);
        Mockito.when(tapAlterFieldAttributesEvent.getPrimaryChange()).thenReturn(primaryChange);

        List<String> sqlList = generator.alterColumnAttr(config, tapAlterFieldAttributesEvent);

        Assertions.assertNotNull(sqlList);
        Assertions.assertTrue(sqlList.size() > 0);
        String expectedSql = String.format("alter table \"%s\".\"%s\" modify column `%s` Nullable(Int32) comment '%s' key", database, tableId, fieldName, comment);
        Assertions.assertEquals(expectedSql, sqlList.get(0));
    }

    @Test
    void testAlterColumnAttrWithBlankTableName() {
        Mockito.when(tapAlterFieldAttributesEvent.getTableId()).thenReturn("");

        Assertions.assertThrows(RuntimeException.class, () -> generator.alterColumnAttr(config, tapAlterFieldAttributesEvent));
    }

    @Test
    void testAlterColumnAttrWithBlankFieldName() {
        Mockito.when(tapAlterFieldAttributesEvent.getFieldName()).thenReturn("");

        Assertions.assertThrows(RuntimeException.class, () -> generator.alterColumnAttr(config, tapAlterFieldAttributesEvent));
    }
    @Test
    void testAlterColumnAttrWithBlankDataType() {
        ValueChange<String> dataTypeChange = new ValueChange<>("", "");
        Mockito.when(tapAlterFieldAttributesEvent.getDataTypeChange()).thenReturn(dataTypeChange);

        Assertions.assertThrows(RuntimeException.class, () -> generator.alterColumnAttr(config, tapAlterFieldAttributesEvent));
    }

    @Test
    void testAlterColumnName() {
        ValueChange<String> nameChange = new ValueChange<>("oldColumnName", "newColumnName");
        tapAlterFieldNameEvent.setTableId("test_table");
        tapAlterFieldNameEvent.setNameChange(nameChange);
        Mockito.when(config.getDatabase()).thenReturn("test_db");
        List<String> sqls = generator.alterColumnName(config, tapAlterFieldNameEvent);

        Assertions.assertNotNull(sqls, "The SQL list should not be null");
        Assertions.assertEquals(1, sqls.size(), "The SQL list should contain exactly one SQL statement");
        Assertions.assertEquals(
                 "alter table \"test_db\".\"test_table\" rename column `oldColumnName` to `newColumnName`",
                sqls.get(0),
                "The generated SQL statement is incorrect"
        );
    }

    @Test
    void testAlterColumnNameWithNullEvent() {
        Assertions.assertNull(generator.alterColumnName(config, null));
    }

    @Test
    void testAlterColumnNameWithBlankTableName() {
        tapAlterFieldNameEvent.setTableId("  "); // Set blank table name

        Assertions.assertThrows(RuntimeException.class, () -> {
            generator.alterColumnName(config, tapAlterFieldNameEvent);
        }, "A blank table name should throw a RuntimeException");
    }

    @Test
    void testAlterColumnNameWithNullNameChange() {
        tapAlterFieldNameEvent.setNameChange(null); // Set null name change

        Assertions.assertThrows(RuntimeException.class, () -> {
            generator.alterColumnName(config, tapAlterFieldNameEvent);
        }, "A null name change should throw a RuntimeException");
    }

    @Test
    void testAlterColumnNameWithBlankOldColumnName() {
        ValueChange<String> nameChange = new ValueChange<>("", "newColumnName");
        tapAlterFieldNameEvent.setNameChange(nameChange); // Set blank old column name

        Assertions.assertThrows(RuntimeException.class, () -> {
            generator.alterColumnName(config, tapAlterFieldNameEvent);
        }, "A blank old column name should throw a RuntimeException");
    }

    @Test
    void testAlterColumnNameWithBlankNewColumnName() {
        ValueChange<String> nameChange = new ValueChange<>("oldColumnName", "");
        tapAlterFieldNameEvent.setNameChange(nameChange); // Set blank new column name

        Assertions.assertThrows(RuntimeException.class, () -> {
            generator.alterColumnName(config, tapAlterFieldNameEvent);
        }, "A blank new column name should throw a RuntimeException");
    }

    @Test
    void testDropColumn() {
        String tableName = "test_table";
        String fieldName = "test_field";
        tapDropFieldEvent.setTableId(tableName);
        tapDropFieldEvent.setFieldName(fieldName);
        Mockito.when(config.getDatabase()).thenReturn("test_db");
        List<String> sqls = generator.dropColumn(config, tapDropFieldEvent);

        Assertions.assertNotNull(sqls, "The SQL list should not be null");
        Assertions.assertEquals(1, sqls.size(), "The SQL list should contain exactly one SQL statement");
        Assertions.assertEquals("alter table \"test_db\".\"test_table\" drop column`test_field`", sqls.get(0), "The generated SQL should match the expected value");
    }

    @Test
    void testDropColumnWithBlankTableName() {
        tapDropFieldEvent.setTableId(" ");
        Assertions.assertThrows(RuntimeException.class, () -> {
            generator.dropColumn(config, tapDropFieldEvent);
        }, "A RuntimeException should be thrown if the table name is blank");
    }

    @Test
    void testDropColumnWithBlankFieldName() {
        tapDropFieldEvent.setFieldName(" ");
        Assertions.assertThrows(RuntimeException.class, () -> {
            generator.dropColumn(config, tapDropFieldEvent);
        }, "A RuntimeException should be thrown if the field name is blank");
    }

    @Test
    void testDropColumnWithNullEvent() {
        Assertions.assertNull(generator.dropColumn(config, null), "The SQL list should be null if the event is null");
    }

}
