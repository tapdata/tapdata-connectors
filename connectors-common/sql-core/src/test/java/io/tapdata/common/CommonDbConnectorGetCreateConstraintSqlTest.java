package io.tapdata.common;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapConstraint;
import io.tapdata.entity.schema.TapConstraintMapping;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.DbKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * getCreateConstraintSql方法的单元测试
 *
 * @author TapData
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("CommonDbConnector.getCreateConstraintSql()方法测试")
class CommonDbConnectorGetCreateConstraintSqlTest {

    @Mock
    private CommonDbConfig commonDbConfig;

    private TestableCommonDbConnector connector;
    private Method getCreateConstraintSqlMethod;

    @BeforeEach
    void setUp() throws Exception {
        connector = new TestableCommonDbConnector() {
            @Override
            public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
                return null;
            }

            @Override
            public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

            }

            @Override
            public void onStart(TapConnectionContext connectionContext) throws Throwable {

            }

            @Override
            public void onStop(TapConnectionContext connectionContext) throws Throwable {

            }
        };
        connector.commonDbConfig = commonDbConfig;

        // 获取私有方法
        getCreateConstraintSqlMethod = CommonDbConnector.class.getDeclaredMethod(
                "getCreateConstraintSql", TapTable.class, TapConstraint.class);
        getCreateConstraintSqlMethod.setAccessible(true);
    }

    @Test
    @DisplayName("测试基本外键约束SQL生成")
    void testBasicForeignKeyConstraint() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("users");
        TapConstraint constraint = createBasicConstraint();

        // 执行测试
        String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

        // 验证结果
        String expected = "alter table `test_schema`.`users` add constraint `fk_user_dept` " +
                "foreign key (`dept_id`) references `test_schema`.`departments`(`id`)";
        assertEquals(expected, result);
    }

    @Test
    @DisplayName("测试多列外键约束SQL生成")
    void testMultiColumnForeignKeyConstraint() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('"');

        TapTable tapTable = createTestTable("order_items");
        TapConstraint constraint = createMultiColumnConstraint();

        // 执行测试
        String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

        // 验证结果
        String expected = "alter table \"test_schema\".\"order_items\" add constraint \"fk_order_product\" " +
                "foreign key (\"order_id\",\"product_id\") references \"test_schema\".\"order_products\"(\"order_id\",\"product_id\")";
        assertEquals(expected, result);
    }

    @Test
    @DisplayName("测试带ON UPDATE和ON DELETE的约束SQL生成")
    void testConstraintWithCascadeOptions() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("employees");
        TapConstraint constraint = createConstraintWithCascadeOptions();

        // 执行测试
        String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

        // 验证结果
        String expected = "alter table `test_schema`.`employees` add constraint `fk_emp_manager` " +
                "foreign key (`manager_id`) references `test_schema`.`employees`(`id`) " +
                "on update cascade on delete set null";
        assertEquals(expected, result.toLowerCase());
    }

    @Test
    @DisplayName("测试约束名为空时自动生成约束名")
    void testAutoGenerateConstraintName() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("users");
        TapConstraint constraint = createConstraintWithoutName();

        try (MockedStatic<DbKit> dbKitMock = mockStatic(DbKit.class)) {
            dbKitMock.when(() -> DbKit.buildForeignKeyName(eq("users"), eq(constraint), eq(32)))
                    .thenReturn("FK_users_dept_id_12345678");

            // 执行测试
            String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

            // 验证结果
            assertTrue(result.contains("add constraint `FK_users_dept_id_12345678`"));
            assertTrue(result.contains("foreign key (`dept_id`)"));
            assertTrue(result.contains("references `test_schema`.`departments`(`id`)"));
        }
    }

    @Test
    @DisplayName("测试ON UPDATE和ON DELETE选项的下划线替换")
    void testCascadeOptionsUnderscoreReplacement() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("users");
        TapConstraint constraint = new TapConstraint("fk_test", TapConstraint.ConstraintType.FOREIGN_KEY);
        constraint.referencesTable("departments");
        constraint.add(new TapConstraintMapping().foreignKey("dept_id").referenceKey("id"));
        constraint.onUpdate("SET_NULL");  // 包含下划线
        constraint.onDelete("NO_ACTION"); // 包含下划线

        // 执行测试
        String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

        // 验证结果 - 下划线应该被替换为空格
        assertTrue(result.contains("on update SET NULL"));
        assertTrue(result.contains("on delete NO ACTION"));
    }

    // ==================== 辅助方法 ====================

    /**
     * 创建测试表
     */
    private TapTable createTestTable(String tableName) {
        return new TapTable(tableName);
    }

    /**
     * 创建基本的外键约束
     */
    private TapConstraint createBasicConstraint() {
        TapConstraint constraint = new TapConstraint("fk_user_dept", TapConstraint.ConstraintType.FOREIGN_KEY);
        constraint.referencesTable("departments");
        constraint.add(new TapConstraintMapping()
                .foreignKey("dept_id")
                .referenceKey("id"));
        return constraint;
    }

    /**
     * 创建多列外键约束
     */
    private TapConstraint createMultiColumnConstraint() {
        TapConstraint constraint = new TapConstraint("fk_order_product", TapConstraint.ConstraintType.FOREIGN_KEY);
        constraint.referencesTable("order_products");
        constraint.add(new TapConstraintMapping()
                .foreignKey("order_id")
                .referenceKey("order_id"));
        constraint.add(new TapConstraintMapping()
                .foreignKey("product_id")
                .referenceKey("product_id"));
        return constraint;
    }

    /**
     * 创建带级联选项的约束
     */
    private TapConstraint createConstraintWithCascadeOptions() {
        TapConstraint constraint = new TapConstraint("fk_emp_manager", TapConstraint.ConstraintType.FOREIGN_KEY);
        constraint.referencesTable("employees");
        constraint.add(new TapConstraintMapping()
                .foreignKey("manager_id")
                .referenceKey("id"));
        constraint.onUpdate("CASCADE");
        constraint.onDelete("SET_NULL");
        return constraint;
    }

    /**
     * 创建没有约束名的约束
     */
    private TapConstraint createConstraintWithoutName() {
        TapConstraint constraint = new TapConstraint(null, TapConstraint.ConstraintType.FOREIGN_KEY);
        constraint.referencesTable("departments");
        constraint.add(new TapConstraintMapping()
                .foreignKey("dept_id")
                .referenceKey("id"));
        return constraint;
    }

    @Test
    @DisplayName("测试特殊字符在表名和字段名中的处理")
    void testSpecialCharactersInNames() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("user-table");
        TapConstraint constraint = new TapConstraint("fk-test", TapConstraint.ConstraintType.FOREIGN_KEY);
        constraint.referencesTable("dept-table");
        constraint.add(new TapConstraintMapping()
                .foreignKey("dept-id")
                .referenceKey("id-field"));

        // 执行测试
        String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

        // 验证结果
        assertTrue(result.contains("`user-table`"));
        assertTrue(result.contains("`fk-test`"));
        assertTrue(result.contains("`dept-id`"));
        assertTrue(result.contains("`dept-table`"));
        assertTrue(result.contains("`id-field`"));
    }

    @Test
    @DisplayName("测试NULL值的处理")
    void testNullValueHandling() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("users");
        TapConstraint constraint = new TapConstraint("fk_test", TapConstraint.ConstraintType.FOREIGN_KEY);
        constraint.referencesTable("departments");
        constraint.add(new TapConstraintMapping()
                .foreignKey("dept_id")
                .referenceKey("id"));
        // onUpdate和onDelete保持为null

        // 执行测试
        String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

        // 验证结果 - 不应该包含ON UPDATE或ON DELETE子句
        assertFalse(result.contains("on update"));
        assertFalse(result.contains("on delete"));
        assertTrue(result.endsWith("references `test_schema`.`departments`(`id`)"));
    }

    @Test
    @DisplayName("测试约束名长度限制场景")
    void testConstraintNameLengthLimit() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("very_long_table_name_that_exceeds_normal_limits");
        TapConstraint constraint = createConstraintWithoutName();

        try (MockedStatic<DbKit> dbKitMock = mockStatic(DbKit.class)) {
            // 模拟DbKit返回截断后的约束名
            dbKitMock.when(() -> DbKit.buildForeignKeyName(anyString(), any(TapConstraint.class), eq(32)))
                    .thenReturn("FK_very_long_table_name_12345");

            // 执行测试
            String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

            // 验证结果
            assertTrue(result.contains("add constraint `FK_very_long_table_name_12345`"));
        }
    }

    @Test
    @DisplayName("测试单个字符的约束名")
    void testSingleCharacterConstraintName() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("t");
        TapConstraint constraint = new TapConstraint("f", TapConstraint.ConstraintType.FOREIGN_KEY);
        constraint.referencesTable("d");
        constraint.add(new TapConstraintMapping()
                .foreignKey("i")
                .referenceKey("j"));

        // 执行测试
        String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

        // 验证结果
        String expected = "alter table `test_schema`.`t` add constraint `f` " +
                "foreign key (`i`) references `test_schema`.`d`(`j`)";
        assertEquals(expected, result);
    }

    @Test
    @DisplayName("测试空字符串约束名的处理")
    void testEmptyStringConstraintName() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("users");
        TapConstraint constraint = new TapConstraint("", TapConstraint.ConstraintType.FOREIGN_KEY);
        constraint.referencesTable("departments");
        constraint.add(new TapConstraintMapping()
                .foreignKey("dept_id")
                .referenceKey("id"));

        try (MockedStatic<DbKit> dbKitMock = mockStatic(DbKit.class)) {
            dbKitMock.when(() -> DbKit.buildForeignKeyName(eq("users"), eq(constraint), eq(32)))
                    .thenReturn("FK_users_dept_id_auto");

            // 执行测试
            String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

            // 验证结果 - 应该使用自动生成的约束名
            assertTrue(result.contains("add constraint `FK_users_dept_id_auto`"));
        }
    }

    @Test
    @DisplayName("测试所有级联选项的组合")
    void testAllCascadeOptionsCombinations() throws Exception {
        // 准备测试数据
        when(commonDbConfig.getEscapeChar()).thenReturn('`');

        TapTable tapTable = createTestTable("users");

        // 测试所有可能的级联选项组合
        String[][] cascadeOptions = {
                {"CASCADE", "CASCADE"},
                {"SET_NULL", "RESTRICT"},
                {"NO_ACTION", "SET_DEFAULT"},
                {"RESTRICT", "NO_ACTION"}
        };

        for (String[] options : cascadeOptions) {
            TapConstraint constraint = new TapConstraint("fk_test", TapConstraint.ConstraintType.FOREIGN_KEY);
            constraint.referencesTable("departments");
            constraint.add(new TapConstraintMapping()
                    .foreignKey("dept_id")
                    .referenceKey("id"));
            constraint.onUpdate(options[0]);
            constraint.onDelete(options[1]);

            // 执行测试
            String result = (String) getCreateConstraintSqlMethod.invoke(connector, tapTable, constraint);

            // 验证结果
            assertTrue(result.contains("on update " + options[0].replace("_", " ")));
            assertTrue(result.contains("on delete " + options[1].replace("_", " ")));
        }
    }

    /**
     * 可测试的CommonDbConnector子类
     */
    private static abstract class TestableCommonDbConnector extends CommonDbConnector {
        @Override
        protected String getSchemaAndTable(String tableName) {
            return commonDbConfig.getEscapeChar() + "test_schema" + commonDbConfig.getEscapeChar() + "." + commonDbConfig.getEscapeChar() + tableName + commonDbConfig.getEscapeChar();
        }
    }
}
