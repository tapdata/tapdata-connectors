/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.text.ParsingException;
import io.debezium.util.IoUtil;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class MySqlDatabaseSchemaTest {

    private static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("dbHistory.log");
    private final UniqueDatabase DATABASE = new UniqueDatabase("testServer", "connector_test", null, null)
            .withDbHistoryPath(TEST_FILE_PATH);

    private static final String SERVER_NAME = "testServer";

    private MySqlDatabaseSchema mysql;
    private MySqlConnectorConfig connectorConfig;

    @Before
    public void beforeEach() {
        Testing.Files.delete(TEST_FILE_PATH);
    }

    private MySqlDatabaseSchema getSchema(Configuration config) {
        config = config.edit().with(AbstractDatabaseHistory.INTERNAL_PREFER_DDL, true).build();
        connectorConfig = new MySqlConnectorConfig(config);
        final MySqlValueConverters mySqlValueConverters = new MySqlValueConverters(
                DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE,
                BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                MySqlValueConverters::adjustTemporal,
                MySqlValueConverters::defaultParsingErrorHandler);
        return new MySqlDatabaseSchema(
                connectorConfig,
                mySqlValueConverters,
                MySqlTopicSelector.defaultSelector(connectorConfig),
                SchemaNameAdjuster.create(),
                false);
    }

    @After
    public void afterEach() {
        if (mysql != null) {
            try {
                mysql.close();
            }
            finally {
                mysql = null;
            }
        }
    }

    @Test
    public void shouldApplyDdlStatementsAndRecover() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig().build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl("SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null, offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertHistoryRecorded(config, offset);
    }

    @Test
    public void shouldIgnoreUnparseableDdlAndRecover() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl("SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null, offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl("xxxCREATE TABLE mytable\n" + IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertHistoryRecorded(config, offset);
    }

    @Test(expected = ParsingException.class)
    public void shouldFailOnUnparseableDdl() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl("SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null, offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl("xxxCREATE TABLE mytable\n" + IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));
    }

    @Test
    public void shouldLoadSystemAndNonSystemTablesAndConsumeOnlyFilteredDatabases() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl("SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null, offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-test-init-5.7.ddl"), "mysql", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        offset.setBinlogStartPoint("binlog-001", 1000);
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertTableExcluded("mysql.columns_priv");
        assertNoTablesExistForDatabase("mysql");
        assertHistoryRecorded(config, offset);
    }

    @Test
    public void shouldLoadSystemAndNonSystemTablesAndConsumeAllDatabases() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .with(MySqlConnectorConfig.TABLE_IGNORE_BUILTIN, false)
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl("SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null, offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-test-init-5.7.ddl"), "mysql", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        offset.setBinlogStartPoint("binlog-001", 1000);
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertTableIncluded("mysql.columns_priv");
        assertTablesExistForDatabase("mysql");
        assertHistoryRecorded(config, offset);
    }

    @Test
    public void shouldAllowDecimalPrecision() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-decimal-issue.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        assertTableIncluded("connector_test.business_order");
        assertTableIncluded("connector_test.business_order_detail");
        assertHistoryRecorded(config, offset);
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldStoreNonCapturedDatabase() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "captured")
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        mysql = getSchema(configFull);
        mysql.recover(offset);

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableIncluded("non_captured.nct");
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldNotStoreNonCapturedDatabase() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "captured")
                .with(DatabaseHistory.STORE_ONLY_MONITORED_TABLES_DDL, true)
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        mysql = getSchema(configFull);
        mysql.recover(offset);

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableExcluded("non_captured.nct");
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldStoreNonCapturedTable() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, "captured.ct")
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        assertTableIncluded("captured.ct");
        assertTableExcluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        mysql = getSchema(configFull);
        mysql.recover(offset);

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableIncluded("non_captured.nct");
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldNotStoreNonCapturedTable() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(DatabaseHistory.STORE_ONLY_MONITORED_TABLES_DDL, true)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, "captured.ct")
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog-001", 400);
        mysql.parseStreamingDdl(IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1", offset,
                Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        assertTableIncluded("captured.ct");
        assertTableExcluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        mysql = getSchema(configFull);
        mysql.recover(offset);

        assertTableIncluded("captured.ct");
        assertTableExcluded("captured.nct");
        assertTableExcluded("non_captured.nct");
    }

    protected void assertTableIncluded(String fullyQualifiedTableName) {
        TableId tableId = TableId.parse(fullyQualifiedTableName);
        TableSchema tableSchema = mysql.schemaFor(tableId);
        assertThat(tableSchema).isNotNull();
        assertThat(tableSchema.keySchema().name()).isEqualTo(SchemaNameAdjuster.validFullname(SERVER_NAME + "." + fullyQualifiedTableName + ".Key"));
        assertThat(tableSchema.valueSchema().name()).isEqualTo(SchemaNameAdjuster.validFullname(SERVER_NAME + "." + fullyQualifiedTableName + ".Value"));
    }

    protected void assertTableExcluded(String fullyQualifiedTableName) {
        TableId tableId = TableId.parse(fullyQualifiedTableName);
        assertThat(mysql.schemaFor(tableId)).isNull();
    }

    protected void assertNoTablesExistForDatabase(String dbName) {
        assertThat(mysql.tableIds().stream().filter(id -> id.catalog().equals(dbName)).count()).isEqualTo(0);
    }

    protected void assertTablesExistForDatabase(String dbName) {
        assertThat(mysql.tableIds().stream().filter(id -> id.catalog().equals(dbName)).count()).isGreaterThan(0);
    }

    protected void assertHistoryRecorded(Configuration config, OffsetContext offset) {
        MySqlDatabaseSchema duplicate = getSchema(config);
        duplicate.recover(offset);

        // Make sure table is defined in each ...
        assertThat(duplicate.tableIds()).isEqualTo(mysql.tableIds());
        for (int i = 0; i != 2; ++i) {
            duplicate.tableIds().forEach(tableId -> {
                TableSchema dupSchema = duplicate.schemaFor(tableId);
                TableSchema schema = mysql.schemaFor(tableId);
                assertThat(schema).isEqualTo(dupSchema);
                Table dupTable = duplicate.tableFor(tableId);
                Table table = mysql.tableFor(tableId);
                assertThat(table).isEqualTo(dupTable);
            });
            mysql.tableIds().forEach(tableId -> {
                TableSchema dupSchema = duplicate.schemaFor(tableId);
                TableSchema schema = mysql.schemaFor(tableId);
                assertThat(schema).isEqualTo(dupSchema);
                Table dupTable = duplicate.tableFor(tableId);
                Table table = mysql.tableFor(tableId);
                assertThat(table).isEqualTo(dupTable);
            });
            duplicate.refreshSchemas();
        }
    }

    protected void printStatements(String dbName, Set<TableId> tables, String ddlStatements) {
        Testing.print("Running DDL for '" + dbName + "': " + ddlStatements + " changing tables '" + tables + "'");
    }
    @Test
    public void testHandleForUnparseableDDL(){
        String ddlStatements = "CREATE TABLE `memberd` (\n" +
                "\t`id` bigint(20) NOT NULL COMMENT '会员id',\n" +
                "\t`code` varchar(30) DEFAULT NULL COMMENT '会员编号',\n" +
                "\t`phone` varchar(22) DEFAULT NULL COMMENT '会员手机号',\n" +
                "\t`telephone` varchar(15) DEFAULT '' COMMENT '座机固话',\n" +
                "\t`name` varchar(100) NOT NULL DEFAULT '' COMMENT '会员姓名',\n" +
                "\t`nick_name` varchar(64) NOT NULL DEFAULT '' COMMENT '会员昵称',\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tGLOBAL INDEX `idx_g_register_time` (`register_time`) PARTITION BY KEY (`register_time`),\n" +
                "\tUNIQUE INDEX `uk_g_phone` (`phone`) PARTITION BY KEY (`phone`) PARTITIONS 16,\n" +
                "\tUNIQUE GLOBAL INDEX `__advise_index_gsi_member_phone` (`phone`) COVERING (`code`, `telephone`, `name`, `nick_name`, `gender`, `birthday`, `month_day`, `status`, `identity`, `head_img_url`, `store_code`, `channel_code`, `application_code`, `password`, `invitor`, `invitor_name`, `register_time`, `deleted`, `create_user`, `create_user_name`, `create_time`, `update_user`, `update_user_name`, `update_time`, `level_id`, `is_real_name`) PARTITION BY KEY (`phone`) PARTITIONS 16,\n" +
                "\tGLOBAL INDEX `__advise_index_gsi_member_store_code_create_time` (`store_code`, `create_time`) PARTITION BY KEY (`store_code`, `create_time`) PARTITIONS 16,\n" +
                "\tUNIQUE KEY `UN_CODE` (`code`),\n" +
                "\tUNIQUE KEY `UK_PHONE` USING BTREE (`phone`),\n" +
                "\tKEY `INX_REGISTER_TIME` USING BTREE (`register_time`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT '会员基础信息表'\n" +
                "PARTITION BY KEY(`id`)\n" +
                "PARTITIONS 16";
        final Configuration config = DATABASE.defaultConfig().build();
        mysql = getSchema(config);
        String actual = mysql.handleForUnparseableDDL(ddlStatements);
        String expect = "CREATE TABLE `memberd` (\n" +
                "\t`id` bigint(20) NOT NULL COMMENT '会员id',\n" +
                "\t`code` varchar(30) DEFAULT NULL COMMENT '会员编号',\n" +
                "\t`phone` varchar(22) DEFAULT NULL COMMENT '会员手机号',\n" +
                "\t`telephone` varchar(15) DEFAULT '' COMMENT '座机固话',\n" +
                "\t`name` varchar(100) NOT NULL DEFAULT '' COMMENT '会员姓名',\n" +
                "\t`nick_name` varchar(64) NOT NULL DEFAULT '' COMMENT '会员昵称',\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tUNIQUE KEY `UN_CODE` (`code`),\n" +
                "\tUNIQUE KEY `UK_PHONE` USING BTREE (`phone`),\n" +
                "\tKEY `INX_REGISTER_TIME` USING BTREE (`register_time`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT '会员基础信息表'\n" +
                "PARTITION BY KEY(`id`)\n" +
                "PARTITIONS 16";
        assertEquals(expect, actual);
    }

    @Test
    public void testHandleForUnparseableDDL_filterDefaultFunction(){
        String ddlStatements = "CREATE TABLE Orders (" +
                "  OrderID INT NOT NULL," +
                "  OrderQuantity INT NOT NULL," +
                "  OrderPrice DECIMAL(18,2)," +
                "  OrderDate DATETIME DEFAULT CURRENT_TIMESTAMP() COMMENT '时间'," +
                "  created_at DATETIME DEFAULT sysdate() COMMENT '时间'" +
                ");";
        final Configuration config = DATABASE.defaultConfig().build();
        mysql = getSchema(config);
        String actual = mysql.handleForUnparseableDDL(ddlStatements);
        String expect = "CREATE TABLE Orders (" +
                "  OrderID INT NOT NULL," +
                "  OrderQuantity INT NOT NULL," +
                "  OrderPrice DECIMAL(18,2)," +
                "  OrderDate DATETIME COMMENT '时间'," +
                "  created_at DATETIME COMMENT '时间'" +
                ");";
        assertEquals(expect, actual);
    }
}
