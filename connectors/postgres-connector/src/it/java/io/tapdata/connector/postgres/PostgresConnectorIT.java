package io.tapdata.connector.postgres;

import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.ConnectorTestContext;
import io.tapdata.it.dbforge.DbForgeLeaseProvider;
import io.tapdata.it.performance.PerformanceAdapter;
import io.tapdata.it.schema.TestDataType;
import io.tapdata.it.schema.TestFieldSpec;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.it.schema.TestTableSpec;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.it.tpcc.TpccAdapter;
import io.tapdata.it.tpcc.TpccConnectorIT;
import io.tapdata.it.verifier.ConnectorVerifier;
import io.tapdata.it.verifier.JdbcVerifier;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgresConnectorIT extends TpccConnectorIT {

    protected DbForgeLeaseProvider dbForgeLeaseProvider;
    protected DataMap dbForgeConnectionConfig;

    @Override
    protected PerformanceAdapter createPerformanceAdapter() {
        return new PostgresPerformanceAdapter(context.getConfig());
    }

    @Override
    protected TpccAdapter createTpccAdapter() {
        return new PostgresTpccAdapter(context.getConfig());
    }

    @Override
    protected ConnectorVerifier createVerifier() {
        ConnectorVerifier verifier = super.createVerifier();
        if (!(verifier instanceof JdbcVerifier)) {
            return verifier;
        }
        return new PostgresJdbcVerifier(((JdbcVerifier) verifier).jdbcContext(), context.getConfig().getString("schema"));
    }

    @Override
    protected TestTableSpec createTestTableSpec() {
        return TestTableSpec.builder()
                .tableName(TestTableSpec.randomTableName("TAP_POSTGRES_IT_"))
                .addField(TestFieldSpec.builder().name("id").dataType("BIGINT").testDataType(TestDataType.BIGINT).primaryKey(true).build())
                .addField(TestFieldSpec.builder().name("c_int").dataType("INTEGER").testDataType(TestDataType.INT).build())
                .addField(TestFieldSpec.builder().name("c_bigint").dataType("BIGINT").testDataType(TestDataType.BIGINT).build())
                .addField(TestFieldSpec.builder().name("c_varchar").dataType("VARCHAR(255)").testDataType(TestDataType.VARCHAR).build())
                .addField(TestFieldSpec.builder().name("c_decimal").dataType("DECIMAL(18,4)").testDataType(TestDataType.DECIMAL).build())
                .addField(TestFieldSpec.builder().name("c_double").dataType("DOUBLE PRECISION").testDataType(TestDataType.DOUBLE).build())
                .build();
    }

    @Override
    protected ConnectorTestContext createContext() throws Throwable {
        PostgresConnector connector = new PostgresConnector();
        DataMap config = loadConnectionConfig();
        config.put("port", Integer.parseInt(String.valueOf(config.get("port"))));
        TapLog log = new TapLog();
        TapConnectorContext nodeContext = new TapConnectorContext(loadSpecification("spec_postgres.json"), config,
                DataMap.create().kv("enableTransaction", true), log);
        nodeContext.setStateMap(new TestStateMap());
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        connector.registerCapabilities(functions, codecRegistry);
        functions.supportCreateConstraint(null);
        functions.supportQueryConstraints(null);
        functions.supportDropConstraint(null);
        return ConnectorTestContext.builder().connector(connector).nodeContext(nodeContext)
                .connectorFunctions(functions).codecRegistry(codecRegistry).config(config).log(log).build();
    }

    @Override
    protected String rawQueryCommand(String tableName) {
        return "select * from \"public\".\"" + tableName.replace("\"", "\"\"") + "\"";
    }

    @Override
    protected String rawCountCommand(String tableName) {
        return rawQueryCommand(tableName);
    }

    protected boolean supportsSourceDdlCapture() {
        return true;
    }

    protected List<String> sourceDdlStatements(String table) {
        // PostgreSQL 需要 schema.table 格式的完整表名
        String qualifiedTable = "\"public\".\"" + table.replace("\"", "\"\"") + "\"";
        return List.of(
                "ALTER TABLE " + qualifiedTable + " ADD COLUMN c_ddl_probe varchar(64)",
                // TAP-13061 回归场景：多空格 ADD CONSTRAINT（旧版正则会误判为加列）
                "ALTER TABLE " + qualifiedTable + "  ADD  CONSTRAINT  chk_ddl_probe CHECK (c_ddl_probe IS NULL OR length(c_ddl_probe) < 100)",
                "ALTER TABLE " + qualifiedTable + " DROP CONSTRAINT chk_ddl_probe",
                "ALTER TABLE " + qualifiedTable + " RENAME COLUMN c_ddl_probe TO c_ddl_probe_new",
                "ALTER TABLE " + qualifiedTable + " DROP COLUMN c_ddl_probe_new");
    }

    @Override
    protected String sourceDdlInterleaveDropFieldStatement(String table) {
        String qualifiedTable = "\"public\".\"" + table.replace("\"", "\"\"") + "\"";
        return "ALTER TABLE " + qualifiedTable + " DROP COLUMN c_ddl_probe";
    }

    protected String dbForgeTopology() {
        return "single";
    }

    protected String defaultCdcMode() {
        return "postgres-logical-trigger";
    }

    protected synchronized DataMap loadConnectionConfig() throws Exception {
        String cdcMode = System.getProperty("connector.it.cdc.mode", defaultCdcMode());
        if (!DbForgeLeaseProvider.isDbForgeSelected()) {
            DataMap config = readConnectionConfig("config/postgres-connection.json");
            applyCdcMode(config, cdcMode);
            return config;
        }
        if (dbForgeConnectionConfig == null) {
            dbForgeLeaseProvider = DbForgeLeaseProvider.fromEnvironment("tapdata-postgres-connector-it");
            DbForgeLeaseProvider.Connection connection = dbForgeLeaseProvider.acquire(
                    "postgresql", "dedicated", dbForgeTopology());
            dbForgeConnectionConfig = DataMap.create();
            dbForgeConnectionConfig.put("host", connection.required("host"));
            dbForgeConnectionConfig.put("port", connection.requiredPort());
            dbForgeConnectionConfig.put("database", connection.required("database"));
            dbForgeConnectionConfig.put("schema", "public");
            dbForgeConnectionConfig.put("user", connection.firstRequired("user", "username"));
            dbForgeConnectionConfig.put("password", connection.required("password"));
            dbForgeConnectionConfig.put("slotName", "tapdata_connector_it");
            if ("efm-tde".equals(dbForgeTopology())) {
                assertDbForgeEfMEndpointIsStandby(dbForgeConnectionConfig);
                dbForgeConnectionConfig.put("deploymentMode", "master-slave");
                dbForgeConnectionConfig.put("checkCdcSlave", true);
                // DbForge's EFM TDE image wraps key.bin with OpenSSL AES-128-CBC.
                // Auto-detection can accept a wrong AES-256 unwrap if random padding happens to validate.
                dbForgeConnectionConfig.put("walTdeKeyWrapAlgorithm", "aes-128-cbc");
                LinkedHashMap<String, Object> standbyAddress = new LinkedHashMap<>();
                standbyAddress.put("host", dbForgeConnectionConfig.getString("host"));
                standbyAddress.put("port", dbForgeConnectionConfig.getInteger("port"));
                dbForgeConnectionConfig.put("masterSlaveAddress", new ArrayList<>(List.of(standbyAddress)));
                String tdeKey = connection.optional("walTdeKey");
                if (tdeKey == null) {
                    tdeKey = System.getProperty("connector.it.postgres.walTdeKey",
                            System.getenv("CONNECTOR_IT_POSTGRES_WAL_TDE_KEY"));
                }
                if (tdeKey == null || tdeKey.isBlank()) {
                    tdeKey = readTdeKeyFile(dbForgeConnectionConfig);
                }
                if (tdeKey != null && !tdeKey.isBlank()) {
                    dbForgeConnectionConfig.put("walTdeKey", tdeKey);
                }
                String tdePassword = connection.optional("walTdeKeyPassword");
                if (tdePassword == null) {
                    tdePassword = connection.optional("tde");
                }
                if (tdePassword == null) {
                    tdePassword = System.getProperty("connector.it.postgres.walTdeKeyPassword",
                            System.getenv("CONNECTOR_IT_POSTGRES_WAL_TDE_KEY_PASSWORD"));
                }
                if (tdePassword != null && !tdePassword.isBlank()) {
                    dbForgeConnectionConfig.put("walTdeKeyPassword", tdePassword);
                }
            }
            System.out.printf("[IT] DBForge PostgreSQL lease acquired: leaseId=%s, host=%s, port=%s, database=%s%n",
                    dbForgeLeaseProvider.getLeaseId(), dbForgeConnectionConfig.getString("host"),
                    dbForgeConnectionConfig.getInteger("port"), dbForgeConnectionConfig.getString("database"));
        }
        DataMap config = DataMap.create();
        config.putAll(dbForgeConnectionConfig);
        applyCdcMode(config, cdcMode);
        return config;
    }

    private String readTdeKeyFile(DataMap connectionConfig) throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException error) {
            throw new SQLException("PostgreSQL JDBC driver is unavailable to the EFM IT", error);
        }
        String jdbcUrl = "jdbc:postgresql://" + connectionConfig.getString("host") + ":"
                + connectionConfig.getInteger("port") + "/" + connectionConfig.getString("database");
        try (Connection connection = DriverManager.getConnection(jdbcUrl,
                connectionConfig.getString("user"), connectionConfig.getString("password"));
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT pg_is_in_recovery(), encode(pg_read_binary_file('pg_encryption/key.bin'), 'base64')")) {
            if (!result.next()) {
                throw new SQLException("DBForge EFM PostgreSQL did not return its TDE key file");
            }
            if (!result.getBoolean(1)) {
                throw new SQLException("DBForge efm-tde endpoint did not route to a PostgreSQL standby");
            }
            return result.getString(2);
        }
    }

    private void assertDbForgeEfMEndpointIsStandby(DataMap connectionConfig) throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException error) {
            throw new SQLException("PostgreSQL JDBC driver is unavailable to the EFM IT", error);
        }
        String jdbcUrl = "jdbc:postgresql://" + connectionConfig.getString("host") + ":"
                + connectionConfig.getInteger("port") + "/" + connectionConfig.getString("database");
        try (Connection connection = DriverManager.getConnection(jdbcUrl,
                connectionConfig.getString("user"), connectionConfig.getString("password"));
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT pg_is_in_recovery()")) {
            if (!result.next() || !result.getBoolean(1)) {
                throw new SQLException("DBForge efm-tde endpoint must route to a PostgreSQL standby");
            }
        }
    }

    protected String dbForgeEphemeralNamespace() {
        return System.getProperty("connector.it.dbforge.ephemeral.namespace",
                System.getenv().getOrDefault("CONNECTOR_IT_DBFORGE_EPHEMERAL_NAMESPACE", "dbforge-env-ephemeral"));
    }

    private void applyCdcMode(DataMap config, String cdcMode) {
        if (cdcMode == null || cdcMode.isBlank()) return;
        if ("postgres-logical-trigger".equalsIgnoreCase(cdcMode)) {
            config.put("logPluginName", "pgoutput");
            config.put("ddlTriggerEnable", true);
        } else if ("postgres-physical".equalsIgnoreCase(cdcMode)) {
            config.put("logPluginName", "physical");
            config.put("ddlTriggerEnable", false);
            if ("efm-tde".equals(dbForgeTopology())) {
                config.put("deploymentMode", "master-slave");
                config.put("checkCdcSlave", true);
            }
        } else {
            throw new IllegalArgumentException("Unsupported PostgreSQL CDC test mode: " + cdcMode);
        }
    }

    @AfterAll
    void releaseDbForgeLease() {
        if (dbForgeLeaseProvider == null) {
            return;
        }
        try {
            dbForgeLeaseProvider.release();
            System.out.println("[IT] DBForge PostgreSQL lease released");
        } catch (Exception error) {
            System.err.println("[IT] Failed to release DBForge PostgreSQL lease: " + error.getMessage());
        }
    }

    // ---- TAP-13194：clearTable 的 TRUNCATE CASCADE 由节点参数 cascadeTruncate 控制，dropTable 保持 CASCADE ----

    @Test
    void should_fail_truncate_referenced_parent_and_keep_child_when_cascade_truncate_off() throws Throwable {
        String suffix = randomSuffix();
        String parent = "tap13194_p_" + suffix;
        String child = "tap13194_c_" + suffix;
        try (Connection connection = openJdbc()) {
            createParentChild(connection, parent, child);
            PostgresConnector connector = startConnector(false);
            try {
                Assertions.assertThrows(SQLException.class, () -> connector.clearTable(nodeContext(), clearEvent(parent)));
            } finally {
                connector.onStop(nodeContext());
            }
            Assertions.assertEquals(2, count(connection, parent));
            Assertions.assertEquals(3, count(connection, child));
        } finally {
            dropQuietly(child, parent);
        }
    }

    @Test
    void should_truncate_parent_and_child_when_cascade_truncate_on() throws Throwable {
        String suffix = randomSuffix();
        String parent = "tap13194_p_" + suffix;
        String child = "tap13194_c_" + suffix;
        try (Connection connection = openJdbc()) {
            createParentChild(connection, parent, child);
            PostgresConnector connector = startConnector(true);
            try {
                connector.clearTable(nodeContext(), clearEvent(parent));
            } finally {
                connector.onStop(nodeContext());
            }
            Assertions.assertEquals(0, count(connection, parent));
            Assertions.assertEquals(0, count(connection, child));
        } finally {
            dropQuietly(child, parent);
        }
    }

    @Test
    void should_keep_drop_cascade_when_cascade_truncate_off() throws Throwable {
        String suffix = randomSuffix();
        String parent = "tap13194_p_" + suffix;
        String child = "tap13194_c_" + suffix;
        try (Connection connection = openJdbc()) {
            createParentChild(connection, parent, child);
            PostgresConnector connector = startConnector(false);
            try {
                connector.dropTable(nodeContext(), dropEvent(parent));
            } finally {
                connector.onStop(nodeContext());
            }
            Assertions.assertFalse(tableExists(connection, parent));
            Assertions.assertEquals(3, count(connection, child));
            Assertions.assertEquals(0, foreignKeyCount(connection, child));
        } finally {
            dropQuietly(child, parent);
        }
    }

    @Test
    void should_clear_and_drop_plain_table_when_cascade_truncate_off() throws Throwable {
        String table = "tap13194_t_" + randomSuffix();
        try (Connection connection = openJdbc()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + qualified(table) + " (id int primary key, v varchar(32))");
                statement.execute("INSERT INTO " + qualified(table) + " VALUES (1,'a'),(2,'b')");
            }
            PostgresConnector connector = startConnector(false);
            try {
                connector.clearTable(nodeContext(), clearEvent(table));
                Assertions.assertEquals(0, count(connection, table));
                connector.dropTable(nodeContext(), dropEvent(table));
            } finally {
                connector.onStop(nodeContext());
            }
            Assertions.assertFalse(tableExists(connection, table));
        } finally {
            dropQuietly(table);
        }
    }

    // ---- TAP-13224：同步账号对被引用表无权限时，外键被引用列名不能丢失 ----

    @Test
    void should_discover_reference_key_when_referenced_table_not_granted() throws Throwable {
        String suffix = randomSuffix();
        String parent = "tap13224_p_" + suffix;
        String child = "tap13224_c_" + suffix;
        String role = "tap13224_u_" + suffix;
        String password = "P" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = openJdbc(); Statement statement = connection.createStatement()) {
            createParentChild(connection, parent, child);
            statement.execute("CREATE ROLE " + role + " LOGIN PASSWORD '" + password + "'");
            statement.execute("GRANT USAGE ON SCHEMA \"" + schemaName() + "\" TO " + role);
            statement.execute("GRANT SELECT ON " + qualified(child) + " TO " + role);

            DataMap restricted = DataMap.create();
            restricted.putAll(context.getConfig());
            restricted.put("user", role);
            restricted.put("password", password);
            PostgresConfig config = (PostgresConfig) new PostgresConfig().load(restricted);
            try (PostgresJdbcContext jdbcContext = new PostgresJdbcContext(config)) {
                List<DataMap> foreignKeys = jdbcContext.queryAllForeignKeys(List.of(child));
                Assertions.assertEquals(1, foreignKeys.size());
                Assertions.assertEquals(parent, foreignKeys.get(0).getString("referencesTableName"));
                Assertions.assertEquals("pid", foreignKeys.get(0).getString("fk"));
                Assertions.assertEquals("id", foreignKeys.get(0).getString("rfk"));
            }
        } finally {
            dropQuietly(child, parent);
            dropRoleQuietly(role);
        }
    }

    private void dropRoleQuietly(String role) {
        try (Connection connection = openJdbc(); Statement statement = connection.createStatement()) {
            statement.execute("DROP OWNED BY " + role);
            statement.execute("DROP ROLE IF EXISTS " + role);
        } catch (SQLException ignored) {
            // best-effort cleanup
        }
    }

    private PostgresConnector startConnector(boolean cascadeTruncate) throws Throwable {
        DataMap nodeConfig = nodeContext().getNodeConfig();
        if (nodeConfig == null) {
            nodeConfig = DataMap.create();
            nodeContext().setNodeConfig(nodeConfig);
        }
        nodeConfig.put("cascadeTruncate", cascadeTruncate);
        PostgresConnector connector = new PostgresConnector();
        connector.onStart(nodeContext());
        return connector;
    }

    private Connection openJdbc() throws SQLException {
        DataMap config = context.getConfig();
        String url = "jdbc:postgresql://" + config.getString("host") + ":" + config.getValue("port", 5432)
                + "/" + config.getString("database");
        return DriverManager.getConnection(url, config.getString("user"), config.getString("password"));
    }

    private String schemaName() {
        String schema = context.getConfig().getString("schema");
        return schema == null || schema.isEmpty() ? "public" : schema;
    }

    private String qualified(String table) {
        return "\"" + schemaName() + "\".\"" + table + "\"";
    }

    private void createParentChild(Connection connection, String parent, String child) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + qualified(parent) + " (id int primary key)");
            statement.execute("CREATE TABLE " + qualified(child) + " (id int primary key, pid int REFERENCES "
                    + qualified(parent) + "(id))");
            statement.execute("INSERT INTO " + qualified(parent) + " VALUES (1),(2)");
            statement.execute("INSERT INTO " + qualified(child) + " VALUES (1,1),(2,1),(3,2)");
        }
    }

    private long count(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM " + qualified(table))) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private boolean tableExists(Connection connection, String table) throws SQLException {
        try (ResultSet resultSet = connection.getMetaData().getTables(null, schemaName(), table, null)) {
            return resultSet.next();
        }
    }

    private int foreignKeyCount(Connection connection, String table) throws SQLException {
        int count = 0;
        try (ResultSet resultSet = connection.getMetaData().getImportedKeys(null, schemaName(), table)) {
            while (resultSet.next()) {
                count++;
            }
        }
        return count;
    }

    private void dropQuietly(String... tables) {
        try (Connection connection = openJdbc(); Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("DROP TABLE IF EXISTS " + qualified(table) + " CASCADE");
            }
        } catch (SQLException ignored) {
            // best-effort cleanup
        }
    }

    private static String randomSuffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    private static TapClearTableEvent clearEvent(String table) {
        TapClearTableEvent event = new TapClearTableEvent();
        event.setTableId(table);
        return event;
    }

    private static TapDropTableEvent dropEvent(String table) {
        TapDropTableEvent event = new TapDropTableEvent();
        event.setTableId(table);
        return event;
    }
}
