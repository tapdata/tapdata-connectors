package io.tapdata.it.ddl;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.ConnectorIT;
import io.tapdata.it.ConnectorTestContext;
import io.tapdata.it.dbforge.DbForgeLeaseProvider;
import io.tapdata.it.schema.TestDataType;
import io.tapdata.it.schema.TestFieldSpec;
import io.tapdata.it.schema.TestTableSpec;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.function.Supplier;

/** Runs the shared source-DDL assertions through each derived connector's own registration and lifecycle. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class JdbcFamilyDdlIT extends ConnectorIT {
    private final Supplier<? extends ConnectorBase> connectorFactory;
    private final String specification;
    private final String engine;
    private final String module;

    protected boolean postgresDialect() { return false; }
    private DbForgeLeaseProvider lease;
    private DataMap connectionConfig;

    protected JdbcFamilyDdlIT(Supplier<? extends ConnectorBase> connectorFactory, String specification,
                              String engine, String module) {
        this.connectorFactory = connectorFactory;
        this.specification = specification;
        this.engine = engine;
        this.module = module;
    }

    @Override
    protected ConnectorTestContext createContext() throws Throwable {
        ConnectorBase connector = connectorFactory.get();
        DataMap config = loadConfig();
        config.put("port", Integer.parseInt(String.valueOf(config.get("port"))));
        if (postgresDialect()) {
            config.put("schema", "public");
            config.put("slotName", "tap_ddl_" + java.util.UUID.randomUUID().toString().replace("-", ""));
            config.put("logPluginName", "pgoutput");
            config.put("ddlTriggerEnable", true);
        } else {
            config.put("highPerformance", false);
        }
        TapLog log = new TapLog();
        TapConnectorContext node = new TapConnectorContext(loadSpecification(specification), config,
                DataMap.create().kv("enableTransaction", true), log);
        node.setStateMap(new TestStateMap());
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry codecs = TapCodecsRegistry.create();
        connector.registerCapabilities(functions, codecs);
        return ConnectorTestContext.builder().connector(connector).nodeContext(node)
                .connectorFunctions(functions).codecRegistry(codecs).config(config).log(log).build();
    }

    private DataMap loadConfig() throws Exception {
        if (!DbForgeLeaseProvider.isDbForgeSelected()) {
            // No fallback to another product's credentials: an explicit config is required.
            String path = sysPropOrEnv("ddl.config", "config/" + module + ".json");
            return readConnectionConfig(path);
        }
        if (connectionConfig == null) {
            lease = DbForgeLeaseProvider.fromEnvironment("tapdata-" + module + "-ddl-it");
            DbForgeLeaseProvider.Connection connection = lease.acquire(engine, "dedicated", "single");
            connectionConfig = DataMap.create();
            connectionConfig.put("host", connection.required("host"));
            connectionConfig.put("port", connection.requiredPort());
            connectionConfig.put("database", connection.required("database"));
            connectionConfig.put("user", connection.firstRequired("user", "username"));
            connectionConfig.put("password", connection.required("password"));
            System.out.printf("[IT] %s implementation against DBForge %s/single, leaseId=%s%n",
                    module, engine, lease.getLeaseId());
        }
        DataMap result = DataMap.create();
        result.putAll(connectionConfig);
        return result;
    }

    @Override
    protected TestTableSpec createTestTableSpec() {
        return TestTableSpec.builder().tableName(TestTableSpec.randomTableName("TAP_DDL_IT_"))
                .addField(TestFieldSpec.builder().name("id").dataType("BIGINT")
                        .testDataType(TestDataType.BIGINT).primaryKey(true).build())
                .addField(TestFieldSpec.builder().name("c_varchar").dataType("VARCHAR(255)")
                        .testDataType(TestDataType.VARCHAR).build())
                .build();
    }

    @Override
    protected boolean supportsSourceDdlCapture() {
        return true;
    }

    @Override
    protected List<String> sourceDdlStatements(String table) {
        return List.of("ALTER TABLE " + table + " ADD COLUMN c_ddl_probe VARCHAR(64)",
                "ALTER TABLE " + table + " CHANGE COLUMN c_ddl_probe c_ddl_probe_new VARCHAR(64)",
                "ALTER TABLE " + table + " DROP COLUMN c_ddl_probe_new");
    }

    @Override
    protected String sourceCompositeAddColumnsStatement(String table) {
        return "ALTER TABLE " + table
                + " ADD COLUMN c_composite_a VARCHAR(64), ADD COLUMN c_composite_b BIGINT";
    }

    @Override
    protected String sourceDdlInterleaveDropFieldStatement(String table) {
        return "ALTER TABLE " + table + " DROP COLUMN c_ddl_probe";
    }

    @Override
    protected long streamReadTimeoutSeconds() {
        return 60L;
    }

    @Override
    protected void prepareStreamReadTable() throws Exception {
        // MysqlReader's timestamp offset has second precision; leave a binlog boundary after CREATE.
        Thread.sleep(2000L);
    }

    @AfterAll
    void releaseLease() throws Exception {
        if (lease != null) {
            lease.release();
            lease = null;
            connectionConfig = null;
        }
    }
}
