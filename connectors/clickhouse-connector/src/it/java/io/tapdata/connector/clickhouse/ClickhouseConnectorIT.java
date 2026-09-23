package io.tapdata.connector.clickhouse;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.ConnectorTestContext;
import io.tapdata.it.dbforge.DbForgeLeaseProvider;
import io.tapdata.it.performance.PerformanceAdapter;
import io.tapdata.it.schema.TestDataType;
import io.tapdata.it.schema.TestFieldSpec;
import io.tapdata.it.schema.TestTableSpec;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.it.tpcc.TpccAdapter;
import io.tapdata.it.tpcc.TpccConnectorIT;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ClickhouseConnectorIT extends TpccConnectorIT {

    private DbForgeLeaseProvider dbForgeLeaseProvider;
    private DataMap dbForgeConnectionConfig;

    @Override
    protected PerformanceAdapter createPerformanceAdapter() {
        return new ClickhousePerformanceAdapter(context.getConfig());
    }

    @Override
    protected TpccAdapter createTpccAdapter() {
        return new ClickhouseTpccAdapter();
    }

    @Override
    protected TestTableSpec createTestTableSpec() {
        return TestTableSpec.builder()
                .tableName(TestTableSpec.randomTableName("TAP_CLICKHOUSE_IT_"))
                .addField(TestFieldSpec.builder().name("id").dataType("Int64").testDataType(TestDataType.BIGINT).primaryKey(true).build())
                .addField(TestFieldSpec.builder().name("c_int").dataType("Int32").testDataType(TestDataType.INT).build())
                .addField(TestFieldSpec.builder().name("c_bigint").dataType("Int64").testDataType(TestDataType.BIGINT).build())
                .addField(TestFieldSpec.builder().name("c_varchar").dataType("String").testDataType(TestDataType.VARCHAR).build())
                .addField(TestFieldSpec.builder().name("c_decimal").dataType("Decimal(18,4)").testDataType(TestDataType.DECIMAL).build())
                .addField(TestFieldSpec.builder().name("c_double").dataType("Float64").testDataType(TestDataType.DOUBLE).build())
                .build();
    }

    @Override
    protected ConnectorTestContext createContext() throws Throwable {
        ClickhouseConnector connector = new ClickhouseConnector();
        DataMap config = loadConnectionConfig();
        config.put("port", Integer.parseInt(String.valueOf(config.get("port"))));
        TapLog log = new TapLog();
        TapConnectorContext nodeContext = new TapConnectorContext(loadSpecification("spec_clickhouse.json"), config,
                DataMap.create().kv("enableTransaction", true), log);
        nodeContext.setStateMap(new TestStateMap());
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        connector.registerCapabilities(functions, codecRegistry);
        functions.supportAlterFieldAttributesFunction(null);
        return ConnectorTestContext.builder().connector(connector).nodeContext(nodeContext)
                .connectorFunctions(functions).codecRegistry(codecRegistry).config(config).log(log).build();
    }

    @Override
    protected String rawCountCommand(String tableName) {
        return "select * from `" + tableName.replace("`", "``") + "`";
    }

    private synchronized DataMap loadConnectionConfig() throws Exception {
        if (!DbForgeLeaseProvider.isDbForgeSelected()) {
            return readConnectionConfig("config/clickhouse-connection.json");
        }
        if (dbForgeConnectionConfig == null) {
            dbForgeLeaseProvider = DbForgeLeaseProvider.fromEnvironment("tapdata-clickhouse-connector-it");
            DbForgeLeaseProvider.Connection connection = dbForgeLeaseProvider.acquire("clickhouse", "dedicated", "single");
            dbForgeConnectionConfig = DataMap.create();
            dbForgeConnectionConfig.put("host", connection.required("host"));
            dbForgeConnectionConfig.put("port", connection.requiredPort());
            dbForgeConnectionConfig.put("database", connection.required("database"));
            dbForgeConnectionConfig.put("user", connection.firstRequired("user", "username"));
            dbForgeConnectionConfig.put("password", connection.required("password"));
            dbForgeConnectionConfig.put("supportPk", true);
            System.out.printf("[IT] DBForge ClickHouse lease acquired: leaseId=%s, host=%s, port=%s, database=%s%n",
                    dbForgeLeaseProvider.getLeaseId(), dbForgeConnectionConfig.getString("host"),
                    dbForgeConnectionConfig.getInteger("port"), dbForgeConnectionConfig.getString("database"));
        }
        DataMap config = DataMap.create();
        config.putAll(dbForgeConnectionConfig);
        return config;
    }

    @AfterAll
    void releaseDbForgeLease() {
        if (dbForgeLeaseProvider == null) {
            return;
        }
        try {
            dbForgeLeaseProvider.release();
            System.out.println("[IT] DBForge ClickHouse lease released");
        } catch (Exception error) {
            System.err.println("[IT] Failed to release DBForge ClickHouse lease: " + error.getMessage());
        }
    }
}
