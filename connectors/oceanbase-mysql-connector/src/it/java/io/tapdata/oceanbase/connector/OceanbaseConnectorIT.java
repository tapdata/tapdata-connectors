package io.tapdata.oceanbase.connector;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.ConnectorIT;
import io.tapdata.it.ConnectorTestContext;
import io.tapdata.it.schema.TestDataType;
import io.tapdata.it.schema.TestFieldSpec;
import io.tapdata.it.schema.TestTableSpec;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OceanbaseConnectorIT extends ConnectorIT {

    @Override
    protected TestTableSpec createTestTableSpec() {
        return TestTableSpec.builder()
                .tableName(TestTableSpec.randomTableName("TAP_OB_MYSQL_IT_"))
                .addField(TestFieldSpec.builder().name("id").dataType("BIGINT").testDataType(TestDataType.BIGINT).primaryKey(true).build())
                .addField(TestFieldSpec.builder().name("c_int").dataType("INT").testDataType(TestDataType.INT).build())
                .addField(TestFieldSpec.builder().name("c_varchar").dataType("VARCHAR(255)").testDataType(TestDataType.VARCHAR).build())
                .addField(TestFieldSpec.builder().name("c_decimal").dataType("DECIMAL(18,4)").testDataType(TestDataType.DECIMAL).build())
                .build();
    }

    @Override
    protected long streamReadTimeoutSeconds() {
        return 90L;
    }

    @Override
    protected boolean waitForStreamReadCatchUp() {
        return true;
    }

    @Override
    protected void prepareStreamReadTable() throws Exception {
        Thread.sleep(20000L);
    }

    @Override
    protected ConnectorTestContext createContext() throws Throwable {
        OceanbaseConnector connector = new OceanbaseConnector();
        DataMap config = readConnectionConfig("config/oceanbase-mysql-connection.json");
        config.put("port", Integer.parseInt(String.valueOf(config.get("port"))));
        config.put("useNativeCdc", Boolean.parseBoolean(String.valueOf(config.get("useNativeCdc"))));
        TapLog log = new TapLog();
        TapConnectorContext nodeContext = new TapConnectorContext(
                loadSpecification("oceanbase-spec.json"), config,
                DataMap.create().kv("enableTransaction", true), log);
        nodeContext.setStateMap(new TestStateMap());
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();
        connector.registerCapabilities(functions, codecRegistry);
        return ConnectorTestContext.builder()
                .connector(connector)
                .nodeContext(nodeContext)
                .connectorFunctions(functions)
                .codecRegistry(codecRegistry)
                .config(config)
                .log(log)
                .build();
    }
}
