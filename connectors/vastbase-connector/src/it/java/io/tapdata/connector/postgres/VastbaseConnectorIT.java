package io.tapdata.connector.postgres;

import io.tapdata.connector.postgres.VastbaseConnector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDouble;
import io.tapdata.entity.schema.type.TapFloat;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.it.config.ConnectionConfigLoader;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.it.support.TestTableMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class VastbaseConnectorIT {

    private final String tableName = "tap_vastbase_float_it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    private final List<TapEvent> events = new ArrayList<>();
    private VastbaseConnector connector;
    private TapConnectorContext connectorContext;
    private ConnectorFunctions functions;
    private DataMap config;
    private boolean tableCreated;

    @BeforeAll
    void startConnector() throws Throwable {
        config = loadConfig();
        config.put("port", Integer.parseInt(String.valueOf(config.get("port"))));
        connector = new VastbaseConnector();
        TapLog log = new TapLog();
        connectorContext = new TapConnectorContext(specification(), config,
                DataMap.create().kv("enableTransaction", true), log);
        connectorContext.setStateMap(new TestStateMap());
        connectorContext.setTableMap(new TestTableMap());
        functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, TapCodecsRegistry.create());
        connectorContext.setConnectorCapabilities(ConnectorCapabilities.create()
                .alternative("dml_insert_policy", "just_insert")
                .alternative("dml_update_policy", "ignore_on_nonexists")
                .alternative("dml_delete_policy", "ignore_on_nonexists"));
        connector.init(connectorContext);
    }

    @AfterAll
    void stopConnector() throws Throwable {
        try {
            if (tableCreated) {
                dropTable();
            }
        } finally {
            if (connector != null && connectorContext != null) {
                connector.stop(connectorContext);
            }
        }
    }

    @Test
    void should_round_trip_real_and_double_precision_values() throws Throwable {
        createTable();
        tableCreated = true;
        insertRow(1L, 1.25F, 2.718281828459045D, "first");

        List<TapTable> discovered = new ArrayList<>();
        connector.discoverSchema(connectorContext, Collections.singletonList(tableName), 100, discovered::addAll);
        TapTable table = discovered.stream().filter(item -> tableName.equals(item.getId())).findFirst()
                .orElseThrow(() -> new AssertionError("VastBase table was not discovered: " + tableName));
        TapField floatField = table.getNameFieldMap().get("c_real");
        TapField doubleField = table.getNameFieldMap().get("c_double");
        assertNotNull(floatField);
        assertNotNull(doubleField);
        assertTrue(floatField.getTapType() instanceof TapFloat, "REAL must map to TapFloat");
        assertTrue(doubleField.getTapType() instanceof TapDouble, "DOUBLE PRECISION must map to TapDouble");

        events.clear();
        functions.getBatchReadFunction().batchRead(connectorContext, table, null, 100,
                (batch, offset) -> events.addAll(batch));
        Map<String, Object> first = rowById(1L);
        assertEquals(1.25D, number(first, "c_real"), 0.0001D);
        assertEquals(2.718281828459045D, number(first, "c_double"), 0.000000000000001D);

        Map<String, Object> second = new HashMap<>();
        second.put("id", 2L);
        second.put("c_real", 3.5F);
        second.put("c_double", 6.283185307179586D);
        second.put("c_text", "written-by-connector");
        TapInsertRecordEvent event = TapInsertRecordEvent.create().table(tableName).after(second);
        AtomicReference<WriteListResult<TapRecordEvent>> result = new AtomicReference<>();
        functions.getWriteRecordFunction().writeRecord(connectorContext, Collections.singletonList(event), table, result::set);
        assertNotNull(result.get());
        assertEquals(1L, result.get().getInsertedCount());
        assertTrue(result.get().getErrorMap() == null || result.get().getErrorMap().isEmpty());

        events.clear();
        functions.getBatchReadFunction().batchRead(connectorContext, table, null, 100,
                (batch, offset) -> events.addAll(batch));
        Map<String, Object> written = rowById(2L);
        assertEquals(3.5D, number(written, "c_real"), 0.0001D);
        assertEquals(6.283185307179586D, number(written, "c_double"), 0.000000000000001D);
    }

    private DataMap loadConfig() throws Exception {
        return ConnectionConfigLoader.load("config/vastbase-connection.json");
    }

    private void createTable() throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE \"public\".\"" + tableName + "\" ("
                    + "\"id\" BIGINT PRIMARY KEY, \"c_real\" REAL NOT NULL, \"c_double\" DOUBLE PRECISION NOT NULL, \"c_text\" VARCHAR(64))");
        }
    }

    private void insertRow(long id, float single, double twice, String text) throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute(String.format("INSERT INTO \"public\".\"%s\" VALUES (%d, %s, %s, '%s')",
                    tableName, id, Float.toString(single), Double.toString(twice), text));
        }
    }

    private void dropTable() throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS \"public\".\"" + tableName + "\"");
        }
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection("jdbc:postgresql://" + config.getString("host") + ":"
                        + config.getInteger("port") + "/" + config.getString("database"),
                config.getString("user"), config.getString("password"));
    }

    private Map<String, Object> rowById(long id) {
        return events.stream()
                .filter(event -> event instanceof TapInsertRecordEvent)
                .map(event -> ((TapInsertRecordEvent) event).getAfter())
                .filter(row -> id == ((Number) row.get("id")).longValue())
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing VastBase row " + id));
    }

    private double number(Map<String, Object> row, String field) {
        return ((Number) row.get(field)).doubleValue();
    }

    private static TapNodeSpecification specification() {
        TapNodeSpecification specification = new TapNodeSpecification();
        specification.setName("VastBase");
        specification.setId("vastbase");
        Map<String, DataMap> mappings = new LinkedHashMap<>();
        mappings.put("real", DataMap.create()
                .kv("to", "TapFloat")
                .kv("precision", Arrays.asList(1, 6))
                .kv("scale", Arrays.asList(0, 6))
                .kv("fixed", false));
        mappings.put("double precision", DataMap.create()
                .kv("to", "TapDouble")
                .kv("precision", Arrays.asList(1, 17))
                .kv("scale", Arrays.asList(0, 17))
                .kv("fixed", false));
        specification.setDataTypesMap(DefaultExpressionMatchingMap.map(mappings));
        return specification;
    }
}
