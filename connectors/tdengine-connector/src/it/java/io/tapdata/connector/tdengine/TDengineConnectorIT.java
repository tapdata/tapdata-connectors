package io.tapdata.connector.tdengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDouble;
import io.tapdata.entity.schema.type.TapFloat;
import io.tapdata.entity.utils.DataMap;
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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TDengineConnectorIT {

    private final String tableName = "tap_tdengine_float_it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    private final List<TapEvent> events = new ArrayList<>();
    private TDengineConnector connector;
    private TapConnectorContext connectorContext;
    private ConnectorFunctions functions;
    private DataMap config;
    private boolean tableCreated;

    @BeforeAll
    void startConnector() throws Throwable {
        config = ConnectionConfigLoader.load("config/tdengine-connection.json");
        config.put("port", Integer.parseInt(String.valueOf(config.get("port"))));
        connector = new TDengineConnector();
        TapLog log = new TapLog();
        connectorContext = new TapConnectorContext(specification("tdengine-spec.json"), config,
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
    void should_round_trip_single_and_double_precision_values() throws Throwable {
        createTable();
        tableCreated = true;
        insertRow(new Timestamp(System.currentTimeMillis()), 1.25F, 2.718281828459045D, "first");

        List<TapTable> discovered = new ArrayList<>();
        connector.discoverSchema(connectorContext, Collections.singletonList(tableName), 100, discovered::addAll);
        TapTable table = discovered.stream().filter(item -> tableName.equals(item.getId())).findFirst()
                .orElseThrow(() -> new AssertionError("TDengine table was not discovered: " + tableName));

        TapField floatField = table.getNameFieldMap().get("c_float");
        TapField doubleField = table.getNameFieldMap().get("c_double");
        assertNotNull(floatField);
        assertNotNull(doubleField);
        assertTrue(floatField.getTapType() instanceof TapFloat,
                "FLOAT must map to TapFloat, dataType=" + floatField.getDataType() + ", tapType=" + floatField.getTapType());
        assertTrue(doubleField.getTapType() instanceof TapDouble,
                "DOUBLE must map to TapDouble, dataType=" + doubleField.getDataType() + ", tapType=" + doubleField.getTapType());

        events.clear();
        functions.getBatchReadFunction().batchRead(connectorContext, table, null, 100,
                (batch, offset) -> events.addAll(batch));
        Map<String, Object> first = rowByText("first");
        assertEquals(1.25D, number(first, "c_float"), 0.0001D);
        assertEquals(2.718281828459045D, number(first, "c_double"), 0.000000000000001D);

        Map<String, Object> second = new HashMap<>();
        second.put("ts", new Timestamp(System.currentTimeMillis() + 1000L));
        second.put("c_float", 3.5F);
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
        Map<String, Object> written = rowByText("written-by-connector");
        assertEquals(3.5D, number(written, "c_float"), 0.0001D);
        assertEquals(6.283185307179586D, number(written, "c_double"), 0.000000000000001D);
    }

    private void createTable() throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE `" + config.getString("database") + "`.`" + tableName + "` ("
                    + "ts TIMESTAMP, c_float FLOAT, c_double DOUBLE, c_text VARCHAR(64))");
        }
    }

    private void insertRow(Timestamp timestamp, float single, double twice, String text) throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute(String.format("INSERT INTO `%s`.`%s` VALUES ('%s', %s, %s, '%s')",
                    config.getString("database"), tableName, timestamp, Float.toString(single), Double.toString(twice), text));
        }
    }

    private void dropTable() throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS `" + config.getString("database") + "`.`" + tableName + "`");
        }
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection("jdbc:TAOS-RS://" + config.getString("host") + ":"
                        + config.getInteger("port") + "/" + config.getString("database"),
                config.getString("user"), config.getString("password"));
    }

    private Map<String, Object> rowByText(String text) {
        return events.stream()
                .filter(event -> event instanceof TapInsertRecordEvent)
                .map(event -> ((TapInsertRecordEvent) event).getAfter())
                .filter(row -> text.equals(row.get("c_text")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing TDengine row " + text));
    }

    private double number(Map<String, Object> row, String field) {
        return ((Number) row.get(field)).doubleValue();
    }

    private static TapNodeSpecification specification(String resource) throws Exception {
        try (InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalArgumentException("missing specification resource: " + resource);
            }
            JsonNode root = new ObjectMapper().readTree(input);
            JsonNode properties = root.path("properties");
            TapNodeSpecification specification = new TapNodeSpecification();
            specification.setName(properties.path("name").asText());
            specification.setRealName(properties.path("realName").asText());
            specification.setId(properties.path("id").asText());
            JsonNode dataTypes = root.get("dataTypes");
            if (dataTypes != null && dataTypes.isObject()) {
                specification.setDataTypesMap(DefaultExpressionMatchingMap.map(dataTypes.toString()));
            }
            return specification;
        }
    }
}
