package io.tapdata.connector.it;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.ConnectorTestContext;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.it.support.TestTableMap;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class FileITSupport {
    private FileITSupport() {
    }

    public static DataMap config(Path directory, String modelName) {
        return DataMap.create()
                .kv("protocol", "local")
                .kv("filePathString", directory.toAbsolutePath().toString())
                .kv("fileEncoding", "UTF-8")
                .kv("recursive", false)
                .kv("modelName", modelName)
                .kv("sheetLocation", "1")
                .kv("colLocation", "A~E")
                .kv("headerLine", 1)
                .kv("dataStartLine", 2)
                .kv("justString", false)
                .kv("streamReadReconnectInterval", 0)
                .kv("streamReadInterval", 1);
    }

    public static Session open(TapConnector connector, String specificationId, DataMap config) throws Throwable {
        TapLog log = new TapLog();
        TapConnectorContext nodeContext = new TapConnectorContext(specification(specificationId), config,
                DataMap.create(), log);
        nodeContext.setStateMap(new TestStateMap());
        nodeContext.setGlobalStateMap(new TestStateMap());
        TestTableMap tableMap = new TestTableMap();
        nodeContext.setTableMap(tableMap);
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();
        connector.registerCapabilities(functions, codecRegistry);
        ConnectorTestContext testContext = ConnectorTestContext.builder()
                .connector(connector)
                .nodeContext(nodeContext)
                .connectionContext(nodeContext)
                .connectorFunctions(functions)
                .codecRegistry(codecRegistry)
                .config(config)
                .log(log)
                .build();
        connector.init(nodeContext);
        return new Session(connector, nodeContext, tableMap, functions, testContext);
    }

    private static TapNodeSpecification specification(String id) {
        TapNodeSpecification specification = new TapNodeSpecification();
        specification.setId(id);
        specification.setName(id);
        specification.setVersion("it");
        return specification;
    }

    public static final class Session implements AutoCloseable {
        public final TapConnector connector;
        public final TapConnectorContext nodeContext;
        public final TestTableMap tableMap;
        public final ConnectorFunctions functions;
        public final ConnectorTestContext testContext;

        private Session(TapConnector connector, TapConnectorContext nodeContext, TestTableMap tableMap,
                        ConnectorFunctions functions, ConnectorTestContext testContext) {
            this.connector = connector;
            this.nodeContext = nodeContext;
            this.tableMap = tableMap;
            this.functions = functions;
            this.testContext = testContext;
        }

        public void register(TapTable table) {
            tableMap.put(table.getId(), table);
        }

        @Override
        public void close() throws Exception {
            try {
                connector.stop(nodeContext);
            } catch (Throwable error) {
                if (error instanceof Exception) {
                    throw (Exception) error;
                }
                throw new Exception(error);
            }
        }
    }

    public static final class Events {
        private final List<TapEvent> events = new CopyOnWriteArrayList<>();
        private final List<Object> offsets = new CopyOnWriteArrayList<>();

        public void accept(List<TapEvent> batch, Object offset) {
            if (batch != null) {
                events.addAll(batch);
            }
            offsets.add(offset);
        }

        public List<TapEvent> events() {
            return new ArrayList<>(events);
        }

        public List<Object> offsets() {
            return new ArrayList<>(offsets);
        }
    }
}
