package io.tapdata.mock.target;

import io.tapdata.dummy.DummyConnector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.*;
import java.util.function.Consumer;

@TapConnectorClass("spec_mock_target.json")
public class MockTargetConnector extends DummyConnector {
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        super.onStart(connectionContext);
        connectionContext.getLog().info("Start mock target connector");
    }
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);

        // target DDL
//        connectorFunctions.supportCreateTable(this::supportCreateTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
//        connectorFunctions.supportCreateIndex(this::supportCreateIndex);
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        // target DML
        connectorFunctions.supportWriteRecord(this::supportWriteRecord);

        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);

    }
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        // mock schema
        Map<String, TapTable> schemas = new HashMap<>();
        schemas.put("mock_target_test",new TapTable("mock_target_test","mock_target_test"));
        List<TapTable> tableSchemas = new ArrayList<>();
        tableSchemas.addAll(schemas.values());

        consumer.accept(tableSchemas);
    }
    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 1;
    }
    @Override
    protected void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        List<String> batchList = new ArrayList<>();
        batchList.add("mock_target_test");
        listConsumer.accept(batchList);
    }

}
