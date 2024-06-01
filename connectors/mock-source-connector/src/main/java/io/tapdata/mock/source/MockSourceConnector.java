package io.tapdata.mock.source;

import io.tapdata.dummy.DummyConnector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

@TapConnectorClass("spec_mock_source.json")
public class MockSourceConnector extends DummyConnector {
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        super.onStart(connectionContext);
        connectionContext.getLog().info("Start mock source connector");
    }
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        // support initial sync
        connectorFunctions.supportBatchCount(this::supportBatchCount);
        connectorFunctions.supportBatchRead(this::supportBatchRead);
        // support incremental sync
        connectorFunctions.supportStreamRead(this::supportStreamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::supportTimestampToStreamOffset);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);

        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
    }

}
