package io.tapdata.kafka;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.utils.ErrorHelper;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.kafka.service.KafkaService;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 标准 Kafka 连接器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/19 14:21 Create
 */
@TapConnectorClass("spec_std_kafka.json")
public class StdKafkaConnector extends ConnectorBase {
    public static final String PDK_ID = "StdKafka";

    private IKafkaService kafkaService;
    protected KafkaConfig kafkaConfig;
    protected final AtomicBoolean stopping = new AtomicBoolean(false);

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        connectionContext.getLog().info("Starting {}", PDK_ID);
        kafkaConfig = KafkaConfig.valueOf(connectionContext);
        kafkaService = new KafkaService(kafkaConfig, stopping);
        isConnectorStarted(connectionContext, connectorContext -> {
            String firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = UUID.randomUUID().toString().replace("-", "");
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
        });
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        stopping.compareAndSet(false, true);
        connectionContext.getLog().info("Stopping {}", PDK_ID);
        ErrorHelper.closeWithNotNull(kafkaService);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        // TapValue Convert Capabilities
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue ->
            Optional.ofNullable(tapRawValue)
                .map(TapValue::getValue)
                .map(Object::toString)
                .orElse("null")
        );
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapYearValue.class, tapYearValue -> Optional.ofNullable(tapYearValue).map(TapValue::getOriginValue).orElse(null));

        // Common Capabilities
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        //  - support tableNames: includes/excludes tables, discoverSchema
        connectorFunctions.supportGetTableNamesFunction((nodeContext, batchSize, consumer) -> kafkaService.tableNames(batchSize, consumer));

        // Source Capabilities
        //  - support snapshot sync
        connectorFunctions.supportBatchCount((context, tapTable) -> kafkaService.batchCount(tapTable));
        connectorFunctions.supportBatchRead((context, table, offset, batchSize, consumer) -> kafkaService.batchRead(table, offset, batchSize, consumer));
        //  - support incremental sync
        connectorFunctions.supportStreamRead((context, tables, offset, batchSize, consumer) -> kafkaService.streamRead(tables, offset, batchSize, consumer));
        connectorFunctions.supportTimestampToStreamOffset((context, startTime) -> kafkaService.timestampToStreamOffset(startTime));

        // Target Capabilities
        connectorFunctions.supportWriteRecord((context, list, tapTable, consumer) -> kafkaService.writeRecord(list, tapTable, consumer));

        // DDL
        //  - createTable
        connectorFunctions.supportCreateTableV2((connectorContext, createTableEvent) -> kafkaService.createTable(createTableEvent));
        //  - dropTable
        connectorFunctions.supportDropTable((connectorContext, dropTableEvent) -> kafkaService.deleteTable(dropTableEvent));
        //  - js
        connectorFunctions.supportQueryByAdvanceFilter((connectorContext, filter, table, consumer) -> kafkaService.queryByAdvanceFilter(filter, table, consumer));
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        kafkaService.discoverSchema(tables, tableSize, consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        return new KafkaTester(connectionContext, consumer).start();
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        IKafkaAdminService adminService = kafkaService.getAdminService();
        Set<String> topics = adminService.listTopics();
        return topics.size();
    }
}
