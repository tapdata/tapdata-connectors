package io.tapdata.kafka;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.utils.ErrorHelper;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.kafka.service.KafkaService;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.tapdata.pdk.apis.entity.ConnectionOptions.*;

/**
 * 标准 Kafka 连接器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/19 14:21 Create
 */
public class KafkaEnhancedCoreConnector extends ConnectorBase {
    public static final String PDK_ID = "kafka_enhanced";

    protected IKafkaService kafkaService;
    protected KafkaConfig kafkaConfig;
    protected final AtomicBoolean stopping = new AtomicBoolean(false);
    protected Map<String, KafkaProducer<Object, Object>> producerMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        connectionContext.getLog().info("Starting {}", PDK_ID);
        isConnectorStarted(connectionContext, connectorContext -> {
            String firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = UUID.randomUUID().toString().replace("-", "");
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
            kafkaConfig = KafkaConfig.valueOf(connectionContext, firstConnectorId);
        });
        if (!(connectionContext instanceof TapConnectorContext)) {
            kafkaConfig = KafkaConfig.valueOf(connectionContext, "");
        }
        stopping.compareAndSet(true, false);
        kafkaService = new KafkaService(kafkaConfig, stopping, schemaModeOverrides());
    }

    /**
     * 子类可重写返回 schema mode 的工厂覆盖表，作用域仅限本连接器实例，
     * 不会污染同 JVM 内其他 Kafka 连接器实例。默认无覆盖。
     */
    protected Map<KafkaSchemaMode, AbsSchemaMode.Factory> schemaModeOverrides() {
        return Collections.emptyMap();
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        stopping.compareAndSet(false, true);
        connectionContext.getLog().info("Stopping {}", PDK_ID);
        // 清理事务 producer，避免泄漏；进行中的事务先 abort（吞异常），再 close
        producerMap.values().forEach(p -> {
            try { p.abortTransaction(); } catch (Exception ignore) {}
            try { p.close(); } catch (Exception e) {
                connectionContext.getLog().warn("Close transaction producer failed: {}", e.getMessage());
            }
        });
        producerMap.clear();
        ErrorHelper.closeWithNotNull(kafkaService);
        kafkaService = null;
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
        connectorFunctions.supportWriteRecord(this::writeRecord);

        // DDL
        //  - createTable
        connectorFunctions.supportCreateTableV2((connectorContext, createTableEvent) -> kafkaService.createTable(createTableEvent));
        //  - dropTable
        connectorFunctions.supportDropTable((connectorContext, dropTableEvent) -> kafkaService.deleteTable(dropTableEvent));
        //  - js
        connectorFunctions.supportQueryByAdvanceFilter((connectorContext, filter, table, consumer) -> kafkaService.queryByAdvanceFilter(filter, table, consumer));
        connectorFunctions.supportTransactionBeginFunction(this::beginTransaction);
        connectorFunctions.supportTransactionCommitFunction(this::commitTransaction);
        connectorFunctions.supportTransactionRollbackFunction(this::rollbackTransaction);
        connectorFunctions.supportAlterTableTTLFunction((connectorContext, alterTableTTLEvent) -> kafkaService.alterTableTTL(alterTableTTLEvent));
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        kafkaService.discoverSchema(tables, tableSize, consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        try {
            onStart(connectionContext);
            connectionOptions.connectionString(kafkaConfig.getConnectionClusterURI());
            try (KafkaEnhancedTest kafkaTest = new KafkaEnhancedTest(kafkaConfig, consumer, connectionContext.getLog())) {
                kafkaTest.testOneByOne();
            }
        } catch (Throwable throwable) {
//            TapLogger.error(TAG, throwable.getMessage());
//            kafkaExceptionCollector.collectTerminateByServer(throwable);
//            kafkaExceptionCollector.collectUserPwdInvalid(kafkaConfig.getMqUsername(), throwable);
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Failed, " + throwable.getMessage()));
        } finally {
            onStop(connectionContext);
        }
        List<Capability> ddlCapabilities = Arrays.asList(
                Capability.create(DDL_NEW_FIELD_EVENT).type(Capability.TYPE_DDL),
                Capability.create(DDL_ALTER_FIELD_NAME_EVENT).type(Capability.TYPE_DDL),
                Capability.create(DDL_ALTER_FIELD_ATTRIBUTES_EVENT).type(Capability.TYPE_DDL),
                Capability.create(DDL_DROP_FIELD_EVENT).type(Capability.TYPE_DDL));
        ddlCapabilities.forEach(connectionOptions::capability);
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        IKafkaAdminService adminService = kafkaService.getAdminService();
        Set<String> topics = adminService.listTopics();
        return topics.size();
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        // 当前线程若已开启事务，使用对应的 transactional producer；否则走普通 producer
         KafkaProducer<Object, Object> producer = producerMap.get(Thread.currentThread().getName());
        if (null != producer) {
            kafkaService.writeRecord(producer, tapRecordEvents, tapTable, writeListResultConsumer);
        } else {
            kafkaService.writeRecord(tapRecordEvents, tapTable, writeListResultConsumer);
        }
    }

    protected void beginTransaction(TapConnectorContext connectorContext) {
        // 同一线程的 transactional producer 在整个任务生命周期内复用：
        // initTransactions() 是 broker 协调器重操作，每个 producer 实例只能、且只需调一次
        producerMap.computeIfAbsent(Thread.currentThread().getName(), k -> {
            KafkaProducer<Object, Object> n = kafkaService.getTransactionProducer();
            n.initTransactions();
            return n;
        }).beginTransaction();
    }

    protected void commitTransaction(TapConnectorContext connectorContext) {
        KafkaProducer<Object, Object> producer = producerMap.get(Thread.currentThread().getName());
        if (null == producer) return;
        try {
            producer.commitTransaction();
        } catch (org.apache.kafka.common.errors.ProducerFencedException
                 | org.apache.kafka.common.errors.OutOfOrderSequenceException
                 | org.apache.kafka.common.errors.AuthorizationException e) {
            // 不可恢复错误：producer 必须关闭重建
            closeAndRemoveProducer(producer);
            throw e;
        }
    }

    protected void rollbackTransaction(TapConnectorContext connectorContext) {
        KafkaProducer<Object, Object> producer = producerMap.get(Thread.currentThread().getName());
        if (null == producer) return;
        try {
            producer.abortTransaction();
        } catch (org.apache.kafka.common.errors.ProducerFencedException
                 | org.apache.kafka.common.errors.OutOfOrderSequenceException
                 | org.apache.kafka.common.errors.AuthorizationException e) {
            closeAndRemoveProducer(producer);
            throw e;
        }
    }

    private void closeAndRemoveProducer(KafkaProducer<Object, Object> producer) {
        producerMap.values().remove(producer);
        try { producer.close(); } catch (Exception ignore) {}
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        try {
            kafkaService.processDDL(tapFieldBaseEvent);
        } catch (Throwable t) {
//            kafkaExceptionCollector.collectTerminateByServer(t);
//            kafkaExceptionCollector.collectUserPwdInvalid(kafkaConfig.getMqUsername(), t);
//            kafkaExceptionCollector.collectWritePrivileges("ddl", Collections.emptyList(), t);
            throw t;
        }
    }
}
