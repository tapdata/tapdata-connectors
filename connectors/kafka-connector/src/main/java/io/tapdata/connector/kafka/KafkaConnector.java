package io.tapdata.connector.kafka;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.kafka.admin.Admin;
import io.tapdata.connector.kafka.admin.DefaultAdmin;
import io.tapdata.connector.kafka.config.AdminConfiguration;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.tapdata.pdk.apis.entity.ConnectionOptions.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

@TapConnectorClass("spec_kafka.json")
public class KafkaConnector extends ConnectorBase {
    public static final String TAG = KafkaConnector.class.getSimpleName();

    private KafkaService kafkaService;
    private KafkaSRService kafkaSRService;
    private KafkaProducer<String, GenericRecord> kafkaProducer;
    private KafkaConfig kafkaConfig;
    private Boolean isSchemaRegister;
    private Properties properties;
    private KafkaExceptionCollector kafkaExceptionCollector;

    private void initConnection(TapConnectionContext connectorContext) {
        kafkaConfig = (KafkaConfig) new KafkaConfig().load(connectorContext.getConnectionConfig());
        kafkaConfig.load(connectorContext.getNodeConfig());
        this.kafkaExceptionCollector = new KafkaExceptionCollector();
        this.isSchemaRegister = kafkaConfig.getSchemaRegister();
        try {
            kafkaService = new KafkaService(kafkaConfig, connectorContext.getLog());
            kafkaService.setConnectorId(connectorContext.getId());
            kafkaService.init();
            if (this.isSchemaRegister) {
                schemaRegisterBuild();
                kafkaSRService = new KafkaSRService(kafkaConfig, connectorContext, kafkaProducer);
                kafkaSRService.setTapLogger(connectorContext.getLog());
                kafkaSRService.setConnectorId(connectorContext.getId());
                kafkaSRService.init();
            }
        } catch (Throwable t) {
            kafkaExceptionCollector.collectTerminateByServer(t);
            kafkaExceptionCollector.collectUserPwdInvalid(kafkaConfig.getMqUsername(), t);
            throw t;
        }
    }

    private void schemaRegisterBuild() {
        properties = new Properties();
        properties.put("bootstrap.servers", kafkaConfig.getNameSrvAddr());
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put("schema.registry.url", "" + "http://" + kafkaConfig.getSchemaRegisterUrl());
        if (kafkaConfig.getBasicAuth()) {
            properties.put("basic.auth.credentials.source", kafkaConfig.getAuthCredentialsSource());
            properties.put("basic.auth.user.info", kafkaConfig.getAuthUserName() + ":" + kafkaConfig.getAuthPassword());
        }
        if (EmptyKit.isNotEmpty(this.kafkaConfig.getMqUsername()) && EmptyKit.isNotEmpty(this.kafkaConfig.getMqPassword())) {
            properties.put("security.protocol", "SASL_PLAINTEXT");
            String saslMechanism;
            String model;
            switch (kafkaConfig.getKafkaSaslMechanism().toUpperCase()) {
                case "PLAIN":
                    saslMechanism = "PLAIN";
                    model = "org.apache.kafka.common.security.plain.PlainLoginModule";
                    break;
                case "SHA256":
                    saslMechanism = "SCRAM-SHA-256";
                    model = "org.apache.kafka.common.security.scram.ScramLoginModule";
                    break;
                case "SHA512":
                    saslMechanism = "SCRAM-SHA-512";
                    model = "org.apache.kafka.common.security.scram.ScramLoginModule";
                    break;
                default:
                    throw new IllegalArgumentException("Un-supported sasl.mechanism: " + kafkaConfig.getKafkaSaslMechanism().toUpperCase());
            }
            properties.put("sasl.mechanism", saslMechanism);
            properties.put("sasl.jaas.config", model + " required " +
                    "username='" + this.kafkaConfig.getMqUsername() +
                    "' password='" + this.kafkaConfig.getMqPassword() + "';");
        }
        kafkaProducer = new KafkaProducer<>(properties);
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        if (kafkaService != null) {
            kafkaService.close();
        }
        if (this.isSchemaRegister) {
            kafkaSRService.close();
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportConnectionCheckFunction(this::checkConnection);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);

        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        String tableId = tapCreateTableEvent.getTableId();
        CreateTableOptions createTableOptions = new CreateTableOptions();
//        if (!this.isSchemaRegister) {
        Integer replicasSize = Optional.ofNullable(kafkaConfig.getReplicasSize()).orElse(1);
        Integer partitionNum = Optional.ofNullable(kafkaConfig.getPartitionNum()).orElse(3);
        AdminConfiguration configuration = new AdminConfiguration(kafkaConfig, tapConnectorContext.getId());
        try (Admin admin = new DefaultAdmin(configuration)) {
            Set<String> existTopics = admin.listTopics();
            if (!existTopics.contains(tableId)) {
                createTableOptions.setTableExists(false);
                admin.createTopics(tableId, partitionNum, replicasSize.shortValue());
            } else {
                List<TopicPartitionInfo> topicPartitionInfos = admin.getTopicPartitionInfo(tableId);
                int existTopicPartition = topicPartitionInfos.size();
                int existReplicasSize = topicPartitionInfos.get(0).replicas().size();
                if (existReplicasSize != replicasSize) {
                    TapLogger.warn(TAG, "cannot change the number of replicasSize of an existing table, will skip");
                }
                if (partitionNum <= existTopicPartition) {
                    TapLogger.warn(TAG, "The number of partitions set is less than or equal to the number of partitions of the existing table，will skip");
                } else {
                    admin.increaseTopicPartitions(tapCreateTableEvent.getTableId(), partitionNum);
                }
                createTableOptions.setTableExists(true);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Create Table " + tableId + " Failed | Error: " + e.getMessage());
        }
//        }else{
//            createTableOptions.setTableExists(true);
//        }
        return createTableOptions;
    }


//    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws ExecutionException, InterruptedException, IOException {
//        TapTable tapTable = tapCreateTableEvent.getTable();
//        CreateTableOptions createTableOptions = new CreateTableOptions();
//        if (this.isSchemaRegister) {
//            if (checkTopicExists(kafkaConfig.getConnectionString(), tapTable.getId())) {
//                createTableOptions.setTableExists(true);
//                return createTableOptions;
//            }
//            AdminClient adminClient = null;
//            try {
////                properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getConnectionString());
//                adminClient = AdminClient.create(properties);
//                int numPartitions = 3;
//                short replicationFactor = 1;
//                NewTopic newTopic = new NewTopic(tapTable.getId(), numPartitions, replicationFactor);
//                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
//            } catch (InterruptedException e) {
//                throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
//            } finally {
//                adminClient.close();
//            }
//            createTableOptions.setTableExists(false);
//        } else {
//            createTableOptions.setTableExists(true);
//        }
//
//        return createTableOptions;
//    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        try {
            if (Boolean.TRUE.equals(this.kafkaConfig.getEnableCustomDDLMessage())) {
                kafkaService.produceCustomDDLRecord(tapFieldBaseEvent);
            } else {
                kafkaService.produceDDLRecord(tapFieldBaseEvent);
            }
        } catch (Throwable t) {
            kafkaExceptionCollector.collectTerminateByServer(t);
            kafkaExceptionCollector.collectUserPwdInvalid(kafkaConfig.getMqUsername(), t);
            kafkaExceptionCollector.collectWritePrivileges("ddl", Collections.emptyList(), t);
            throw t;
        }
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        if (!this.isSchemaRegister) {
            if (Boolean.TRUE.equals(kafkaConfig.getEnableCustomParse())) {
                kafkaService.loadTables(tables, tableSize, consumer);
            } else {
                kafkaService.loadTables(tableSize, consumer);
            }
        } else {
            kafkaSRService.loadTables(tableSize, consumer);
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        kafkaConfig = (KafkaConfig) new KafkaConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(kafkaConfig.getConnectionString());
        try {
            onStart(connectionContext);
            CommonDbConfig config = new CommonDbConfig();
            config.set__connectionType(kafkaConfig.get__connectionType());
            KafkaTest kafkaTest = new KafkaTest(kafkaConfig, consumer, this.kafkaService, config, isSchemaRegister, kafkaSRService);
            kafkaTest.testOneByOne();
        } catch (Throwable throwable) {
            TapLogger.error(TAG, throwable.getMessage());
            kafkaExceptionCollector.collectTerminateByServer(throwable);
            kafkaExceptionCollector.collectUserPwdInvalid(kafkaConfig.getMqUsername(), throwable);
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
        return kafkaService.countTables();
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        try {
            if (!this.isSchemaRegister) {
                if (kafkaConfig.getEnableScript()) {
                    kafkaService.produce(connectorContext, tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
                } else {
                    kafkaService.produce(tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
                }
            } else {
                if (kafkaConfig.getEnableScript()) {
                    throw new RuntimeException("Custom message is not support in schema register");
                } else {
                    kafkaSRService.produce(tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
                }
            }
        } catch (Throwable t) {
            kafkaExceptionCollector.collectTerminateByServer(t);
            kafkaExceptionCollector.collectWritePrivileges("writeRecord", Collections.emptyList(), t);
            throw t;
        }

    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        try {
            kafkaService.consumeOne(tapTable, eventBatchSize, eventsOffsetConsumer);
        } catch (Throwable e) {
            kafkaExceptionCollector.collectTerminateByServer(e);
            kafkaExceptionCollector.collectUserPwdInvalid(kafkaConfig.getMqUsername(), e);
            kafkaExceptionCollector.collectReadPrivileges("batchRead", Collections.emptyList(), e);
            throw e;
        }
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        try {
            kafkaService.streamConsume(tableList, recordSize, consumer);
        } catch (Throwable e) {
            kafkaExceptionCollector.collectTerminateByServer(e);
            kafkaExceptionCollector.collectUserPwdInvalid(kafkaConfig.getMqUsername(), e);
            kafkaExceptionCollector.collectReadPrivileges("streamRead", Collections.emptyList(), e);
            throw e;
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return TapSimplify.list();
    }

    private void checkConnection(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) {
        ConnectionCheckItem testPing = kafkaService.testPing();
        consumer.accept(testPing);
        if (testPing.getResult() == ConnectionCheckItem.RESULT_FAILED) {
            return;
        }
        ConnectionCheckItem testConnection = kafkaService.testConnection();
        consumer.accept(testConnection);
    }

}
