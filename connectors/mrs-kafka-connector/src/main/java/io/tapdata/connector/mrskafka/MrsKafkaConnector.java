package io.tapdata.connector.mrskafka;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.mrskafka.admin.Admin;
import io.tapdata.connector.mrskafka.admin.DefaultAdmin;
import io.tapdata.connector.mrskafka.config.MrsAdminConfiguration;
import io.tapdata.connector.mrskafka.config.MrsKafkaConfig;
import io.tapdata.connector.mrskafka.core.MrsKafkaService;
import io.tapdata.connector.mrskafka.exception.MrsKafkaExceptionController;
import io.tapdata.connector.mrskafka.util.LoginUtil;
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
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.tapdata.connector.mrskafka.util.LoginUtil.*;
import static io.tapdata.pdk.apis.entity.ConnectionOptions.*;

@TapConnectorClass("spec_mrsKafka.json")
public class MrsKafkaConnector extends ConnectorBase {
    public static final String TAG = MrsKafkaConnector.class.getSimpleName();
    private MrsKafkaConfig mrsKafkaConfig;

    private MrsKafkaService mrsKafkaService;
    private MrsKafkaExceptionController mrsKafkaExceptionController;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws IOException {
        initConnection(connectionContext);
//        initConnection(connectionContext);
    }

    //    private void initConnection(TapConnectionContext connectorContext) throws IOException {
//        mrsKafkaConfig = (MrsKafkaConfig) new MrsKafkaConfig().load(connectorContext.getConnectionConfig());
////        String krb5Path = Krb5Util.saveByCatalog("connections-" + connectorContext.getId(), mrsKafkaConfig.getHuaweiKeytab(), mrsKafkaConfig.getHuaweiKrb5Conf(), true);
////        String confFilePath = confPath(krb5Path);
////        String keytabPath = keytabPath(krb5Path);
////        HuaweiKerberosUtil.setKrb5Config(confFilePath);
////        HuaweiKerberosUtil.setZookeeperServerPrincipal(mrsKafkaConfig.getHuaweiZookeeperPrincipal());
////        HuaweiKerberosUtil.setJaasFile("connections-" + connectorContext.getId(),mrsKafkaConfig.getHuaweiPrincipal(),keytabPath);
//        LoginUtil.securityPrepare("tapdata", "user.keytab");
////        LoginUtil.setJaasFile(mrsKafkaConfig.getHuaweiPrincipal(),keytabPath);
//        Properties props = initProperties();
//        KafkaProducer kafkaProducer=new KafkaProducer(props);
////        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test123", "123", "bcd");
////        try{
////            kafkaProducer.send(record).get();
////        }catch (Exception e){
////
////        }
//    }
    public static Properties initProperties() {

        Properties props = new Properties();

        // Broker地址列表
        props.put(BOOTSTRAP_SERVER, "192.168.100.11:21007,192.168.100.12:21007,192.168.100.13:21007");
        // 客户端ID
        props.put(CLIENT_ID, "producer");
        // Key序列化类
        props.put(KEY_SERIALIZER,
                "org.apache.kafka.common.serialization.StringSerializer");
        // Value序列化类
        props.put(VALUE_SERIALIZER,
                "org.apache.kafka.common.serialization.StringSerializer");
        // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
        props.put(SECURITY_PROTOCOL, "SASL_PLAINTEXT");
        // 服务名
        props.put(SASL_KERBEROS_SERVICE_NAME, "kafka");
        // 域名
        props.put(KERBEROS_DOMAIN_NAME, "hadoop.hadoop.com");
        return props;
    }

    private void initConnection(TapConnectionContext connectorContext) {
        mrsKafkaConfig = (MrsKafkaConfig) new MrsKafkaConfig().load(connectorContext.getConnectionConfig());
        mrsKafkaConfig.load(connectorContext.getNodeConfig());
        mrsKafkaExceptionController = new MrsKafkaExceptionController();
        mrsKafkaService = new MrsKafkaService(mrsKafkaConfig, connectorContext.getId());
        try {
            mrsKafkaService.init();
        } catch (Throwable t) {
            mrsKafkaExceptionController.collectTerminateByServer(t);
            mrsKafkaExceptionController.collectUserPwdInvalid(mrsKafkaConfig.getMqUsername(), t);
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        if (mrsKafkaService != null) {
            mrsKafkaService.close();
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));

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
        DataMap nodeConfig = tapConnectorContext.getNodeConfig();
        Integer replicasSize = (Integer) nodeConfig.get("replicasSize");
        Integer partitionNum = (Integer) nodeConfig.get("partitionNum");
        MrsAdminConfiguration configuration = new MrsAdminConfiguration(mrsKafkaConfig, tapConnectorContext.getId());
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


    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        try {
            mrsKafkaService.produce(tapFieldBaseEvent);
        } catch (Throwable t) {
            mrsKafkaExceptionController.collectTerminateByServer(t);
            mrsKafkaExceptionController.collectUserPwdInvalid(mrsKafkaConfig.getMqUsername(), t);
            mrsKafkaExceptionController.collectWritePrivileges("writeRecord", Collections.emptyList(), t);
        }
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        mrsKafkaService.loadTables(tableSize, consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        mrsKafkaConfig = (MrsKafkaConfig) new MrsKafkaConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(mrsKafkaConfig.getConnectionString());
        try {
            onStart(connectionContext);
            CommonDbConfig config = new CommonDbConfig();
            config.set__connectionType(mrsKafkaConfig.get__connectionType());
            MrsKafkaTest mrsKafkaTest = new MrsKafkaTest(mrsKafkaConfig, consumer, this.mrsKafkaService, config);
            mrsKafkaTest.testOneByOne();
        } catch (Throwable throwable) {
            TapLogger.error(TAG, throwable.getMessage());
            mrsKafkaExceptionController.collectTerminateByServer(throwable);
            mrsKafkaExceptionController.collectUserPwdInvalid(mrsKafkaConfig.getMqUsername(), throwable);
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
        return mrsKafkaService.countTables();
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        try {
            if (mrsKafkaConfig.getEnableScript()) {
                mrsKafkaService.produce(connectorContext, tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
            } else {
                mrsKafkaService.produce(tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
            }
        } catch (Throwable t) {
            mrsKafkaExceptionController.collectTerminateByServer(t);
            mrsKafkaExceptionController.collectWritePrivileges("writeRecord", Collections.emptyList(), t);
        }
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        try {
            mrsKafkaService.consumeOne(tapTable, eventBatchSize, eventsOffsetConsumer);
        } catch (Throwable e) {
            mrsKafkaExceptionController.collectTerminateByServer(e);
            mrsKafkaExceptionController.collectUserPwdInvalid(mrsKafkaConfig.getMqUsername(), e);
            mrsKafkaExceptionController.collectReadPrivileges("batchRead", Collections.emptyList(), e);
        }
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        try {
            mrsKafkaService.streamConsume(tableList, recordSize, consumer);
        } catch (Throwable e) {
            mrsKafkaExceptionController.collectTerminateByServer(e);
            mrsKafkaExceptionController.collectUserPwdInvalid(mrsKafkaConfig.getMqUsername(), e);
            mrsKafkaExceptionController.collectReadPrivileges("streamRead", Collections.emptyList(), e);
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return TapSimplify.list();
    }

    private void checkConnection(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) {
        ConnectionCheckItem testPing = mrsKafkaService.testPing();
        consumer.accept(testPing);
        if (testPing.getResult() == ConnectionCheckItem.RESULT_FAILED) {
            return;
        }
        ConnectionCheckItem testConnection = mrsKafkaService.testConnection();
        consumer.accept(testConnection);
    }
}
