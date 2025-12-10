package io.tapdata.kafka;

import io.tapdata.connector.utils.ConcurrentUtils;
import io.tapdata.constant.DMLType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.kafka.schema_mode.*;
import io.tapdata.kafka.utils.KafkaUtils;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Kafka 结构模式服务接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/3 17:00 Create
 */
public abstract class AbsSchemaMode {

    protected final KafkaSchemaMode kafkaSchemaMode;
    protected final IKafkaService kafkaService;
    protected final Log tapLogger;

    protected AbsSchemaMode(KafkaSchemaMode kafkaSchemaMode, IKafkaService kafkaService) {
        this.kafkaSchemaMode = kafkaSchemaMode;
        this.kafkaService = kafkaService;
        this.tapLogger = kafkaService.getLog();
    }

    public KafkaSchemaMode getSchemaMode() {
        return kafkaSchemaMode;
    }

    public IKafkaService getKafkaService() {
        return kafkaService;
    }

    public void discoverSchema(IKafkaService kafkaService, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        Queue<String> toBeLoadTables = tables.stream().filter(Objects::nonNull).distinct().collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        List<TapTable> results = new LinkedList<>();
        try {
            String executorGroup = String.format("%s-discoverSchema", KafkaEnhancedConnector.PDK_ID);
            ConcurrentUtils.runWithQueue(kafkaService.getExecutorService(), executorGroup, toBeLoadTables, 20, table -> {
                TapTable sampleTable = new TapTable(table);
                try {
                    sampleOneSchema(table, sampleTable);
                } catch (Exception e) {
                    tapLogger.warn("topic: {} sample failed!");
                }
                results.add(sampleTable);
            });
            consumer.accept(results);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sampleOneSchema(String table, TapTable sampleTable) {

    }

    public abstract TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord);

    public abstract List<ProducerRecord<Object, Object>> fromTapEvent(TapTable table, TapEvent tapEvent);

    public abstract void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer);

    public static AbsSchemaMode create(KafkaSchemaMode schemaMode, IKafkaService kafkaService) {

        if (null == schemaMode) {
            throw new IllegalArgumentException("Connection schemaMode is required");
        }
        switch (schemaMode) {
            case ORIGINAL:
                return new OriginalSchemaMode(kafkaService);
            case STANDARD:
                return new StandardSchemaMode(kafkaService);
            case CANAL:
                return new CanalSchemaMode(kafkaService);
            case DEBEZIUM:
                return new DebeziumSchemaMode(kafkaService);
            case FLINK_CDC:
                return new FlinkSchemaMode(kafkaService);
            case REGISTRY_AVRO:
                return new RegistryAvroMode(kafkaService);
            case REGISTRY_PROTOBUF:
                return new RegistryProtobufMode(kafkaService);
            case REGISTRY_JSON:
                return new RegistryJsonMode(kafkaService);
            default:
                throw new NotSupportedException(String.format("schema mode '%s'", schemaMode));
        }
    }

    protected void processIfStringNotBlank(Object param, Consumer<String> consumer) {
        if (null != param && StringUtils.isNotBlank(param.toString())) {
            consumer.accept(param.toString());
        }
    }

    protected byte[] createKafkaKey(Map<String, Object> data, TapTable tapTable) {
        Collection<String> keys = tapTable.primaryKeys(true);
        if (EmptyKit.isEmpty(keys)) {
            return null;
        }
        return keys.stream().map(key -> String.valueOf(data.get(key))).collect(Collectors.joining("_")).getBytes();
    }

    protected String createKafkaKeyValueMap(Map<String, Object> data, TapTable tapTable) {
        Collection<String> keys = tapTable.primaryKeys(true);
        if (EmptyKit.isEmpty(keys)) {
            return null;
        }
        Map<String, Object> keyValue = new HashMap<>();
        keys.forEach(v -> keyValue.put(v, data.get(v)));
        return TapSimplify.toJson(keyValue);
    }

    protected String topic(TapTable table, TapEvent tapEvent) {
        return KafkaUtils.pickTopic(kafkaService.getConfig(), tapEvent.getDatabase(), tapEvent.getSchema(), table);
    }

    /**
     * 从 ConsumerRecord 的 header 中获取操作类型
     */
    protected DMLType getOperationType(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord.headers() != null) {
            org.apache.kafka.common.header.Header opHeader = consumerRecord.headers().lastHeader("op");
            if (opHeader != null && opHeader.value() != null) {
                String opValue = new String(opHeader.value());
                try {
                    return DMLType.valueOf(opValue.toUpperCase());
                } catch (IllegalArgumentException e) {
                    // 如果无法解析，返回默认值
                }
            }
        }
        // 默认为 INSERT
        return DMLType.INSERT;
    }

    protected String toTapType(String dataType) {
        switch (dataType) {
            case "INT":
                return "INTEGER";
            default:
                return dataType;
        }
    }
}
