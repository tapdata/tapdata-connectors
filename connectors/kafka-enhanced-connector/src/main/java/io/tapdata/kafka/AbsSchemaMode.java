package io.tapdata.kafka;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    protected AbsSchemaMode(KafkaSchemaMode kafkaSchemaMode, IKafkaService kafkaService) {
        this.kafkaSchemaMode = kafkaSchemaMode;
        this.kafkaService = kafkaService;
    }

    public KafkaSchemaMode getSchemaMode() {
        return kafkaSchemaMode;
    }

    public IKafkaService getKafkaService() {
        return kafkaService;
    }

    public abstract void discoverSchema(IKafkaService kafkaService, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer);

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

    protected String topic(TapTable table, TapEvent tapEvent) {
        return KafkaUtils.pickTopic(kafkaService.getConfig(), tapEvent.getDatabase(), tapEvent.getSchema(), table);
    }
}
