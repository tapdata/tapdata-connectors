package io.tapdata.kafka;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.kafka.schema_mode.OriginalSchemaMode;
import io.tapdata.kafka.schema_mode.StandardSchemaMode;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.function.Consumer;

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

    public abstract ProducerRecord<Object, Object> fromTapEvent(TapTable table, TapEvent tapEvent);

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
            default:
                throw new NotSupportedException(String.format("schema mode '%s'", schemaMode));
        }
    }
}
