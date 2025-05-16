package io.tapdata.kafka.schema_mode;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.mapping.TapEntry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.kafka.KafkaConfig;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.kafka.constants.KafkaSerialization;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.utils.KafkaUtils;
import io.tapdata.kafka.utils.RecordHeadersUtils;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Kafka 原始结构模式服务接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/3 17:00 Create
 */
public class OriginalSchemaMode extends AbsSchemaMode {
    private static final String FIELD_PARTITION = "partition";
    private static final String FIELD_OFFSET = "offset";
    private static final String FIELD_TIMESTAMP = "timestamp";
    private static final String FIELD_TIMESTAMP_TYPE = "timestampType";
    private static final String FIELD_HEADERS = "headers";
    private static final String FIELD_HEADERS_KEY = "headers.key";
    private static final String FIELD_HEADERS_VALUE = "headers.value";
    private static final String FIELD_KEY = "key";
    private static final String FIELD_VALUE = "value";

    public OriginalSchemaMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.ORIGINAL, kafkaService);
    }

    @Override
    public void discoverSchema(IKafkaService kafkaService, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        List<TapTable> tableSchemas = new ArrayList<>();
        for (String table : tables) {
            tableSchemas.add(topic2Table(kafkaService.getConfig(), table));
        }
        consumer.accept(tableSchemas);
    }

    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        long referenceTime = consumerRecord.timestamp();
        String topic = consumerRecord.topic();
        Map<String, Object> data = toMap(consumerRecord);
        return new TapInsertRecordEvent().init().table(topic).after(data).referenceTime(referenceTime);
    }

    @Override
    public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable table, TapEvent tapEvent) {
        if (tapEvent instanceof TapInsertRecordEvent) {
            TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) tapEvent;
            String topic = topic(table, tapEvent);
            Map<String, Object> afterMap = insertRecordEvent.getAfter();
            Object key = afterMap.get(FIELD_KEY);
            Object value = afterMap.containsKey(FIELD_KEY) ? afterMap.get(FIELD_VALUE) : afterMap;
            Integer partition = (Integer) afterMap.get(FIELD_PARTITION);
            Long ts = Optional.ofNullable(afterMap.get(FIELD_TIMESTAMP)).map(o -> {
                if (o instanceof String) {
                    return KafkaUtils.parseDateTime((String) o);
                } else if (o instanceof DateTime) {
                    return (DateTime) o;
                }
                return null;
            }).map(DateTime::toLong).orElse(null);
            List<TapEntry<String, byte[]>> headerList = (List<TapEntry<String, byte[]>>) afterMap.get(FIELD_HEADERS);
            Headers headers = RecordHeadersUtils.fromList(headerList);
            return Arrays.asList(new ProducerRecord<>(topic, partition, ts, key, value, headers));
        } else {
            throw new NotSupportedException("TapEvent type '" + tapEvent.getClass().getName() + "'");
        }
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {
        List<String> tables = Collections.singletonList(table.getId());
        Integer limit = Optional.ofNullable(filter.getLimit()).orElse(1);
        AtomicInteger total = new AtomicInteger(0);
        FilterResults filterResults = new FilterResults();
        kafkaService.sampleValue(tables, null, consumerRecord -> {
            filterResults.add(toMap(consumerRecord));
            return total.incrementAndGet() < limit;
        });
        consumer.accept(filterResults);
    }

// ---------- 工具方法 ----------

    Map<String, Object> toMap(ConsumerRecord<?, ?> consumerRecord) {
        long referenceTime = consumerRecord.timestamp();
        List<TapEntry<String, byte[]>> headerList = RecordHeadersUtils.toList(consumerRecord.headers());

        Map<String, Object> data = new LinkedHashMap<>();
        data.put(FIELD_PARTITION, consumerRecord.partition());
        data.put(FIELD_OFFSET, consumerRecord.offset());
        data.put(FIELD_TIMESTAMP, referenceTime);
        data.put(FIELD_TIMESTAMP_TYPE, consumerRecord.timestampType());
        data.put(FIELD_HEADERS, headerList);
        data.put(FIELD_KEY, consumerRecord.key());
        data.put(FIELD_VALUE, consumerRecord.value());
        return data;
    }

    TapTable topic2Table(KafkaConfig kafkaConfig, String topic) {
        KafkaSerialization keySerialization = kafkaConfig.getConnectionKeySerialization();
        if (null == keySerialization) {
            throw new IllegalArgumentException("KeySerialization for " + topic + " is not supported");
        }
        KafkaSerialization valueSerialization = kafkaConfig.getConnectionValueSerialization();
        if (null == valueSerialization) {
            throw new IllegalArgumentException("ValueSerialization for " + topic + " is not supported");
        }
        return new TapTable(topic)
            .add(new TapField(FIELD_PARTITION, "INTEGER").primaryKeyPos(1))
            .add(new TapField(FIELD_OFFSET, "LONG").primaryKeyPos(2).nullable(false))
            .add(new TapField(FIELD_TIMESTAMP, "TIMESTAMP(3)"))
            .add(new TapField(FIELD_TIMESTAMP_TYPE, "STRING(64)"))
            .add(new TapField(FIELD_HEADERS, "ARRAY"))
            .add(new TapField(FIELD_HEADERS_KEY, "VARCHAR(256)"))
            .add(new TapField(FIELD_HEADERS_VALUE, "BINARY(256)"))
            .add(new TapField(FIELD_KEY, keySerialization.getDataType()))
            .add(new TapField(FIELD_VALUE, valueSerialization.getDataType()));
    }
}
