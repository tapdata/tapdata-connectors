package io.tapdata.kafka.schema_mode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.constant.DMLType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Consumer;

import static io.tapdata.constant.DMLType.*;

/**
 * Registry JSON Schema 模式实现
 * 使用 Confluent Schema Registry 和 JSON Schema 序列化
 *
 * @author Jarad
 */
public class RegistryJsonMode extends AbsSchemaMode {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public RegistryJsonMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.REGISTRY_JSON, kafkaService);
    }

    @Override
    public void sampleOneSchema(String table, TapTable sampleTable) {
        kafkaService.<String, Object>sampleValue(Collections.singletonList(table), null, record -> {
            if (null != record && record.value() != null) {
                try {
                    // JSON Schema 反序列化后通常是 JsonNode 或 Map
                    Object value = record.value();
                    Map<String, Object> dataMap = convertToMap(value);

                    // 从数据推断 schema
                    List<String> primaryKeys = new ArrayList<>();
                    if (record.key() != null) {
                        Map<String, Object> keyMap = (Map<String, Object>) TapSimplify.fromJson(record.key());
                        primaryKeys.addAll(keyMap.keySet());
                    }

                    // 构建 TapTable schema
                    for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                        String fieldName = entry.getKey();
                        Object fieldValue = entry.getValue();

                        TapField tapField = new TapField(fieldName, inferTapType(fieldValue));
                        tapField.setPrimaryKey(primaryKeys.contains(fieldName));
                        tapField.setPrimaryKeyPos(primaryKeys.indexOf(fieldName) + 1);
                        sampleTable.add(tapField);
                    }
                    return false; // 采样一条即可
                } catch (Exception e) {
                    // 继续采样下一条
                }
            }
            return true;
        });
    }

    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord == null || consumerRecord.value() == null) {
            return null;
        }

        try {
            // 从 Kafka 读取 JSON 数据
            Object value = consumerRecord.value();
            Map<String, Object> data = convertToMap(value);

            // 从 header 中获取操作类型
            DMLType op = getOperationType(consumerRecord);

            // 根据操作类型创建对应的 TapEvent
            TapRecordEvent tapEvent;
            switch (op) {
                case INSERT:
                    tapEvent = TapInsertRecordEvent.create();
                    ((TapInsertRecordEvent) tapEvent).setAfter(data);
                    break;

                case UPDATE:
                    tapEvent = TapUpdateRecordEvent.create();
                    ((TapUpdateRecordEvent) tapEvent).setAfter(data);
                    break;

                case DELETE:
                    tapEvent = TapDeleteRecordEvent.create();
                    ((TapDeleteRecordEvent) tapEvent).setBefore(data);
                    break;

                default:
                    tapEvent = TapInsertRecordEvent.create();
                    ((TapInsertRecordEvent) tapEvent).setAfter(data);
                    break;
            }

            // 设置事件元数据
            tapEvent.setTableId(consumerRecord.topic());
            tapEvent.setReferenceTime(consumerRecord.timestamp());

            return tapEvent;

        } catch (Exception e) {
            throw new RuntimeException("Failed to convert JSON message to TapEvent from topic: " + consumerRecord.topic(), e);
        }
    }

    @Override
    public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable tapTable, TapEvent tapEvent) {
        // 提取数据
        Map<String, Object> data;
        DMLType op = INSERT;
        if (tapEvent instanceof TapInsertRecordEvent) {
            data = ((TapInsertRecordEvent) tapEvent).getAfter();
        } else if (tapEvent instanceof TapUpdateRecordEvent) {
            data = ((TapUpdateRecordEvent) tapEvent).getAfter();
            op = UPDATE;
        } else if (tapEvent instanceof TapDeleteRecordEvent) {
            data = ((TapDeleteRecordEvent) tapEvent).getBefore();
            op = DELETE;
        } else {
            data = new HashMap<>();
        }

        if (data == null || data.isEmpty()) {
            data = new HashMap<>();
        }

        try {
            // 创建 Kafka 主键
            String keyValue = createKafkaKeyValueMap(data, tapTable);

            // 将 Map 转换为 JsonNode，KafkaJsonSchemaSerializer 支持 JsonNode
            // 它会自动从 JsonNode 生成 JSON Schema 并注册到 Schema Registry
            JsonNode valueNode = OBJECT_MAPPER.valueToTree(data);

            // 创建 ProducerRecord
            ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(
                    topic(tapTable, tapEvent),
                    null,
                    tapEvent.getTime(),
                    keyValue,
                    valueNode,  // 使用 JsonNode 而不是硬编码的对象
                    new RecordHeaders().add("op", op.name().getBytes())
            );

            return Collections.singletonList(producerRecord);

        } catch (Exception e) {
            throw new RuntimeException("Failed to convert TapEvent to JSON message for table: " + tapTable.getId(), e);
        }
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {
        // 高级过滤查询暂不实现
    }

    /**
     * 将对象转换为 Map
     */
    private Map<String, Object> convertToMap(Object value) {
        if (value == null) {
            return new HashMap<>();
        }

        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }

        if (value instanceof JsonNode) {
            return OBJECT_MAPPER.convertValue(value, Map.class);
        }

        // 尝试通过 Jackson 转换
        try {
            return OBJECT_MAPPER.convertValue(value, Map.class);
        } catch (Exception e) {
            // 转换失败，返回空 Map
            return new HashMap<>();
        }
    }

    /**
     * 根据值推断 TapData 类型
     */
    private String inferTapType(Object value) {
        if (value == null) {
            return "STRING";
        }

        if (value instanceof Boolean) {
            return "BOOLEAN";
        } else if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return "INTEGER";
        } else if (value instanceof Long) {
            return "BIGINT";
        } else if (value instanceof Float) {
            return "FLOAT";
        } else if (value instanceof Double) {
            return "DOUBLE";
        } else if (value instanceof BigDecimal) {
            return "DOUBLE";
        } else if (value instanceof String) {
            return "STRING";
        } else if (value instanceof List) {
            return "ARRAY";
        } else if (value instanceof Map) {
            return "MAP";
        } else {
            return "STRING";
        }
    }

}
