package io.tapdata.kafka.schema_mode;

import io.tapdata.constant.DMLType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kit.StringKit;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.constant.DMLType.*;

/**
 * {@link RegistryAvroMode} 的扩展范例：在父类生成的事件之上，把 Kafka 消息的元数据
 * （headers / partition / offset / timestamp）附加到 {@link TapEvent#getInfo()}，
 * 供下游做溯源或自定义路由。父类已实现的 schema diff、DDL 检测、主键 DDL 过滤等行为全部保留。
 * <p>
 * 作用域仅限本连接器实例，不影响同 JVM 内的其他 Kafka 连接器。
 */
public class AvroEnhanceMode extends RegistryAvroMode {

    public static final String INFO_KEY_KAFKA_META = "_kafkaMeta";

    public AvroEnhanceMode(IKafkaService kafkaService) {
        super(kafkaService);
    }

    /**
     * 复用父类的 schema diff + DDL 生成 + 主键 DDL 过滤；
     * 在事件返回前为每个事件挂上 Kafka 元数据。
     */
    @Override
    public List<TapEvent> toTapEvents(ConsumerRecord<?, ?> consumerRecord) {
        List<TapEvent> events = super.toTapEvents(consumerRecord);
        if (events == null || events.isEmpty()) {
            return events;
        }
        Map<String, Object> meta = buildKafkaMeta(consumerRecord);
        for (TapEvent e : events) {
            if (e != null) {
                e.addInfo(INFO_KEY_KAFKA_META, meta);
            }
        }
        return events;
    }

    /**
     * 单条 DML 路径（被父类 {@code toTapEvents} 内部和外部都可能直接调用，二者保持一致行为）。
     */
    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        TapEvent event = super.toTapEvent(consumerRecord);
        if (event != null) {
            event.addInfo(INFO_KEY_KAFKA_META, buildKafkaMeta(consumerRecord));
        }
        return event;
    }

    private Map<String, Object> buildKafkaMeta(ConsumerRecord<?, ?> record) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("topic", record.topic());
        meta.put("partition", record.partition());
        meta.put("offset", record.offset());
        meta.put("timestamp", record.timestamp());
        if (record.headers() != null) {
            Map<String, String> headers = new LinkedHashMap<>();
            for (Header h : record.headers()) {
                if (h == null || h.key() == null) continue;
                headers.put(h.key(), h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8));
            }
            if (!headers.isEmpty()) {
                meta.put("headers", headers);
            }
        }
        return meta;
    }

    @Override
    public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable tapTable, TapEvent tapEvent) {
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

        final Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        Schema avroSchema = buildAvroSchemaFromTable(tapTable);
        GenericRecord record = new GenericData.Record(avroSchema);

        // 填充数据，需要进行类型转换以匹配 Avro schema
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();

            // 获取字段的 schema 信息
            Schema.Field schemaField = avroSchema.getField(fieldName);
            if (schemaField == null) {
                continue; // 跳过 schema 中不存在的字段
            }

            // 转换值以匹配 Avro schema 类型
            TapField tapField = nameFieldMap.get(fieldName);
            if (tapField != null) {
                Object convertedValue = convertToAvroType(value, StringKit.removeParentheses(tapField.getDataType()));
                record.put(fieldName, convertedValue);
            } else {
                record.put(fieldName, value);
            }
        }

        String keyValue = createKafkaKeyValueMap(data, tapTable);
        // 创建 ProducerRecord
        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic(tapTable, tapEvent), computePartition(createKafkaKey(data, tapTable), kafkaService.getConfig().getNodePartitionSize()),
                tapEvent.getTime(), keyValue, record, new RecordHeaders().add("op", op.name().getBytes()));
        return List.of(producerRecord);
    }

    protected Schema.Field getOrCreateAvroField(TapTable tapTable, TapField tapField) {
        final String columnName = tapField.getName();
        if (fieldCache.containsKey(tapTable.getId() + "." + columnName)) {
            return fieldCache.get(tapTable.getId() + "." + columnName);
        }

        final String columnType = StringKit.removeParentheses(tapField.getDataType());
        Schema avroType;

        // 判断字段是否可为 null
        boolean nullable = !Boolean.FALSE.equals(tapField.getNullable());

        // 根据类型创建基础 Schema
        Schema baseType;
        switch (columnType) {
            case "BOOLEAN":
                baseType = SchemaBuilder.builder().booleanType();
                break;
            case "INTEGER":
            case "SHORT":
                baseType = SchemaBuilder.builder().intType();
                break;
            case "LONG":
                baseType = SchemaBuilder.builder().longType();
                break;
            case "FLOAT":
                baseType = SchemaBuilder.builder().floatType();
                break;
            case "DOUBLE":
                baseType = SchemaBuilder.builder().doubleType();
                break;
            case "NUMBER":
                baseType = new LogicalType.DecimalConversion()
                break;
            case "BINARY":
            case "STRING":
            case "TEXT":
            case "CHAR":
            case "VARCHAR":
            case "DATE":
            case "TIME":
            case "DATETIME":
            case "TIMESTAMP":
            case "ARRAY":
            case "MAP":
            case "OBJECT":
            default:
                baseType = SchemaBuilder.builder().stringType();
                break;
        }

        // 如果字段可为 null，创建 union [null, type] schema
        if (nullable) {
            avroType = SchemaBuilder.builder().unionOf()
                    .nullType().and()
                    .type(baseType)
                    .endUnion();
        } else {
            avroType = baseType;
        }

        Schema.Field field;
        if (applyDefault) {
            field = new Schema.Field(columnName, avroType, null, tapField.getDefaultValue());
        } else {
            field = new Schema.Field(columnName, avroType, null, null);
        }
        fieldCache.put(tapTable.getId() + "." + columnName, field);
        return field;
    }
}
