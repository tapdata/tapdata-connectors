package io.tapdata.kafka.schema_mode;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class RegistryAvroMode extends AbsSchemaMode {

    private final Map<String, Schema.Field> fieldCache = new ConcurrentHashMap<>();

    public RegistryAvroMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.REGISTRY_AVRO, kafkaService);
    }

    @Override
    public void discoverSchema(IKafkaService kafkaService, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {

    }

    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        return null;
    }

    @Override
    public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable tapTable, TapEvent tapEvent) {
        Map<String, Object> data;
        if (tapEvent instanceof TapInsertRecordEvent) {
            data = ((TapInsertRecordEvent) tapEvent).getAfter();
        } else if (tapEvent instanceof TapUpdateRecordEvent) {
            data = ((TapUpdateRecordEvent) tapEvent).getAfter();
        } else if (tapEvent instanceof TapDeleteRecordEvent) {
            data = ((TapDeleteRecordEvent) tapEvent).getBefore();
        } else {
            data = new HashMap<>();
        }

        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(tapTable.getId());
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

        final Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        for (final String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            final String columnType = tapField.getDataType();
            if (StringUtils.isBlank(columnType)) {
                continue;
            }
            // 根据列的类型映射为 Avro 模式的类型
            Schema.Field field = getOrCreateAvroField(tapTable, tapField);
            fieldAssembler.name(columnName).type(field.schema()).noDefault();
        }
        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(fieldAssembler.endRecord().toString());
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
                Object convertedValue = convertToAvroType(value, tapField.getDataType());
                record.put(fieldName, convertedValue);
            } else {
                record.put(fieldName, value);
            }
        }

        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(
                tapTable.getId(),
                record
        );
        return List.of(producerRecord);
    }

    /**
     * 将值转换为 Avro 兼容的类型
     * Avro 要求类型严格匹配，例如 Integer 不能自动转换为 Long
     */
    private Object convertToAvroType(Object value, String dataType) {
        if (value == null) {
            return null;
        }

        try {
            switch (dataType) {
                case "BOOLEAN":
                    if (value instanceof Boolean) {
                        return value;
                    }
                    return Boolean.parseBoolean(value.toString());

                case "INTEGER":
                    // Avro INTEGER 映射为 long 类型
                    if (value instanceof Long) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                    return Long.parseLong(value.toString());

                case "NUMBER":
                    // Avro NUMBER 映射为 double 类型
                    if (value instanceof Double) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                    return Double.parseDouble(value.toString());

                case "STRING":
                case "TEXT":
                case "CHAR":
                case "VARCHAR":
                    return value.toString();

                case "ARRAY":
                case "MAP":
                default:
                    // 对于复杂类型或未知类型，转换为字符串
                    if (value instanceof String) {
                        return value;
                    }
                    return value.toString();
            }
        } catch (Exception e) {
            // 转换失败时返回字符串形式
            return value.toString();
        }
    }

    private Schema.Field getOrCreateAvroField(TapTable tapTable, TapField tapField) {
        final String columnName = tapField.getName();
        if (fieldCache.containsKey(tapTable.getId() + "." + columnName)) {
            return fieldCache.get(tapTable.getId() + "." + columnName);
        }

        final String columnType = tapField.getDataType();
        Schema avroType;

        // 判断字段是否可为 null
        boolean nullable = !Boolean.FALSE.equals(tapField.getNullable());

        // 根据类型创建基础 Schema
        Schema baseType;
        switch (columnType) {
            case "BOOLEAN":
                baseType = SchemaBuilder.builder().booleanType();
                break;
            case "NUMBER":
                baseType = SchemaBuilder.builder().doubleType();
                break;
            case "INTEGER":
                baseType = SchemaBuilder.builder().longType();
                break;
            case "STRING":
            case "ARRAY":
            case "TEXT":
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

        Schema.Field field = new Schema.Field(columnName, avroType, null, tapField.getDefaultValue());
        fieldCache.put(tapTable.getId() + "." + columnName, field);
        return field;
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {

    }
}
