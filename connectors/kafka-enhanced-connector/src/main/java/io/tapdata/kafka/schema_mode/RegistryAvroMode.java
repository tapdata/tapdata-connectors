package io.tapdata.kafka.schema_mode;

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
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.tapdata.constant.DMLType.*;

public class RegistryAvroMode extends AbsSchemaMode {

    private final Map<String, Schema.Field> fieldCache = new ConcurrentHashMap<>();

    public RegistryAvroMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.REGISTRY_AVRO, kafkaService);
    }

    @Override
    public void sampleOneSchema(String table, TapTable sampleTable) {
        kafkaService.<String, Object>sampleValue(Collections.singletonList(table), null, record -> {
            if (null != record) {
                if (record.value() instanceof GenericRecord) {
                    GenericRecord genericRecord = (GenericRecord) record.value();
                    List<String> primaryKeys = new ArrayList<>();
                    if (record.key() != null) {
                        primaryKeys.addAll(((Map<String, Object>) TapSimplify.fromJson(record.key())).keySet());
                    }
                    genericRecordToTapTable(sampleTable, genericRecord, primaryKeys);
                    return false;
                }
            }
            return true;
        });
    }

    private void genericRecordToTapTable(TapTable sampleTable, GenericRecord genericRecord, List<String> primaryKeys) {
        genericRecord.getSchema().getFields().forEach(key -> {
            TapField field = new TapField();
            field.setName(key.name());
            if (key.schema().isUnion()) {
                if (key.schema().getTypes().size() == 2) {
                    for (Schema type : key.schema().getTypes()) {
                        if (type.getType() != Schema.Type.NULL) {
                            field.setDataType(toTapType(type.getType().name()));
                        }
                    }
                } else {
                    field.setDataType("STRING");
                }
            } else {
                field.setDataType(toTapType(key.schema().getType().name()));
            }
            field.setDefaultValue(key.defaultVal());
            field.setNullable(key.schema().isNullable());
            if (primaryKeys.contains(key.name())) {
                field.setPrimaryKey(true);
                field.setPrimaryKeyPos(primaryKeys.indexOf(key.name()) + 1);
            }
            sampleTable.add(field);
        });
    }

    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord == null || consumerRecord.value() == null) {
            return null;
        }

        try {
            // 从 Kafka 读取 Avro 数据
            Object value = consumerRecord.value();
            if (!(value instanceof GenericRecord)) {
                return null;
            }

            GenericRecord genericRecord = (GenericRecord) value;

            // 将 GenericRecord 转换成 Map
            Map<String, Object> data = convertGericRecordToMap(genericRecord);

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
            throw new RuntimeException("Failed to convert Protobuf message to TapEvent from topic: " + consumerRecord.topic(), e);
        }
    }

    private Map<String, Object> convertGericRecordToMap(GenericRecord record) {
        Map<String, Object> result = new HashMap<>();
        if (record == null) {
            return result;
        }
        record.getSchema().getFields().forEach(key -> {
            result.put(key.name(), record.get(key.name()));
        });
        return result;
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
            fieldAssembler.name(columnName).type(field.schema()).withDefault(tapField.getDefaultValue());
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
                Object convertedValue = convertToAvroType(value, StringKit.removeParentheses(tapField.getDataType()));
                record.put(fieldName, convertedValue);
            } else {
                record.put(fieldName, value);
            }
        }

        String keyValue = createKafkaKeyValueMap(data, tapTable);
        // 创建 ProducerRecord
        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic(tapTable, tapEvent), null,
                tapEvent.getTime(), keyValue, record, new RecordHeaders().add("op", op.name().getBytes()));
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
                case "SHORT":
                case "INTEGER":
                    if (value instanceof Integer) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).intValue();
                    }
                    return Long.parseLong(value.toString());
                case "LONG":
                    if (value instanceof Long) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                    return Long.parseLong(value.toString());
                case "FLOAT":
                    if (value instanceof Float) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).floatValue();
                    }
                    return Float.parseFloat(value.toString());
                case "DOUBLE":
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
            case "NUMBER":
                baseType = SchemaBuilder.builder().doubleType();
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

        Schema.Field field = new Schema.Field(columnName, avroType, null, tapField.getDefaultValue());
        fieldCache.put(tapTable.getId() + "." + columnName, field);
        return field;
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {

    }
}
