package io.tapdata.kafka.schema_mode;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.tapdata.constant.DMLType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
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
import io.tapdata.kafka.utils.KafkaUtils;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.tapdata.constant.DMLType.*;

public class RegistryAvroMode extends AbsSchemaMode {

    private final Map<String, Schema.Field> fieldCache = new ConcurrentHashMap<>();
    private volatile SchemaRegistryClient schemaRegistryClient;

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
                        try {
                            primaryKeys.addAll(((Map<String, Object>) TapSimplify.fromJson(record.key())).keySet());
                        } catch (Exception e) {
                            tapLogger.warn("Failed to parse primary keys: {}", record.key(), e);
                        }
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
            field.setDefaultValue(getDefaultValue(key.defaultVal()));
            field.setNullable(key.schema().isNullable());
            if (primaryKeys.contains(key.name())) {
                field.setPrimaryKey(true);
                field.setPrimaryKeyPos(primaryKeys.indexOf(key.name()) + 1);
            }
            sampleTable.add(field);
        });
    }

    private Object getDefaultValue(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof JsonProperties.Null) {
            return null;
        }
        if (!(obj instanceof Serializable)) {
            return String.valueOf(obj);
        }
        return obj;
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
            String k = key.name();
            Object v = record.get(k);
            if (v instanceof Utf8) {
                result.put(k, v.toString());
            } else {
                result.put(k, v);
            }
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

        Schema.Field field;
        if (applyDefault) {
            field = new Schema.Field(columnName, avroType, null, tapField.getDefaultValue());
        } else {
            field = new Schema.Field(columnName, avroType, null, null);
        }
        fieldCache.put(tapTable.getId() + "." + columnName, field);
        return field;
    }

    @Override
    public ProducerRecord<Object, Object> fromTapDDLEvent(TapDDLEvent ddlEvent) {
        if (!(ddlEvent instanceof TapFieldBaseEvent)) {
            return null;
        }
        String tableId = ddlEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            tapLogger.warn("Skip DDL event without tableId: {}", ddlEvent.getClass().getSimpleName());
            return null;
        }

        TapFieldBaseEvent fieldEvent = (TapFieldBaseEvent) ddlEvent;
        invalidateFieldCacheForEvent(tableId, fieldEvent);
        TapTable tapTable = kafkaService.getConfig().tableMapGet(tableId);
        if (tapTable == null) {
            tapLogger.warn("Skip DDL '{}' for table '{}': not found in tableMap", ddlEvent.getClass().getSimpleName(), tableId);
            return null;
        }

        // tableMap 中的 TapTable 在引擎应用 DDL 之前可能仍是旧版，需在本地副本上应用事件得到新版 schema
        TapTable evolvedTable = applyFieldEvent(tapTable, fieldEvent);

        String topic = KafkaUtils.pickTopic(kafkaService.getConfig(), ddlEvent.getDatabase(), ddlEvent.getSchema(), tableId);
        String subject = topic + "-value";
        try {
            Schema avroSchema = buildAvroSchemaFromTable(evolvedTable);
            int id = getSchemaRegistryClient().register(subject, new AvroSchema(avroSchema));
            tapLogger.info("Registered avro schema for subject '{}' (id={}) by ddl '{}'", subject, id, ddlEvent.getClass().getSimpleName());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Register avro schema failed for subject '%s' (event %s): %s",
                    subject, ddlEvent.getClass().getSimpleName(), e.getMessage()), e);
        }
        return null;
    }

    private TapTable applyFieldEvent(TapTable original, TapFieldBaseEvent ddlEvent) {
        LinkedHashMap<String, TapField> source = original.getNameFieldMap();
        LinkedHashMap<String, TapField> evolved = source == null ? new LinkedHashMap<>() : new LinkedHashMap<>(source);
        if (ddlEvent instanceof TapNewFieldEvent) {
            List<TapField> newFields = ((TapNewFieldEvent) ddlEvent).getNewFields();
            if (newFields != null) {
                for (TapField nf : newFields) {
                    if (nf != null && StringUtils.isNotBlank(nf.getName())) {
                        evolved.put(nf.getName(), nf);
                    }
                }
            }
        } else if (ddlEvent instanceof TapDropFieldEvent) {
            String fieldName = ((TapDropFieldEvent) ddlEvent).getFieldName();
            if (StringUtils.isNotBlank(fieldName)) {
                evolved.remove(fieldName);
            }
        } else if (ddlEvent instanceof TapAlterFieldNameEvent) {
            ValueChange<String> nc = ((TapAlterFieldNameEvent) ddlEvent).getNameChange();
            if (nc != null && StringUtils.isNotBlank(nc.getBefore()) && StringUtils.isNotBlank(nc.getAfter())) {
                TapField field = evolved.remove(nc.getBefore());
                if (field != null) {
                    TapField renamed = field.clone();
                    renamed.setName(nc.getAfter());
                    evolved.put(nc.getAfter(), renamed);
                }
            }
        } else if (ddlEvent instanceof TapAlterFieldAttributesEvent) {
            TapAlterFieldAttributesEvent attr = (TapAlterFieldAttributesEvent) ddlEvent;
            String fieldName = attr.getFieldName();
            TapField existing = evolved.get(fieldName);
            if (existing != null) {
                TapField mutated = existing.clone();
                if (attr.getDataTypeChange() != null && StringUtils.isNotBlank(attr.getDataTypeChange().getAfter())) {
                    mutated.setDataType(attr.getDataTypeChange().getAfter());
                }
                if (attr.getDefaultChange() != null) {
                    mutated.setDefaultValue(attr.getDefaultChange().getAfter());
                }
                if (attr.getNullableChange() != null && attr.getNullableChange().getAfter() != null) {
                    mutated.setNullable(attr.getNullableChange().getAfter());
                }
                evolved.put(fieldName, mutated);
            }
        }
        TapTable synthetic = new TapTable(original.getId());
        synthetic.setNameFieldMap(evolved);
        return synthetic;
    }

    private Schema buildAvroSchemaFromTable(TapTable tapTable) {
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(tapTable.getId());
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (nameFieldMap != null) {
            for (final String columnName : nameFieldMap.keySet()) {
                TapField tapField = nameFieldMap.get(columnName);
                if (tapField == null || StringUtils.isBlank(tapField.getDataType())) {
                    continue;
                }
                Schema.Field field = getOrCreateAvroField(tapTable, tapField);
                if (EmptyKit.isNotNull(tapField.getDefaultValue()) && applyDefault) {
                    fieldAssembler.name(columnName).type(field.schema()).withDefault(tapField.getDefaultValue());
                } else if (isNullableUnion(field.schema())) {
                    // 可空字段统一兜底为 null default，保证 Schema Registry BACKWARD 兼容（新增列对旧消费者可读）
                    fieldAssembler.name(columnName).type(field.schema()).withDefault(null);
                } else {
                    fieldAssembler.name(columnName).type(field.schema()).noDefault();
                }
            }
        }
        return new Schema.Parser().parse(fieldAssembler.endRecord().toString());
    }

    private boolean isNullableUnion(Schema schema) {
        if (schema == null || schema.getType() != Schema.Type.UNION) {
            return false;
        }
        List<Schema> types = schema.getTypes();
        return !types.isEmpty() && types.get(0).getType() == Schema.Type.NULL;
    }

    private void invalidateFieldCacheForEvent(String tableId, TapFieldBaseEvent ddlEvent) {
        primaryKeyMap.remove(tableId);
        if (ddlEvent instanceof TapAlterFieldNameEvent) {
            ValueChange<String> nameChange = ((TapAlterFieldNameEvent) ddlEvent).getNameChange();
            if (nameChange != null) {
                if (StringUtils.isNotBlank(nameChange.getBefore())) {
                    fieldCache.remove(tableId + "." + nameChange.getBefore());
                }
                if (StringUtils.isNotBlank(nameChange.getAfter())) {
                    fieldCache.remove(tableId + "." + nameChange.getAfter());
                }
            }
        } else if (ddlEvent instanceof TapAlterFieldAttributesEvent) {
            String fieldName = ((TapAlterFieldAttributesEvent) ddlEvent).getFieldName();
            if (StringUtils.isNotBlank(fieldName)) {
                fieldCache.remove(tableId + "." + fieldName);
            }
        } else if (ddlEvent instanceof TapDropFieldEvent) {
            String fieldName = ((TapDropFieldEvent) ddlEvent).getFieldName();
            if (StringUtils.isNotBlank(fieldName)) {
                fieldCache.remove(tableId + "." + fieldName);
            }
        }
        // TapNewFieldEvent: 新字段无既有缓存，按需重建即可
    }

    private SchemaRegistryClient getSchemaRegistryClient() {
        SchemaRegistryClient client = schemaRegistryClient;
        if (client != null) {
            return client;
        }
        synchronized (this) {
            if (schemaRegistryClient != null) {
                return schemaRegistryClient;
            }
            String raw = kafkaService.getConfig().getConnectionSchemaRegisterUrl();
            if (StringUtils.isBlank(raw)) {
                throw new IllegalStateException("schema registry url is not configured");
            }
            List<String> urls = new ArrayList<>();
            for (String u : raw.split(",")) {
                String t = u.trim();
                if (t.isEmpty()) continue;
                urls.add(t.startsWith("http://") || t.startsWith("https://") ? t : "http://" + t);
            }
            Map<String, Object> configs = new HashMap<>();
            if (kafkaService.getConfig().getConnectionBasicAuth()) {
                String source = kafkaService.getConfig().getConnectionAuthCredentialsSource();
                configs.put("basic.auth.credentials.source", StringUtils.isBlank(source) ? "USER_INFO" : source);
                configs.put("basic.auth.user.info",
                        kafkaService.getConfig().getConnectionAuthUserName() + ":" + kafkaService.getConfig().getConnectionAuthPassword());
            }
            schemaRegistryClient = new CachedSchemaRegistryClient(urls, 1000, configs);
            return schemaRegistryClient;
        }
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {

    }
}
