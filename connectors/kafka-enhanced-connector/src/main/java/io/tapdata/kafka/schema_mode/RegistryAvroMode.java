package io.tapdata.kafka.schema_mode;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.tapdata.constant.DMLType.*;

public class RegistryAvroMode extends AbsSchemaMode {

    private final Map<String, Schema.Field> fieldCache = new ConcurrentHashMap<>();
    // 每个 topic 最近一次见到的 Avro Schema 引用；命中时（==）走快速路径，跳过任何 schema diff 计算
    private final Map<String, Schema> lastSchemaPerTopic = new ConcurrentHashMap<>();
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
    public List<TapEvent> toTapEvents(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord == null || consumerRecord.value() == null) {
            return Collections.emptyList();
        }
        Object value = consumerRecord.value();
        if (!(value instanceof GenericRecord)) {
            return super.toTapEvents(consumerRecord);
        }
        Schema currentSchema = ((GenericRecord) value).getSchema();
        String topic = consumerRecord.topic();
        // 快速路径：与上次见到的 schema 引用相同时，KafkaAvroDeserializer 内部缓存已保证 == 命中，直接产生 DML 事件
        Schema lastSchema = lastSchemaPerTopic.get(topic);
        if (lastSchema == currentSchema) {
            TapEvent dml = toTapEvent(consumerRecord);
            return dml == null ? Collections.emptyList() : Collections.singletonList(dml);
        }
        // 慢速路径：首次见到或 schema 已变化
        List<TapEvent> events;
        if (lastSchema == null) {
            // 首次：与 tableMap 中已知的 TapTable 结构对比，覆盖任务重启后内存缓存丢失、
            // 但持久化的 TapTable 已落后于实际 schema 的场景；若 tableMap 没有记录则只能基线
            TapTable knownTable = kafkaService.getConfig().tableMapGet(topic);
            if (knownTable != null) {
                events = detectSchemaChangesFromTable(topic, knownTable, currentSchema, consumerRecord.timestamp());
            } else {
                events = new ArrayList<>(1);
            }
        } else {
            events = detectSchemaChanges(topic, lastSchema, currentSchema, consumerRecord.timestamp());
        }
        lastSchemaPerTopic.put(topic, currentSchema);
        TapEvent dml = toTapEvent(consumerRecord);
        if (dml != null) {
            events.add(dml);
        }
        return events;
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

    /**
     * 比较前后两个 Avro Schema，反向生成对应的 TapFieldBaseEvent。
     * 规则：
     * - 别名（{@link Schema.Field#aliases()}）从旧 schema 字段名映射到新名 → {@link TapAlterFieldNameEvent}
     * - 新增字段（排除 rename 的目标） → {@link TapNewFieldEvent}
     * - 删除字段（排除 rename 的源） → {@link TapDropFieldEvent}
     * - 同名字段属性差异（dataType / nullable / default） → {@link TapAlterFieldAttributesEvent}
     */
    private List<TapEvent> detectSchemaChanges(String topic, Schema oldSchema, Schema newSchema, long referenceTime) {
        List<TapEvent> events = new ArrayList<>();
        Map<String, Schema.Field> oldFields = new LinkedHashMap<>();
        for (Schema.Field f : oldSchema.getFields()) {
            oldFields.put(f.name(), f);
        }
        Map<String, Schema.Field> newFields = new LinkedHashMap<>();
        for (Schema.Field f : newSchema.getFields()) {
            newFields.put(f.name(), f);
        }
        // 1) rename：通过 alias 把旧名映射到新名
        Map<String, String> renameAfterToBefore = new HashMap<>();
        for (Schema.Field nf : newSchema.getFields()) {
            if (oldFields.containsKey(nf.name())) {
                continue;
            }
            Set<String> aliases = nf.aliases();
            if (aliases == null || aliases.isEmpty()) {
                continue;
            }
            for (String alias : aliases) {
                if (oldFields.containsKey(alias) && !newFields.containsKey(alias) && !renameAfterToBefore.containsValue(alias)) {
                    renameAfterToBefore.put(nf.name(), alias);
                    TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
                    rename.setTime(System.currentTimeMillis());
                    rename.setTableId(topic);
                    rename.setReferenceTime(referenceTime);
                    rename.nameChange(ValueChange.create(alias, nf.name()));
                    events.add(rename);
                    break;
                }
            }
        }
        // 1.5) 启发式 rename 兜底：alias 未命中时，若剩余 drop / add 各恰好 1 个且数据类型完全一致，视为 rename
        {
            List<Schema.Field> remainingDrops = new ArrayList<>();
            for (Schema.Field of : oldSchema.getFields()) {
                if (newFields.containsKey(of.name())) continue;
                if (renameAfterToBefore.containsValue(of.name())) continue;
                remainingDrops.add(of);
            }
            List<Schema.Field> remainingAdds = new ArrayList<>();
            for (Schema.Field nf : newSchema.getFields()) {
                if (oldFields.containsKey(nf.name())) continue;
                if (renameAfterToBefore.containsKey(nf.name())) continue;
                remainingAdds.add(nf);
            }
            if (remainingDrops.size() == 1 && remainingAdds.size() == 1) {
                Schema.Field d = remainingDrops.get(0);
                Schema.Field a = remainingAdds.get(0);
                String dType = avroPrimaryTypeName(d.schema());
                String aType = avroPrimaryTypeName(a.schema());
                if (Objects.equals(dType, aType)) {
                    renameAfterToBefore.put(a.name(), d.name());
                    TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
                    rename.setTime(System.currentTimeMillis());
                    rename.setTableId(topic);
                    rename.setReferenceTime(referenceTime);
                    rename.nameChange(ValueChange.create(d.name(), a.name()));
                    events.add(rename);
                    tapLogger.info("Heuristic rename detected on topic '{}' (alias missing): {} -> {} (type={})", topic, d.name(), a.name(), dType);
                }
            }
        }
        // 2) 新增字段
        List<TapField> addedFields = new ArrayList<>();
        for (Schema.Field nf : newSchema.getFields()) {
            if (oldFields.containsKey(nf.name())) {
                continue;
            }
            if (renameAfterToBefore.containsKey(nf.name())) {
                continue;
            }
            TapField tf = avroFieldToTapField(nf);
            if (tf != null) {
                addedFields.add(tf);
            }
        }
        if (!addedFields.isEmpty()) {
            TapNewFieldEvent add = new TapNewFieldEvent();
            add.setTime(System.currentTimeMillis());
            add.setTableId(topic);
            add.setReferenceTime(referenceTime);
            add.setNewFields(addedFields);
            events.add(add);
        }
        // 3) 删除字段
        Set<String> renameSources = new HashSet<>(renameAfterToBefore.values());
        for (Schema.Field of : oldSchema.getFields()) {
            if (newFields.containsKey(of.name())) {
                continue;
            }
            if (renameSources.contains(of.name())) {
                continue;
            }
            TapDropFieldEvent drop = new TapDropFieldEvent();
            drop.setTime(System.currentTimeMillis());
            drop.setTableId(topic);
            drop.setReferenceTime(referenceTime);
            drop.fieldName(of.name());
            events.add(drop);
        }
        // 4) 属性变化
        for (Schema.Field nf : newSchema.getFields()) {
            Schema.Field of = oldFields.get(nf.name());
            if (of == null) {
                continue;
            }
            TapAlterFieldAttributesEvent attr = diffFieldAttributes(topic, of, nf, referenceTime);
            if (attr != null) {
                events.add(attr);
            }
        }
        return events;
    }

    private TapAlterFieldAttributesEvent diffFieldAttributes(String topic, Schema.Field oldField, Schema.Field newField, long referenceTime) {
        String oldType = avroPrimaryTypeName(oldField.schema());
        String newType = avroPrimaryTypeName(newField.schema());
        boolean oldNullable = oldField.schema().isNullable();
        boolean newNullable = newField.schema().isNullable();
        Object oldDefault = getDefaultValue(oldField.defaultVal());
        Object newDefault = getDefaultValue(newField.defaultVal());
        boolean dataTypeChanged = !Objects.equals(oldType, newType);
        boolean nullableChanged = oldNullable != newNullable;
        boolean defaultChanged = !Objects.equals(oldDefault, newDefault);
        if (!dataTypeChanged && !nullableChanged && !defaultChanged) {
            return null;
        }
        TapAlterFieldAttributesEvent attr = new TapAlterFieldAttributesEvent();
        attr.setTime(System.currentTimeMillis());
        attr.setTableId(topic);
        attr.setReferenceTime(referenceTime);
        attr.fieldName(newField.name());
        if (dataTypeChanged) {
            attr.dataType(ValueChange.create(oldType, newType));
        }
        if (nullableChanged) {
            attr.nullable(ValueChange.create(oldNullable, newNullable));
        }
        if (defaultChanged) {
            attr.defaultChange(ValueChange.create(oldDefault, newDefault));
        }
        return attr;
    }

    /**
     * 用 {@link TapTable} 作为基线（任务重启后内存中没有上一份 Avro Schema 时使用），与新到的 Avro Schema 比对生成 DDL。
     * 规则与 {@link #detectSchemaChanges} 保持一致，仅基线来源不同。
     */
    private List<TapEvent> detectSchemaChangesFromTable(String topic, TapTable tapTable, Schema newSchema, long referenceTime) {
        List<TapEvent> events = new ArrayList<>();
        Map<String, TapField> oldFields = tapTable.getNameFieldMap();
        if (oldFields == null) {
            oldFields = Collections.emptyMap();
        }
        Map<String, Schema.Field> newFields = new LinkedHashMap<>();
        for (Schema.Field f : newSchema.getFields()) {
            newFields.put(f.name(), f);
        }
        // 1) rename：通过 alias 把 TapTable 中的旧字段名映射到新名
        Map<String, String> renameAfterToBefore = new HashMap<>();
        for (Schema.Field nf : newSchema.getFields()) {
            if (oldFields.containsKey(nf.name())) {
                continue;
            }
            Set<String> aliases = nf.aliases();
            if (aliases == null || aliases.isEmpty()) {
                continue;
            }
            for (String alias : aliases) {
                if (oldFields.containsKey(alias) && !newFields.containsKey(alias) && !renameAfterToBefore.containsValue(alias)) {
                    renameAfterToBefore.put(nf.name(), alias);
                    TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
                    rename.setTime(System.currentTimeMillis());
                    rename.setTableId(topic);
                    rename.setReferenceTime(referenceTime);
                    rename.nameChange(ValueChange.create(alias, nf.name()));
                    events.add(rename);
                    break;
                }
            }
        }
        // 1.5) 启发式 rename 兜底：alias 未命中时，若剩余 drop / add 各恰好 1 个且数据类型完全一致，视为 rename
        {
            List<String> remainingDropNames = new ArrayList<>();
            for (String oldName : oldFields.keySet()) {
                if (newFields.containsKey(oldName)) continue;
                if (renameAfterToBefore.containsValue(oldName)) continue;
                remainingDropNames.add(oldName);
            }
            List<Schema.Field> remainingAdds = new ArrayList<>();
            for (Schema.Field nf : newSchema.getFields()) {
                if (oldFields.containsKey(nf.name())) continue;
                if (renameAfterToBefore.containsKey(nf.name())) continue;
                remainingAdds.add(nf);
            }
            if (remainingDropNames.size() == 1 && remainingAdds.size() == 1) {
                String dName = remainingDropNames.get(0);
                Schema.Field a = remainingAdds.get(0);
                TapField dField = oldFields.get(dName);
                String dType = dField == null || dField.getDataType() == null ? null : StringKit.removeParentheses(dField.getDataType());
                String aType = avroPrimaryTypeName(a.schema());
                if (dType != null && Objects.equals(dType, aType)) {
                    renameAfterToBefore.put(a.name(), dName);
                    TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
                    rename.setTime(System.currentTimeMillis());
                    rename.setTableId(topic);
                    rename.setReferenceTime(referenceTime);
                    rename.nameChange(ValueChange.create(dName, a.name()));
                    events.add(rename);
                    tapLogger.info("Heuristic rename detected on topic '{}' (alias missing, baseline=TapTable): {} -> {} (type={})", topic, dName, a.name(), dType);
                }
            }
        }
        // 2) 新增字段
        List<TapField> addedFields = new ArrayList<>();
        for (Schema.Field nf : newSchema.getFields()) {
            if (oldFields.containsKey(nf.name())) {
                continue;
            }
            if (renameAfterToBefore.containsKey(nf.name())) {
                continue;
            }
            TapField tf = avroFieldToTapField(nf);
            if (tf != null) {
                addedFields.add(tf);
            }
        }
        if (!addedFields.isEmpty()) {
            TapNewFieldEvent add = new TapNewFieldEvent();
            add.setTime(System.currentTimeMillis());
            add.setTableId(topic);
            add.setReferenceTime(referenceTime);
            add.setNewFields(addedFields);
            events.add(add);
        }
        // 3) 删除字段
        Set<String> renameSources = new HashSet<>(renameAfterToBefore.values());
        for (String oldName : oldFields.keySet()) {
            if (newFields.containsKey(oldName)) {
                continue;
            }
            if (renameSources.contains(oldName)) {
                continue;
            }
            TapDropFieldEvent drop = new TapDropFieldEvent();
            drop.setTime(System.currentTimeMillis());
            drop.setTableId(topic);
            drop.setReferenceTime(referenceTime);
            drop.fieldName(oldName);
            events.add(drop);
        }
        // 4) 属性变化
        for (Schema.Field nf : newSchema.getFields()) {
            TapField of = oldFields.get(nf.name());
            if (of == null) {
                continue;
            }
            TapAlterFieldAttributesEvent attr = diffFieldAttributesFromTapField(topic, of, nf, referenceTime);
            if (attr != null) {
                events.add(attr);
            }
        }
        return events;
    }

    private TapAlterFieldAttributesEvent diffFieldAttributesFromTapField(String topic, TapField oldField, Schema.Field newField, long referenceTime) {
        String oldType = oldField.getDataType() == null ? null : StringKit.removeParentheses(oldField.getDataType());
        String newType = avroPrimaryTypeName(newField.schema());
        boolean oldNullable = !Boolean.FALSE.equals(oldField.getNullable());
        boolean newNullable = newField.schema().isNullable();
        Object oldDefault = oldField.getDefaultValue();
        Object newDefault = getDefaultValue(newField.defaultVal());
        boolean dataTypeChanged = !Objects.equals(oldType, newType);
        boolean nullableChanged = oldNullable != newNullable;
        boolean defaultChanged = !Objects.equals(oldDefault, newDefault);
        if (!dataTypeChanged && !nullableChanged && !defaultChanged) {
            return null;
        }
        TapAlterFieldAttributesEvent attr = new TapAlterFieldAttributesEvent();
        attr.setTime(System.currentTimeMillis());
        attr.setTableId(topic);
        attr.setReferenceTime(referenceTime);
        attr.fieldName(newField.name());
        if (dataTypeChanged) {
            attr.dataType(ValueChange.create(oldType, newType));
        }
        if (nullableChanged) {
            attr.nullable(ValueChange.create(oldNullable, newNullable));
        }
        if (defaultChanged) {
            attr.defaultChange(ValueChange.create(oldDefault, newDefault));
        }
        return attr;
    }

    private TapField avroFieldToTapField(Schema.Field avroField) {
        TapField field = new TapField();
        field.setName(avroField.name());
        field.setDataType(avroPrimaryTypeName(avroField.schema()));
        field.setNullable(avroField.schema().isNullable());
        field.setDefaultValue(getDefaultValue(avroField.defaultVal()));
        return field;
    }

    /**
     * 返回 Avro Schema 中除 NULL 之外的主要类型名（与 {@link #genericRecordToTapTable} 处理一致）。
     */
    private String avroPrimaryTypeName(Schema schema) {
        if (schema == null) {
            return "STRING";
        }
        if (schema.isUnion()) {
            List<Schema> types = schema.getTypes();
            if (types.size() == 2) {
                for (Schema t : types) {
                    if (t.getType() != Schema.Type.NULL) {
                        return toTapType(t.getType().name());
                    }
                }
            }
            return "STRING";
        }
        return toTapType(schema.getType().name());
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

        // 收集本次事件中的直接重命名映射（after -> before），用于在新 schema 中登记 alias
        Map<String, String> immediateRenames = new HashMap<>();
        if (fieldEvent instanceof TapAlterFieldNameEvent) {
            ValueChange<String> nc = ((TapAlterFieldNameEvent) fieldEvent).getNameChange();
            if (nc != null && StringUtils.isNotBlank(nc.getBefore()) && StringUtils.isNotBlank(nc.getAfter())) {
                immediateRenames.put(nc.getAfter(), nc.getBefore());
            }
        }

        String topic = KafkaUtils.pickTopic(kafkaService.getConfig(), ddlEvent.getDatabase(), ddlEvent.getSchema(), tableId);
        String subject = topic + "-value";
        try {
            // 合并 Registry 中已登记的历史 alias 链 + 本次事件的直接 rename，确保 A -> B -> C 不丢失
            Map<String, Set<String>> aliasMap = mergeAliasHistory(subject, immediateRenames, evolvedTable);
            Schema avroSchema = buildAvroSchemaFromTable(evolvedTable, aliasMap);
            int id = getSchemaRegistryClient().register(subject, new AvroSchema(avroSchema));
            tapLogger.info("Registered avro schema for subject '{}' (id={}) by ddl '{}'", subject, id, ddlEvent.getClass().getSimpleName());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Register avro schema failed for subject '%s' (event %s): %s",
                    subject, ddlEvent.getClass().getSimpleName(), e.getMessage()), e);
        }
        return null;
    }

    /**
     * 拉取 Registry 中该 subject 最新版本的 schema，提取每个字段已有的 alias 集合并继承到演进后的字段上。
     * 同时把本次事件的直接 rename 的 before 名追加为 after 字段的 alias。
     * <p>
     * 处理细节：
     * <ul>
     *   <li>若 subject 尚未注册（404 / 40401）：返回仅含本次直接 rename 的 alias map。</li>
     *   <li>历史字段在演进后仍存在：直接继承其 alias。</li>
     *   <li>历史字段被本次重命名（出现在 immediateRenames 的 value 中）：把它的 alias 转移给新名字。</li>
     *   <li>历史字段在演进后既不存在也未被 rename（即被 drop）：其 alias 不再继承（避免污染）。</li>
     * </ul>
     */
    private Map<String, Set<String>> mergeAliasHistory(String subject, Map<String, String> immediateRenames, TapTable evolvedTable) {
        Map<String, Set<String>> aliasMap = new HashMap<>();
        try {
            SchemaMetadata meta = getSchemaRegistryClient().getLatestSchemaMetadata(subject);
            if (meta != null && StringUtils.isNotBlank(meta.getSchema())) {
                Schema previous = new Schema.Parser().parse(meta.getSchema());
                Map<String, TapField> evolvedFields = evolvedTable.getNameFieldMap();
                for (Schema.Field f : previous.getFields()) {
                    Set<String> aliases = f.aliases();
                    if (aliases == null || aliases.isEmpty()) {
                        continue;
                    }
                    if (evolvedFields != null && evolvedFields.containsKey(f.name())) {
                        aliasMap.computeIfAbsent(f.name(), k -> new LinkedHashSet<>()).addAll(aliases);
                    } else {
                        // 字段名不在新 schema 中：可能本次被 rename，把 alias 转移给新名字
                        for (Map.Entry<String, String> e : immediateRenames.entrySet()) {
                            if (Objects.equals(e.getValue(), f.name())) {
                                aliasMap.computeIfAbsent(e.getKey(), k -> new LinkedHashSet<>()).addAll(aliases);
                            }
                        }
                    }
                }
            }
        } catch (RestClientException re) {
            // 40401: subject not found；首次注册时正常发生，忽略即可
            if (re.getStatus() != 404 && re.getErrorCode() != 40401) {
                tapLogger.warn("Fetch latest schema for subject '{}' failed (status={}, code={}): {}",
                        subject, re.getStatus(), re.getErrorCode(), re.getMessage());
            }
        } catch (IOException ioe) {
            tapLogger.warn("Fetch latest schema for subject '{}' failed: {}", subject, ioe.getMessage());
        }
        // 追加本次事件的直接 rename：after 字段的 alias 中加入 before 名
        for (Map.Entry<String, String> e : immediateRenames.entrySet()) {
            aliasMap.computeIfAbsent(e.getKey(), k -> new LinkedHashSet<>()).add(e.getValue());
        }
        return aliasMap;
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
        return buildAvroSchemaFromTable(tapTable, null);
    }

    private Schema buildAvroSchemaFromTable(TapTable tapTable, Map<String, Set<String>> aliasMap) {
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
                SchemaBuilder.FieldBuilder<Schema> fb = fieldAssembler.name(columnName);
                if (aliasMap != null) {
                    Set<String> aliases = aliasMap.get(columnName);
                    if (aliases != null && !aliases.isEmpty()) {
                        fb = fb.aliases(aliases.toArray(new String[0]));
                    }
                }
                if (EmptyKit.isNotNull(tapField.getDefaultValue()) && applyDefault) {
                    fb.type(field.schema()).withDefault(tapField.getDefaultValue());
                } else if (isNullableUnion(field.schema())) {
                    // 可空字段统一兜底为 null default，保证 Schema Registry BACKWARD 兼容（新增列对旧消费者可读）
                    fb.type(field.schema()).withDefault(null);
                } else {
                    fb.type(field.schema()).noDefault();
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
