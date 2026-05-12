package io.tapdata.kafka.schema_mode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.constant.DMLType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
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
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    // 每个 topic 最近一次推断出的 schema（fieldName -> tapType）；命中时跳过 schema diff 计算
    private final Map<String, Map<String, String>> lastSchemaPerTopic = new ConcurrentHashMap<>();

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
                        try {
                            primaryKeys.addAll(((Map<String, Object>) TapSimplify.fromJson(record.key())).keySet());
                        } catch (Exception e) {
                            tapLogger.warn("Failed to parse primary keys: {}", record.key(), e);
                        }
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
    public List<TapEvent> toTapEvents(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord == null || consumerRecord.value() == null) {
            return Collections.emptyList();
        }
        Map<String, Object> data;
        try {
            data = convertToMap(consumerRecord.value());
        } catch (Exception e) {
            return super.toTapEvents(consumerRecord);
        }
        if (data == null || data.isEmpty()) {
            return super.toTapEvents(consumerRecord);
        }
        Map<String, String> currentSchema = deriveSchemaFromMap(data);
        String topic = consumerRecord.topic();
        // 快速路径：与上次推断的 schema 完全相同时，直接产生 DML 事件
        Map<String, String> lastSchema = lastSchemaPerTopic.get(topic);
        if (lastSchema != null && lastSchema.equals(currentSchema)) {
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
        return filterPrimaryKeyDDL(topic, events);
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
                    computePartition(createKafkaKey(data, tapTable), kafkaService.getConfig().getNodePartitionSize()),
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
     * 从 JSON 数据 Map 推断 schema：fieldName -> tapType（通过父类 {@link #inferTapType} 归一化）。
     */
    private Map<String, String> deriveSchemaFromMap(Map<String, Object> data) {
        Map<String, String> schema = new LinkedHashMap<>();
        if (data == null) {
            return schema;
        }
        for (Map.Entry<String, Object> e : data.entrySet()) {
            schema.put(e.getKey(), inferTapType(e.getValue()));
        }
        return schema;
    }

    /**
     * 比较前后两次推断出的 JSON schema，反向生成对应的 TapFieldBaseEvent。
     * 规则：
     * - JSON Schema 不携带原生 alias，仅依赖启发式 rename：剩余 drop / add 各恰好 1 个且数据类型完全一致 → {@link TapAlterFieldNameEvent}
     * - 新增字段（排除 rename 的目标） → {@link TapNewFieldEvent}
     * - 删除字段（排除 rename 的源） → {@link TapDropFieldEvent}
     * - 同名字段类型差异 → {@link TapAlterFieldAttributesEvent}
     */
    private List<TapEvent> detectSchemaChanges(String topic, Map<String, String> oldSchema, Map<String, String> newSchema, long referenceTime) {
        List<TapEvent> events = new ArrayList<>();
        // 1.5) 启发式 rename 兜底
        Map<String, String> renameAfterToBefore = new HashMap<>();
        {
            List<String> remainingDrops = new ArrayList<>();
            for (String oldName : oldSchema.keySet()) {
                if (newSchema.containsKey(oldName)) continue;
                remainingDrops.add(oldName);
            }
            List<String> remainingAdds = new ArrayList<>();
            for (String newName : newSchema.keySet()) {
                if (oldSchema.containsKey(newName)) continue;
                remainingAdds.add(newName);
            }
            if (remainingDrops.size() == 1 && remainingAdds.size() == 1) {
                String d = remainingDrops.get(0);
                String a = remainingAdds.get(0);
                String dType = oldSchema.get(d);
                String aType = newSchema.get(a);
                if (dType != null && Objects.equals(dType, aType)) {
                    renameAfterToBefore.put(a, d);
                    TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
                    rename.setTime(System.currentTimeMillis());
                    rename.setTableId(topic);
                    rename.setReferenceTime(referenceTime);
                    rename.nameChange(ValueChange.create(d, a));
                    events.add(rename);
                    tapLogger.info("Heuristic rename detected on topic '{}': {} -> {} (type={})", topic, d, a, dType);
                }
            }
        }
        // 2) 新增字段
        List<TapField> addedFields = new ArrayList<>();
        for (Map.Entry<String, String> e : newSchema.entrySet()) {
            if (oldSchema.containsKey(e.getKey())) continue;
            if (renameAfterToBefore.containsKey(e.getKey())) continue;
            TapField tf = new TapField(e.getKey(), e.getValue());
            tf.setNullable(true);
            addedFields.add(tf);
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
        for (String oldName : oldSchema.keySet()) {
            if (newSchema.containsKey(oldName)) continue;
            if (renameSources.contains(oldName)) continue;
            TapDropFieldEvent drop = new TapDropFieldEvent();
            drop.setTime(System.currentTimeMillis());
            drop.setTableId(topic);
            drop.setReferenceTime(referenceTime);
            drop.fieldName(oldName);
            events.add(drop);
        }
        // 4) 类型变化
        for (Map.Entry<String, String> e : newSchema.entrySet()) {
            String name = e.getKey();
            String oldType = oldSchema.get(name);
            if (oldType == null) continue;
            String newType = e.getValue();
            if (Objects.equals(oldType, newType)) continue;
            TapAlterFieldAttributesEvent attr = new TapAlterFieldAttributesEvent();
            attr.setTime(System.currentTimeMillis());
            attr.setTableId(topic);
            attr.setReferenceTime(referenceTime);
            attr.fieldName(name);
            attr.dataType(ValueChange.create(oldType, newType));
            events.add(attr);
        }
        return events;
    }

    /**
     * 用 {@link TapTable} 作为基线（任务重启后内存中没有上一份 schema 时使用），与新到的 schema 比对生成 DDL。
     * 规则与 {@link #detectSchemaChanges} 保持一致，仅基线来源不同。
     */
    private List<TapEvent> detectSchemaChangesFromTable(String topic, TapTable tapTable, Map<String, String> newSchema, long referenceTime) {
        List<TapEvent> events = new ArrayList<>();
        Map<String, TapField> oldFields = tapTable.getNameFieldMap();
        if (oldFields == null) {
            oldFields = Collections.emptyMap();
        }
        // 1.5) 启发式 rename 兜底
        Map<String, String> renameAfterToBefore = new HashMap<>();
        {
            List<String> remainingDrops = new ArrayList<>();
            for (String oldName : oldFields.keySet()) {
                if (newSchema.containsKey(oldName)) continue;
                remainingDrops.add(oldName);
            }
            List<String> remainingAdds = new ArrayList<>();
            for (String newName : newSchema.keySet()) {
                if (oldFields.containsKey(newName)) continue;
                remainingAdds.add(newName);
            }
            if (remainingDrops.size() == 1 && remainingAdds.size() == 1) {
                String d = remainingDrops.get(0);
                String a = remainingAdds.get(0);
                TapField dField = oldFields.get(d);
                String dType = dField == null || dField.getDataType() == null ? null : StringKit.removeParentheses(dField.getDataType());
                String aType = newSchema.get(a);
                if (dType != null && Objects.equals(dType, aType)) {
                    renameAfterToBefore.put(a, d);
                    TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
                    rename.setTime(System.currentTimeMillis());
                    rename.setTableId(topic);
                    rename.setReferenceTime(referenceTime);
                    rename.nameChange(ValueChange.create(d, a));
                    events.add(rename);
                    tapLogger.info("Heuristic rename detected on topic '{}' (baseline=TapTable): {} -> {} (type={})", topic, d, a, dType);
                }
            }
        }
        // 2) 新增字段
        List<TapField> addedFields = new ArrayList<>();
        for (Map.Entry<String, String> e : newSchema.entrySet()) {
            if (oldFields.containsKey(e.getKey())) continue;
            if (renameAfterToBefore.containsKey(e.getKey())) continue;
            TapField tf = new TapField(e.getKey(), e.getValue());
            tf.setNullable(true);
            addedFields.add(tf);
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
            if (newSchema.containsKey(oldName)) continue;
            if (renameSources.contains(oldName)) continue;
            TapDropFieldEvent drop = new TapDropFieldEvent();
            drop.setTime(System.currentTimeMillis());
            drop.setTableId(topic);
            drop.setReferenceTime(referenceTime);
            drop.fieldName(oldName);
            events.add(drop);
        }
        // 4) 类型变化
        for (Map.Entry<String, String> e : newSchema.entrySet()) {
            TapField of = oldFields.get(e.getKey());
            if (of == null) continue;
            String oldType = of.getDataType() == null ? null : StringKit.removeParentheses(of.getDataType());
            String newType = e.getValue();
            if (Objects.equals(oldType, newType)) continue;
            TapAlterFieldAttributesEvent attr = new TapAlterFieldAttributesEvent();
            attr.setTime(System.currentTimeMillis());
            attr.setTableId(topic);
            attr.setReferenceTime(referenceTime);
            attr.fieldName(e.getKey());
            attr.dataType(ValueChange.create(oldType, newType));
            events.add(attr);
        }
        return events;
    }
}
