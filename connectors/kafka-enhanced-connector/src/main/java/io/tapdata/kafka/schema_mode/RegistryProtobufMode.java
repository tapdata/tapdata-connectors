package io.tapdata.kafka.schema_mode;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
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
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.tapdata.constant.DMLType.*;

/**
 * Registry Protobuf 模式实现
 * 使用 Confluent Schema Registry 和 Protobuf 序列化
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 */
public class RegistryProtobufMode extends AbsSchemaMode {

    // 缓存表的 Protobuf Descriptor，避免重复构建
    private final Map<String, Descriptors.Descriptor> descriptorCache = new ConcurrentHashMap<>();
    // 每个 topic 最近一次见到的 Protobuf Descriptor 引用；命中时（==）走快速路径，跳过任何 schema diff 计算
    private final Map<String, Descriptors.Descriptor> lastDescriptorPerTopic = new ConcurrentHashMap<>();

    public RegistryProtobufMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.REGISTRY_PROTOBUF, kafkaService);
    }

    @Override
    public void sampleOneSchema(String table, TapTable sampleTable) {
        kafkaService.<String, Object>sampleValue(Collections.singletonList(table), null, record -> {
            if (null != record) {
                if (record.value() instanceof DynamicMessage) {
                    DynamicMessage message = (DynamicMessage) record.value();
                    List<String> primaryKeys = new ArrayList<>();
                    if (record.key() != null) {
                        try {
                            primaryKeys.addAll(((Map<String, Object>) TapSimplify.fromJson(record.key())).keySet());
                        } catch (Exception e) {
                            tapLogger.warn("Failed to parse primary keys: {}", record.key(), e);
                        }
                    }
                    dynamicMessageToTapTable(sampleTable, message, primaryKeys);
                    return false;
                }
            }
            return true;
        });
    }


    private void dynamicMessageToTapTable(TapTable sampleTable, DynamicMessage message, List<String> primaryKeys) {
        message.getAllFields().keySet().forEach(key -> {
            TapField field = new TapField(key.getName(), toTapType(key.getType().getJavaType().name()));
            field.setDefaultValue(key.getDefaultValue());
            if (primaryKeys.contains(key.getName())) {
                field.setPrimaryKey(true);
                field.setPrimaryKeyPos(primaryKeys.indexOf(key.getName()) + 1);
            }
            sampleTable.add(field);
        });
    }

    @Override
    public List<TapEvent> toTapEvents(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord == null || consumerRecord.value() == null) {
            return Collections.emptyList();
        }
        Object value = consumerRecord.value();
        if (!(value instanceof DynamicMessage)) {
            return super.toTapEvents(consumerRecord);
        }
        Descriptors.Descriptor currentDescriptor = ((DynamicMessage) value).getDescriptorForType();
        String topic = consumerRecord.topic();
        // 快速路径：与上次见到的 descriptor 引用相同时，KafkaProtobufDeserializer 内部缓存已保证 == 命中，直接产生 DML 事件
        Descriptors.Descriptor lastDescriptor = lastDescriptorPerTopic.get(topic);
        if (lastDescriptor == currentDescriptor) {
            TapEvent dml = toTapEvent(consumerRecord);
            return dml == null ? Collections.emptyList() : Collections.singletonList(dml);
        }
        // 慢速路径：首次见到或 descriptor 已变化
        List<TapEvent> events;
        if (lastDescriptor == null) {
            // 首次：与 tableMap 中已知的 TapTable 结构对比，覆盖任务重启后内存缓存丢失、
            // 但持久化的 TapTable 已落后于实际 schema 的场景；若 tableMap 没有记录则只能基线
            TapTable knownTable = kafkaService.getConfig().tableMapGet(topic);
            if (knownTable != null) {
                events = detectSchemaChangesFromTable(topic, knownTable, currentDescriptor, consumerRecord.timestamp());
            } else {
                events = new ArrayList<>(1);
            }
        } else {
            events = detectSchemaChanges(topic, lastDescriptor, currentDescriptor, consumerRecord.timestamp());
        }
        lastDescriptorPerTopic.put(topic, currentDescriptor);
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
            // 从 Kafka 读取 Protobuf 数据
            Object value = consumerRecord.value();
            if (!(value instanceof DynamicMessage)) {
                return null;
            }

            DynamicMessage message = (DynamicMessage) value;

            // 将 DynamicMessage 转换成 Map
            Map<String, Object> data = convertDynamicMessageToMap(message);

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
            // 获取或创建 Protobuf Descriptor
            Descriptors.Descriptor descriptor = getOrCreateDescriptor(tapTable);

            // 构建 DynamicMessage
            DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);

            // 填充字段数据
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();

                Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
                if (fieldDescriptor != null && value != null) {
                    Object convertedValue = convertValue(value, fieldDescriptor);
                    if (convertedValue != null) {
                        messageBuilder.setField(fieldDescriptor, convertedValue);
                    }
                }
            }

            DynamicMessage message = messageBuilder.build();

            // 创建 Kafka 主键
            //todo key的规范
            String keyValue = createKafkaKeyValueMap(data, tapTable);

            // 创建 ProducerRecord
            ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic(tapTable, tapEvent), computePartition(createKafkaKey(data, tapTable), kafkaService.getConfig().getNodePartitionSize()),
                    tapEvent.getTime(), keyValue, message, new RecordHeaders().add("op", op.name().getBytes()));

            return Collections.singletonList(producerRecord);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert TapEvent to Protobuf message for table: " + tapTable.getId(), e);
        }
    }

    /**
     * 获取或创建表的 Protobuf Descriptor
     */
    private Descriptors.Descriptor getOrCreateDescriptor(TapTable tapTable) throws Descriptors.DescriptorValidationException {
        String tableId = tapTable.getId();
        return descriptorCache.computeIfAbsent(tableId, k -> {
            try {
                return buildProtobufDescriptor(tapTable);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new RuntimeException("Failed to build Protobuf descriptor for table: " + tableId, e);
            }
        });
    }

    /**
     * 根据 TapTable 构建 Protobuf Descriptor
     */
    private Descriptors.Descriptor buildProtobufDescriptor(TapTable tapTable) throws Descriptors.DescriptorValidationException {
        String messageName = sanitizeMessageName(tapTable.getId());

        DescriptorProtos.DescriptorProto.Builder messageBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName(messageName);

        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (nameFieldMap != null && !nameFieldMap.isEmpty()) {
            int fieldNumber = 1;
            for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
                String fieldName = entry.getKey();
                TapField tapField = entry.getValue();
                String dataType = tapField.getDataType();

                if (StringUtils.isBlank(dataType)) {
                    continue;
                }

                DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName(fieldName)
                        .setNumber(fieldNumber++)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(mapTapTypeToProtobufType(StringKit.removeParentheses(dataType)));
                if (EmptyKit.isNotNull(tapField.getDefaultValue()) && applyDefault) {
                    fieldBuilder.setDefaultValue(String.valueOf(tapField.getDefaultValue()));
                }
                messageBuilder.addField(fieldBuilder.build());
            }
        }

        // 构建 FileDescriptor
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(messageName + ".proto")
                .setSyntax("proto3")
                .setOptions(DescriptorProtos.FileOptions.newBuilder().setJavaMultipleFiles(true).build())
                .addMessageType(messageBuilder.build());

        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                fileBuilder.build(),
                new Descriptors.FileDescriptor[0]
        );

        return fileDescriptor.findMessageTypeByName(messageName);
    }

    /**
     * 将 TapData 类型映射为 Protobuf 类型
     */
    private DescriptorProtos.FieldDescriptorProto.Type mapTapTypeToProtobufType(String tapType) {
        switch (tapType.toUpperCase()) {
            case "BOOLEAN":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
            case "INTEGER":
            case "SHORT":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
            case "LONG":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            case "FLOAT":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT;
            case "DOUBLE":
            case "NUMBER":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
            case "BINARY":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
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
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
        }
    }

    /**
     * 转换值为 Protobuf 兼容的类型
     */
    private Object convertValue(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (value == null) {
            return null;
        }

        try {
            switch (fieldDescriptor.getType()) {
                case BOOL:
                    if (value instanceof Boolean) {
                        return value;
                    }
                    return Boolean.parseBoolean(value.toString());

                case INT32:
                case SINT32:
                case SFIXED32:
                    if (value instanceof Integer) {
                        return value;
                    }
                    return Integer.parseInt(value.toString());

                case INT64:
                case SINT64:
                case SFIXED64:
                    if (value instanceof Long) {
                        return value;
                    }
                    return Long.parseLong(value.toString());

                case FLOAT:
                    if (value instanceof Float) {
                        return value;
                    }
                    return Float.parseFloat(value.toString());

                case DOUBLE:
                    if (value instanceof Double) {
                        return value;
                    }
                    return Double.parseDouble(value.toString());

                case STRING:
                    return value.toString();

                case BYTES:
                    if (value instanceof byte[]) {
                        return com.google.protobuf.ByteString.copyFrom((byte[]) value);
                    }
                    return com.google.protobuf.ByteString.copyFromUtf8(value.toString());

                default:
                    return value.toString();
            }
        } catch (Exception e) {
            // 转换失败时返回字符串形式
            return value.toString();
        }
    }

    /**
     * 清理消息名称，确保符合 Protobuf 命名规范
     */
    private String sanitizeMessageName(String name) {
        if (name == null || name.isEmpty()) {
            return "Message";
        }
        // 替换非字母数字字符为下划线
        String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
        // 确保以字母开头
        if (!Character.isLetter(sanitized.charAt(0))) {
            sanitized = "M_" + sanitized;
        }
        return sanitized;
    }

    /**
     * 将 DynamicMessage 转换为 Map
     * 用于从 Kafka 读取 Protobuf 数据后转换为 TapData 格式
     */
    private Map<String, Object> convertDynamicMessageToMap(DynamicMessage message) {
        Map<String, Object> result = new HashMap<>();

        if (message == null) {
            return result;
        }

        // 获取所有已设置的字段
        Map<Descriptors.FieldDescriptor, Object> allFields = message.getAllFields();

        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : allFields.entrySet()) {
            Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
            Object value = entry.getValue();

            String fieldName = fieldDescriptor.getName();
            Object convertedValue = convertProtobufValueToJava(value, fieldDescriptor);

            result.put(fieldName, convertedValue);
        }

        return result;
    }

    /**
     * 将 Protobuf 值转换为 Java 对象
     */
    private Object convertProtobufValueToJava(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (value == null) {
            return null;
        }

        // 处理重复字段（数组）
        if (fieldDescriptor.isRepeated()) {
            if (value instanceof List) {
                List<?> list = (List<?>) value;
                List<Object> result = new ArrayList<>(list.size());
                for (Object item : list) {
                    result.add(convertSingleProtobufValue(item, fieldDescriptor));
                }
                return result;
            }
        }

        return convertSingleProtobufValue(value, fieldDescriptor);
    }

    /**
     * 转换单个 Protobuf 值
     */
    private Object convertSingleProtobufValue(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (value == null) {
            return null;
        }

        switch (fieldDescriptor.getType()) {
            case MESSAGE:
                // 嵌套消息，递归转换
                if (value instanceof DynamicMessage) {
                    return convertDynamicMessageToMap((DynamicMessage) value);
                }
                return value;

            case BYTES:
                // ByteString 转换为 byte[]
                if (value instanceof com.google.protobuf.ByteString) {
                    return ((com.google.protobuf.ByteString) value).toByteArray();
                }
                return value;

            case ENUM:
                // 枚举转换为字符串
                if (value instanceof Descriptors.EnumValueDescriptor) {
                    return ((Descriptors.EnumValueDescriptor) value).getName();
                }
                return value.toString();

            case BOOL:
            case INT32:
            case INT64:
            case UINT32:
            case UINT64:
            case SINT32:
            case SINT64:
            case FIXED32:
            case FIXED64:
            case SFIXED32:
            case SFIXED64:
            case FLOAT:
            case DOUBLE:
            case STRING:
            default:
                // 基本类型直接返回
                return value;
        }
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {
        // 高级过滤查询暂不实现
    }

    /**
     * 比较前后两个 Protobuf Descriptor，反向生成对应的 TapFieldBaseEvent。
     * 规则：
     * - 通过字段编号（field number）将旧字段名映射到新名 → {@link TapAlterFieldNameEvent}
     * - 新增字段（排除 rename 的目标） → {@link TapNewFieldEvent}
     * - 删除字段（排除 rename 的源） → {@link TapDropFieldEvent}
     * - 同名字段属性差异（dataType / nullable / default） → {@link TapAlterFieldAttributesEvent}
     */
    private List<TapEvent> detectSchemaChanges(String topic, Descriptors.Descriptor oldDescriptor, Descriptors.Descriptor newDescriptor, long referenceTime) {
        List<TapEvent> events = new ArrayList<>();
        Map<String, Descriptors.FieldDescriptor> oldFields = new LinkedHashMap<>();
        Map<Integer, Descriptors.FieldDescriptor> oldFieldsByNumber = new HashMap<>();
        for (Descriptors.FieldDescriptor f : oldDescriptor.getFields()) {
            oldFields.put(f.getName(), f);
            oldFieldsByNumber.put(f.getNumber(), f);
        }
        Map<String, Descriptors.FieldDescriptor> newFields = new LinkedHashMap<>();
        for (Descriptors.FieldDescriptor f : newDescriptor.getFields()) {
            newFields.put(f.getName(), f);
        }
        // 1) rename：通过字段编号把旧名映射到新名
        Map<String, String> renameAfterToBefore = new HashMap<>();
        for (Descriptors.FieldDescriptor nf : newDescriptor.getFields()) {
            if (oldFields.containsKey(nf.getName())) {
                continue;
            }
            Descriptors.FieldDescriptor of = oldFieldsByNumber.get(nf.getNumber());
            if (of != null && !newFields.containsKey(of.getName()) && !renameAfterToBefore.containsValue(of.getName())) {
                renameAfterToBefore.put(nf.getName(), of.getName());
                TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
                rename.setTime(System.currentTimeMillis());
                rename.setTableId(topic);
                rename.setReferenceTime(referenceTime);
                rename.nameChange(ValueChange.create(of.getName(), nf.getName()));
                events.add(rename);
            }
        }
        // 1.5) 启发式 rename 兜底：字段编号未命中时，若剩余 drop / add 各恰好 1 个且数据类型完全一致，视为 rename
        {
            List<Descriptors.FieldDescriptor> remainingDrops = new ArrayList<>();
            for (Descriptors.FieldDescriptor of : oldDescriptor.getFields()) {
                if (newFields.containsKey(of.getName())) continue;
                if (renameAfterToBefore.containsValue(of.getName())) continue;
                remainingDrops.add(of);
            }
            List<Descriptors.FieldDescriptor> remainingAdds = new ArrayList<>();
            for (Descriptors.FieldDescriptor nf : newDescriptor.getFields()) {
                if (oldFields.containsKey(nf.getName())) continue;
                if (renameAfterToBefore.containsKey(nf.getName())) continue;
                remainingAdds.add(nf);
            }
            if (remainingDrops.size() == 1 && remainingAdds.size() == 1) {
                Descriptors.FieldDescriptor d = remainingDrops.get(0);
                Descriptors.FieldDescriptor a = remainingAdds.get(0);
                String dType = protobufPrimaryTypeName(d);
                String aType = protobufPrimaryTypeName(a);
                if (Objects.equals(dType, aType)) {
                    renameAfterToBefore.put(a.getName(), d.getName());
                    TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
                    rename.setTime(System.currentTimeMillis());
                    rename.setTableId(topic);
                    rename.setReferenceTime(referenceTime);
                    rename.nameChange(ValueChange.create(d.getName(), a.getName()));
                    events.add(rename);
                    tapLogger.info("Heuristic rename detected on topic '{}' (field-number missing): {} -> {} (type={})", topic, d.getName(), a.getName(), dType);
                }
            }
        }
        // 2) 新增字段
        List<TapField> addedFields = new ArrayList<>();
        for (Descriptors.FieldDescriptor nf : newDescriptor.getFields()) {
            if (oldFields.containsKey(nf.getName())) {
                continue;
            }
            if (renameAfterToBefore.containsKey(nf.getName())) {
                continue;
            }
            TapField tf = protobufFieldToTapField(nf);
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
        for (Descriptors.FieldDescriptor of : oldDescriptor.getFields()) {
            if (newFields.containsKey(of.getName())) {
                continue;
            }
            if (renameSources.contains(of.getName())) {
                continue;
            }
            TapDropFieldEvent drop = new TapDropFieldEvent();
            drop.setTime(System.currentTimeMillis());
            drop.setTableId(topic);
            drop.setReferenceTime(referenceTime);
            drop.fieldName(of.getName());
            events.add(drop);
        }
        // 4) 属性变化
        for (Descriptors.FieldDescriptor nf : newDescriptor.getFields()) {
            Descriptors.FieldDescriptor of = oldFields.get(nf.getName());
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

    /**
     * 用 {@link TapTable} 作为基线（任务重启后内存中没有上一份 Descriptor 时使用），与新到的 Descriptor 比对生成 DDL。
     * 规则与 {@link #detectSchemaChanges} 保持一致，仅基线来源不同。
     * 注：TapTable 不携带 protobuf 字段编号，因此此处只能依赖启发式 rename。
     */
    private List<TapEvent> detectSchemaChangesFromTable(String topic, TapTable tapTable, Descriptors.Descriptor newDescriptor, long referenceTime) {
        List<TapEvent> events = new ArrayList<>();
        Map<String, TapField> oldFields = tapTable.getNameFieldMap();
        if (oldFields == null) {
            oldFields = Collections.emptyMap();
        }
        Map<String, Descriptors.FieldDescriptor> newFields = new LinkedHashMap<>();
        for (Descriptors.FieldDescriptor f : newDescriptor.getFields()) {
            newFields.put(f.getName(), f);
        }
        // 1.5) 启发式 rename 兜底：剩余 drop / add 各恰好 1 个且数据类型完全一致，视为 rename
        Map<String, String> renameAfterToBefore = new HashMap<>();
        {
            List<String> remainingDropNames = new ArrayList<>();
            for (String oldName : oldFields.keySet()) {
                if (newFields.containsKey(oldName)) continue;
                remainingDropNames.add(oldName);
            }
            List<Descriptors.FieldDescriptor> remainingAdds = new ArrayList<>();
            for (Descriptors.FieldDescriptor nf : newDescriptor.getFields()) {
                if (oldFields.containsKey(nf.getName())) continue;
                remainingAdds.add(nf);
            }
            if (remainingDropNames.size() == 1 && remainingAdds.size() == 1) {
                String dName = remainingDropNames.get(0);
                Descriptors.FieldDescriptor a = remainingAdds.get(0);
                TapField dField = oldFields.get(dName);
                String dType = dField == null || dField.getDataType() == null ? null : StringKit.removeParentheses(dField.getDataType());
                String aType = protobufPrimaryTypeName(a);
                if (dType != null && Objects.equals(dType, aType)) {
                    renameAfterToBefore.put(a.getName(), dName);
                    TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
                    rename.setTime(System.currentTimeMillis());
                    rename.setTableId(topic);
                    rename.setReferenceTime(referenceTime);
                    rename.nameChange(ValueChange.create(dName, a.getName()));
                    events.add(rename);
                    tapLogger.info("Heuristic rename detected on topic '{}' (baseline=TapTable): {} -> {} (type={})", topic, dName, a.getName(), dType);
                }
            }
        }
        // 2) 新增字段
        List<TapField> addedFields = new ArrayList<>();
        for (Descriptors.FieldDescriptor nf : newDescriptor.getFields()) {
            if (oldFields.containsKey(nf.getName())) {
                continue;
            }
            if (renameAfterToBefore.containsKey(nf.getName())) {
                continue;
            }
            TapField tf = protobufFieldToTapField(nf);
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
        for (Descriptors.FieldDescriptor nf : newDescriptor.getFields()) {
            TapField of = oldFields.get(nf.getName());
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

    private TapAlterFieldAttributesEvent diffFieldAttributes(String topic, Descriptors.FieldDescriptor oldField, Descriptors.FieldDescriptor newField, long referenceTime) {
        String oldType = protobufPrimaryTypeName(oldField);
        String newType = protobufPrimaryTypeName(newField);
        boolean oldNullable = !oldField.isRequired();
        boolean newNullable = !newField.isRequired();
        Object oldDefault = oldField.hasDefaultValue() ? oldField.getDefaultValue() : null;
        Object newDefault = newField.hasDefaultValue() ? newField.getDefaultValue() : null;
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
        attr.fieldName(newField.getName());
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

    private TapAlterFieldAttributesEvent diffFieldAttributesFromTapField(String topic, TapField oldField, Descriptors.FieldDescriptor newField, long referenceTime) {
        String oldType = oldField.getDataType() == null ? null : StringKit.removeParentheses(oldField.getDataType());
        String newType = protobufPrimaryTypeName(newField);
        boolean oldNullable = !Boolean.FALSE.equals(oldField.getNullable());
        boolean newNullable = !newField.isRequired();
        Object oldDefault = oldField.getDefaultValue();
        Object newDefault = newField.hasDefaultValue() ? newField.getDefaultValue() : null;
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
        attr.fieldName(newField.getName());
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

    private TapField protobufFieldToTapField(Descriptors.FieldDescriptor f) {
        TapField field = new TapField();
        field.setName(f.getName());
        field.setDataType(protobufPrimaryTypeName(f));
        field.setNullable(!f.isRequired());
        field.setDefaultValue(f.hasDefaultValue() ? f.getDefaultValue() : null);
        return field;
    }

    /**
     * 返回 Protobuf 字段的主要类型名（与 {@link #dynamicMessageToTapTable} 处理一致）。
     */
    private String protobufPrimaryTypeName(Descriptors.FieldDescriptor f) {
        if (f == null) {
            return "STRING";
        }
        return toTapType(f.getJavaType().name());
    }


}
