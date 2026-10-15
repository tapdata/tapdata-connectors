package io.tapdata.kafka.schema_mode;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Any;
import com.google.protobuf.Api;
import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
import com.google.protobuf.ListValue;
import com.google.protobuf.Method;
import com.google.protobuf.Mixin;
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;
import com.google.protobuf.Option;
import com.google.protobuf.SourceContext;
import com.google.protobuf.Struct;
import com.google.protobuf.Type;
import com.google.protobuf.Value;
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

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.constant.DMLType.*;

/**
 * Registry Protobuf 模式实现
 * 使用 Confluent Schema Registry 和 Protobuf 序列化
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 */
public class RegistryProtobufMode extends AbsSchemaMode {

    private static final String REPEATED_SCALAR_PREFIX = "__protobuf_repeated_scalar__:";
    private static final String REPEATED_ENUM_PREFIX = "__protobuf_repeated_enum__:";
    private static final String MAP_TYPE_PREFIX = "__protobuf_map__:";

    // 缓存表的 Protobuf Descriptor，避免重复构建
    private final Map<String, Descriptors.Descriptor> descriptorCache = new ConcurrentHashMap<>();
    private final Map<String, String> descriptorSchemaKeys = new ConcurrentHashMap<>();
    // 每个 topic 最近一次见到的 Protobuf Descriptor 引用；命中时（==）走快速路径，跳过任何 schema diff 计算
    private final Map<String, Descriptors.Descriptor> lastDescriptorPerTopic = new ConcurrentHashMap<>();

    public RegistryProtobufMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.REGISTRY_PROTOBUF, kafkaService);
    }

    @Override
    public void sampleOneSchema(String table, TapTable sampleTable) {
        kafkaService.<String, Object>sampleValue(Collections.singletonList(table), null, record -> {
            if (null != record) {
                Object value = record.value();
                if (!(value instanceof DynamicMessage)) {
                    tapLogger.warn("Skip Protobuf schema sample because value is not DynamicMessage, topic={}, valueClass={}",
                            table, value == null ? null : value.getClass().getName());
                    return true;
                }

                DynamicMessage message = (DynamicMessage) value;
                Descriptors.Descriptor descriptor = message.getDescriptorForType();
                tapLogger.info("Protobuf schema sample, topic={}, messageType={}, declaredFields={}, populatedFields={}",
                        table, descriptor.getFullName(), descriptor.getFields().size(), message.getAllFields().size());

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
            return true;
        });
    }


    private void dynamicMessageToTapTable(TapTable sampleTable, DynamicMessage message, List<String> primaryKeys) {
        for (FlattenedField flattenedField : flattenDescriptorFields(message.getDescriptorForType())) {
            Descriptors.FieldDescriptor protobufField = flattenedField.field;
            String fieldName = flattenedField.path;
            String tapType = protobufPrimaryTypeName(protobufField);
            tapLogger.info("Protobuf schema field, name={}, protobufType={}, javaType={}, repeated={}, mapField={}, tapType={}",
                    fieldName, protobufField.getType(), protobufField.getJavaType(), protobufField.isRepeated(),
                    protobufField.isMapField(), tapType);

            TapField field = new TapField(fieldName, tapType);
            field.setNullable(!protobufField.isRequired());
            if (protobufField.hasDefaultValue()) {
                field.setDefaultValue(protobufField.getDefaultValue());
            }
            setProtobufMessageType(field, protobufField);
            if (flattenedField.parentPath.isEmpty() && primaryKeys.contains(protobufField.getName())) {
                field.setPrimaryKey(true);
                field.setPrimaryKeyPos(primaryKeys.indexOf(protobufField.getName()) + 1);
            }
            sampleTable.add(field);
        }
    }

    private List<FlattenedField> flattenDescriptorFields(Descriptors.Descriptor descriptor) {
        List<FlattenedField> fields = new ArrayList<>();
        flattenDescriptorFields(descriptor, "", new HashSet<>(), fields, 0);
        return fields;
    }

    private void flattenDescriptorFields(Descriptors.Descriptor descriptor,
                                         String parentPath,
                                         Set<Descriptors.Descriptor> descriptorPath,
                                         List<FlattenedField> fields,
                                         int depth) {
        if (descriptor == null || depth > 32 || !descriptorPath.add(descriptor)) {
            if (descriptor != null) {
                tapLogger.warn("Skip recursive Protobuf descriptor, messageType={}, parentPath={}",
                        descriptor.getFullName(), parentPath);
            }
            return;
        }
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            String path = StringUtils.isBlank(parentPath) ? field.getName() : parentPath + "." + field.getName();
            fields.add(new FlattenedField(path, parentPath, field));
            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                    && !field.isMapField()
                    && !isWellKnownMessage(field.getMessageType())) {
                // TapTable keeps nested array element fields as dotted metadata, while
                // convertDynamicMessageToMap keeps the runtime value as List<Map> so
                // downstream document targets can preserve the array structure.
                flattenDescriptorFields(field.getMessageType(), path, descriptorPath, fields, depth + 1);
            }
        }
        descriptorPath.remove(descriptor);
    }

    private boolean isWellKnownMessage(Descriptors.Descriptor descriptor) {
        if (descriptor == null) {
            return false;
        }
        // Keep dynamic container messages opaque. Wrapper messages and Timestamp are
        // intentionally expanded because TapData schemas expose their value/seconds/nanos fields.
        switch (descriptor.getFullName()) {
            case "google.protobuf.Any":
            case "google.protobuf.Api":
            case "google.protobuf.DescriptorProto":
            case "google.protobuf.Duration":
            case "google.protobuf.Empty":
            case "google.protobuf.Enum":
            case "google.protobuf.EnumValue":
            case "google.protobuf.FieldMask":
            case "google.protobuf.ListValue":
            case "google.protobuf.Method":
            case "google.protobuf.Mixin":
            case "google.protobuf.Option":
            case "google.protobuf.SourceContext":
            case "google.protobuf.Struct":
            case "google.protobuf.Type":
            case "google.protobuf.Value":
                return true;
            default:
                return false;
        }
    }

    private static final class FlattenedField {
        private final String path;
        private final String parentPath;
        private final Descriptors.FieldDescriptor field;

        private FlattenedField(String path, String parentPath, Descriptors.FieldDescriptor field) {
            this.path = path;
            this.parentPath = parentPath;
            this.field = field;
        }
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
                events = detectSchemaChangesFromTableFlat(topic, knownTable, currentDescriptor, consumerRecord.timestamp());
            } else {
                events = new ArrayList<>(1);
            }
        } else {
            events = detectSchemaChangesFlat(topic, lastDescriptor, currentDescriptor, consumerRecord.timestamp());
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

            // 将 dotted key 归并为嵌套对象，兼容 TapTable 使用 address.street 形式的字段。
            Map<String, Object> normalizedData = normalizeDataForDescriptor(data, descriptor);

            // 填充字段数据
            for (Map.Entry<String, Object> entry : normalizedData.entrySet()) {
                String fieldName = sanitizeFieldName(entry.getKey());
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
        String schemaKey = descriptorSchemaKey(tapTable);
        synchronized (descriptorCache) {
            Descriptors.Descriptor cached = descriptorCache.get(tableId);
            if (cached != null && schemaKey.equals(descriptorSchemaKeys.get(tableId))) {
                return cached;
            }
            try {
                Descriptors.Descriptor rebuilt = buildProtobufDescriptor(tapTable);
                descriptorCache.put(tableId, rebuilt);
                descriptorSchemaKeys.put(tableId, schemaKey);
                return rebuilt;
            } catch (Descriptors.DescriptorValidationException e) {
                throw new RuntimeException("Failed to build Protobuf descriptor for table: " + tableId, e);
            }
        }
    }

    private String descriptorSchemaKey(TapTable tapTable) {
        StringBuilder key = new StringBuilder();
        if (tapTable != null && tapTable.getNameFieldMap() != null) {
            for (Map.Entry<String, TapField> entry : tapTable.getNameFieldMap().entrySet()) {
                TapField field = entry.getValue();
                key.append(entry.getKey()).append('|')
                        .append(field == null ? null : field.getDataType()).append('|')
                        .append(field == null ? null : field.getPureDataType()).append('|')
                        .append(field == null ? null : field.getDefaultValue()).append(';');
            }
        }
        return key.toString();
    }

    /**
     * 根据 TapTable 构建 Protobuf Descriptor
     */
    private Descriptors.Descriptor buildProtobufDescriptor(TapTable tapTable) throws Descriptors.DescriptorValidationException {
        String messageName = sanitizeMessageName(tapTable.getId());

        DescriptorProtos.DescriptorProto.Builder messageBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName(messageName);

        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (nameFieldMap != null && nameFieldMap.keySet().stream().anyMatch(name -> name.contains("."))) {
            return buildNestedProtobufDescriptor(tapTable, messageName);
        }
        if (nameFieldMap != null && !nameFieldMap.isEmpty()) {
            int fieldNumber = 1;
            for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
                String fieldName = sanitizeFieldName(entry.getKey());
                TapField tapField = entry.getValue();
                String dataType = tapField.getDataType();

                if (StringUtils.isBlank(dataType)) {
                    continue;
                }

                DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName(fieldName)
                        .setNumber(fieldNumber++)
                        .setLabel(labelForTapType(dataType))
                        .setType(mapTapTypeToProtobufType(dataType));
                String normalizedType = normalizeTapType(dataType);
                String repeatedScalarType = repeatedScalarType(tapField);
                String repeatedEnumType = repeatedEnumType(tapField);
                String explicitMessageType = protobufMessageType(tapField);
                if (repeatedScalarType != null) {
                    fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                            .setType(protobufTypeFromName(repeatedScalarType));
                } else if (repeatedEnumType != null) {
                    // The enum declaration is not persisted with TapField; downgrade
                    // repeated enums to strings instead of emitting a dangling typeName.
                    fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
                } else if (explicitMessageType != null && isWellKnownMessageName(explicitMessageType)) {
                    fieldBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName("." + explicitMessageType)
                            .setLabel(isRepeatedProtobufMessage(tapField)
                                    ? DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED
                                    : DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
                } else if ("OBJECT".equals(normalizedType)) {
                    fieldBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName(".google.protobuf.Struct");
                } else if ("ARRAY".equals(normalizedType)) {
                    fieldBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName(".google.protobuf.Value");
                } else if ("MAP".equals(normalizedType)) {
                    String entryName = sanitizeMessageName(fieldName + "Entry");
                    messageBuilder.addNestedType(buildMapEntryDescriptor(entryName, tapField));
                    fieldBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName("." + messageName + "." + entryName);
                }
                if (EmptyKit.isNotNull(tapField.getDefaultValue()) && applyDefault) {
                    fieldBuilder.setDefaultValue(String.valueOf(tapField.getDefaultValue()));
                }
                messageBuilder.addField(fieldBuilder.build());
            }
        }

        // 构建 FileDescriptor
        List<Descriptors.FileDescriptor> dependencies = protobufDependencyFiles(tapTable);
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(messageName + ".proto")
                .setSyntax("proto3")
                .setOptions(DescriptorProtos.FileOptions.newBuilder().setJavaMultipleFiles(true).build());
        for (Descriptors.FileDescriptor dependency : dependencies) {
            fileBuilder.addDependency(dependency.getName());
        }
        fileBuilder.addMessageType(messageBuilder.build());

        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                fileBuilder.build(),
                dependencies.toArray(new Descriptors.FileDescriptor[0])
        );

        return fileDescriptor.findMessageTypeByName(messageName);
    }

    private Descriptors.Descriptor buildNestedProtobufDescriptor(TapTable tapTable, String messageName)
            throws Descriptors.DescriptorValidationException {
        DescriptorTreeNode root = descriptorTree(tapTable);
        DescriptorProtos.DescriptorProto rootProto = buildNestedMessageProto(root, messageName, messageName);
        List<Descriptors.FileDescriptor> dependencies = protobufDependencyFiles(tapTable);
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(messageName + ".proto")
                .setSyntax("proto3")
                .setOptions(DescriptorProtos.FileOptions.newBuilder().setJavaMultipleFiles(true).build());
        for (Descriptors.FileDescriptor dependency : dependencies) {
            fileBuilder.addDependency(dependency.getName());
        }
        fileBuilder.addMessageType(rootProto);
        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                fileBuilder.build(), dependencies.toArray(new Descriptors.FileDescriptor[0]));
        return fileDescriptor.findMessageTypeByName(messageName);
    }

    private DescriptorTreeNode descriptorTree(TapTable tapTable) {
        DescriptorTreeNode root = new DescriptorTreeNode("", null);
        for (Map.Entry<String, TapField> entry : tapTable.getNameFieldMap().entrySet()) {
            DescriptorTreeNode current = root;
            String[] segments = entry.getKey().split("\\.");
            for (String segment : segments) {
                current = current.children.computeIfAbsent(segment, key -> new DescriptorTreeNode(key, null));
            }
            current.field = entry.getValue();
        }
        return root;
    }

    private DescriptorProtos.DescriptorProto buildNestedMessageProto(DescriptorTreeNode node,
                                                                     String messageName,
                                                                     String fullName) {
        DescriptorProtos.DescriptorProto.Builder message = DescriptorProtos.DescriptorProto.newBuilder()
                .setName(messageName);
        int fieldNumber = 1;
        for (DescriptorTreeNode child : node.children.values()) {
            TapField tapField = child.field;
            String normalizedType = tapField == null ? "OBJECT" : normalizeTapType(tapField.getDataType());
            String fieldName = sanitizeFieldName(child.name);
            DescriptorProtos.FieldDescriptorProto.Builder field = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName(fieldName)
                    .setNumber(fieldNumber++)
                    .setLabel(labelForTapType(normalizedType));

            String repeatedScalarType = repeatedScalarType(tapField);
            String repeatedEnumType = repeatedEnumType(tapField);
            String explicitMessageType = protobufMessageType(tapField);
            if (!child.children.isEmpty() && ("OBJECT".equals(normalizedType) || "ARRAY".equals(normalizedType))) {
                String nestedName = sanitizeMessageName(child.name + "Message");
                String nestedFullName = fullName + "." + nestedName;
                message.addNestedType(buildNestedMessageProto(child, nestedName, nestedFullName));
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("." + nestedFullName);
            } else if (repeatedScalarType != null) {
                field.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setType(protobufTypeFromName(repeatedScalarType));
            } else if (repeatedEnumType != null) {
                // The enum declaration is not persisted with TapField; downgrade
                // repeated enums to strings instead of emitting a dangling typeName.
                field.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
            } else if (explicitMessageType != null && isWellKnownMessageName(explicitMessageType)) {
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("." + explicitMessageType)
                        .setLabel(isRepeatedProtobufMessage(tapField)
                                ? DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED
                                : DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
            } else if ("OBJECT".equals(normalizedType)) {
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".google.protobuf.Struct");
            } else if ("ARRAY".equals(normalizedType)) {
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".google.protobuf.Value");
            } else if ("MAP".equals(normalizedType)) {
                String entryName = sanitizeMessageName(child.name + "Entry");
                message.addNestedType(buildMapEntryDescriptor(entryName, tapField));
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("." + fullName + "." + entryName);
            } else {
                field.setType(mapTapTypeToProtobufType(normalizedType));
            }
            if (tapField != null && EmptyKit.isNotNull(tapField.getDefaultValue()) && applyDefault) {
                field.setDefaultValue(String.valueOf(tapField.getDefaultValue()));
            }
            message.addField(field.build());
        }
        return message.build();
    }

    private static final class DescriptorTreeNode {
        private final String name;
        private TapField field;
        private final Map<String, DescriptorTreeNode> children = new LinkedHashMap<>();

        private DescriptorTreeNode(String name, TapField field) {
            this.name = name;
            this.field = field;
        }
    }

    /**
     * 将 TapData 类型映射为 Protobuf 类型
     */
    private DescriptorProtos.FieldDescriptorProto.Type mapTapTypeToProtobufType(String tapType) {
        switch (normalizeTapType(tapType)) {
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
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
            case "ARRAY":
            case "MAP":
            case "OBJECT":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;
            default:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
        }
    }

    private DescriptorProtos.FieldDescriptorProto.Label labelForTapType(String tapType) {
        String normalizedType = normalizeTapType(tapType);
        return "ARRAY".equals(normalizedType) || "MAP".equals(normalizedType)
                ? DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED
                : DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL;
    }

    private String normalizeTapType(String tapType) {
        return StringKit.removeParentheses(StringUtils.defaultString(tapType)).trim().toUpperCase(Locale.ROOT);
    }

    private void setProtobufMessageType(TapField tapField, Descriptors.FieldDescriptor field) {
        if (field.isMapField()) {
            Descriptors.Descriptor entry = field.getMessageType();
            tapField.setPureDataType(MAP_TYPE_PREFIX
                    + mapTypeToken(entry.findFieldByName("key")) + "|"
                    + mapTypeToken(entry.findFieldByName("value")));
        } else if (field.isRepeated()) {
            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
                tapField.setPureDataType(REPEATED_ENUM_PREFIX + field.getEnumType().getFullName());
            } else if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                tapField.setPureDataType(REPEATED_SCALAR_PREFIX + field.getType().name());
            } else if (isWellKnownMessage(field.getMessageType())) {
                tapField.setPureDataType(field.getMessageType().getFullName() + "[]");
            }
        } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                && isWellKnownMessage(field.getMessageType())) {
            tapField.setPureDataType(field.getMessageType().getFullName()
                    + (field.isRepeated() ? "[]" : ""));
        }
    }

    private String mapTypeToken(Descriptors.FieldDescriptor field) {
        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return "MESSAGE:" + field.getMessageType().getFullName();
        }
        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
            return "ENUM:" + field.getEnumType().getFullName();
        }
        return "SCALAR:" + field.getType().name();
    }

    private String mapTypeToken(TapField tapField, int index) {
        String pureType = tapField == null ? null : tapField.getPureDataType();
        if (pureType == null || !pureType.startsWith(MAP_TYPE_PREFIX)) {
            return null;
        }
        String[] types = pureType.substring(MAP_TYPE_PREFIX.length()).split("\\|", -1);
        return types.length > index ? types[index] : null;
    }

    private DescriptorProtos.DescriptorProto buildMapEntryDescriptor(String entryName, TapField tapField) {
        String keyType = mapTypeToken(tapField, 0);
        String valueType = mapTypeToken(tapField, 1);
        if (keyType == null) {
            keyType = "SCALAR:STRING";
        }
        if (valueType == null) {
            valueType = "MESSAGE:google.protobuf.Value";
        }
        return DescriptorProtos.DescriptorProto.newBuilder()
                .setName(entryName)
                .setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true).build())
                .addField(mapEntryField("key", 1, keyType))
                .addField(mapEntryField("value", 2, valueType))
                .build();
    }

    private DescriptorProtos.FieldDescriptorProto mapEntryField(String name, int number, String token) {
        DescriptorProtos.FieldDescriptorProto.Builder field = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        if (token.startsWith("SCALAR:")) {
            field.setType(protobufTypeFromName(token.substring("SCALAR:".length())));
        } else if (token.startsWith("ENUM:")) {
            // Enum declarations are not persisted in TapField; downgrade unknown
            // enum values to google.protobuf.Value instead of emitting a dangling type.
            tapLogger.warn("Downgrade unsupported protobuf map enum value type {} to google.protobuf.Value", token);
            field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(".google.protobuf.Value");
        } else if (token.startsWith("MESSAGE:")) {
            String messageType = token.substring("MESSAGE:".length());
            if (isWellKnownMessageName(messageType) && wellKnownDependency(messageType) != null) {
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("." + messageType);
            } else {
                // Custom message declarations are unavailable when rebuilding from
                // TapField alone; use the generic Value representation explicitly.
                tapLogger.warn("Downgrade unsupported protobuf map message value type {} to google.protobuf.Value", token);
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".google.protobuf.Value");
            }
        } else {
            throw new IllegalArgumentException("Unsupported map entry type " + token);
        }
        return field.build();
    }

    private String repeatedScalarType(TapField tapField) {
        String pureType = tapField == null ? null : tapField.getPureDataType();
        return pureType != null && pureType.startsWith(REPEATED_SCALAR_PREFIX)
                ? pureType.substring(REPEATED_SCALAR_PREFIX.length()) : null;
    }

    private String repeatedEnumType(TapField tapField) {
        String pureType = tapField == null ? null : tapField.getPureDataType();
        return pureType != null && pureType.startsWith(REPEATED_ENUM_PREFIX)
                ? pureType.substring(REPEATED_ENUM_PREFIX.length()) : null;
    }

    private DescriptorProtos.FieldDescriptorProto.Type protobufTypeFromName(String typeName) {
        String enumName = typeName.startsWith("TYPE_") ? typeName : "TYPE_" + typeName;
        return DescriptorProtos.FieldDescriptorProto.Type.valueOf(enumName);
    }

    private String protobufMessageType(TapField tapField) {
        if (tapField == null || StringUtils.isBlank(tapField.getPureDataType())) {
            return null;
        }
        String type = tapField.getPureDataType().trim();
        return type.endsWith("[]") ? type.substring(0, type.length() - 2) : type;
    }

    private boolean isRepeatedProtobufMessage(TapField tapField) {
        return tapField != null && StringUtils.defaultString(tapField.getPureDataType()).trim().endsWith("[]");
    }

    private boolean isWellKnownMessageName(String messageType) {
        if (messageType == null) {
            return false;
        }
        switch (messageType) {
            case "google.protobuf.Any":
            case "google.protobuf.Api":
            case "google.protobuf.DescriptorProto":
            case "google.protobuf.Duration":
            case "google.protobuf.Empty":
            case "google.protobuf.Enum":
            case "google.protobuf.EnumValue":
            case "google.protobuf.FieldMask":
            case "google.protobuf.ListValue":
            case "google.protobuf.Method":
            case "google.protobuf.Mixin":
            case "google.protobuf.Option":
            case "google.protobuf.SourceContext":
            case "google.protobuf.Struct":
            case "google.protobuf.Type":
            case "google.protobuf.Value":
                return true;
            default:
                return false;
        }
    }

    private List<Descriptors.FileDescriptor> protobufDependencyFiles(TapTable tapTable) {
        Map<String, Descriptors.FileDescriptor> dependencies = new LinkedHashMap<>();
        addDependency(dependencies, Struct.getDescriptor().getFile());
        if (tapTable != null && tapTable.getNameFieldMap() != null) {
            for (TapField field : tapTable.getNameFieldMap().values()) {
                Descriptors.FileDescriptor dependency = wellKnownDependency(protobufMessageType(field));
                if (dependency != null) {
                    addDependency(dependencies, dependency);
                }
                String mapValueType = mapTypeToken(field, 1);
                if (mapValueType != null && mapValueType.startsWith("MESSAGE:")) {
                    dependency = wellKnownDependency(mapValueType.substring("MESSAGE:".length()));
                    if (dependency != null) {
                        addDependency(dependencies, dependency);
                    }
                }
            }
        }
        return new ArrayList<>(dependencies.values());
    }

    private void addDependency(Map<String, Descriptors.FileDescriptor> dependencies,
                               Descriptors.FileDescriptor dependency) {
        if (dependency != null) {
            dependencies.put(dependency.getName(), dependency);
        }
    }

    private Descriptors.FileDescriptor wellKnownDependency(String messageType) {
        if (messageType == null) {
            return null;
        }
        switch (messageType) {
            case "google.protobuf.Any":
                return Any.getDescriptor().getFile();
            case "google.protobuf.Api":
            case "google.protobuf.Method":
            case "google.protobuf.Mixin":
            case "google.protobuf.Option":
            case "google.protobuf.Type":
                return Type.getDescriptor().getFile();
            case "google.protobuf.DescriptorProto":
            case "google.protobuf.Enum":
            case "google.protobuf.EnumValue":
                return DescriptorProtos.getDescriptor();
            case "google.protobuf.Duration":
                return Duration.getDescriptor().getFile();
            case "google.protobuf.Empty":
                return Empty.getDescriptor().getFile();
            case "google.protobuf.FieldMask":
                return FieldMask.getDescriptor().getFile();
            case "google.protobuf.SourceContext":
                return SourceContext.getDescriptor().getFile();
            case "google.protobuf.Struct":
            case "google.protobuf.ListValue":
            case "google.protobuf.Value":
                return Struct.getDescriptor().getFile();
            default:
                return null;
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
            if (fieldDescriptor.isMapField()) {
                return convertMapValue(value, fieldDescriptor);
            }
            if (fieldDescriptor.isRepeated()) {
                Collection<?> values = value instanceof Collection
                        ? (Collection<?>) value
                        : Collections.singletonList(value);
                List<Object> converted = new ArrayList<>(values.size());
                for (Object item : values) {
                    converted.add(convertRepeatedValue(item, fieldDescriptor));
                }
                return converted;
            }
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

                case MESSAGE:
                    if ("google.protobuf.Struct".equals(fieldDescriptor.getMessageType().getFullName())) {
                        return toStruct(value);
                    }
                    if ("google.protobuf.Value".equals(fieldDescriptor.getMessageType().getFullName())) {
                        return toProtobufValue(value);
                    }
                    if ("google.protobuf.ListValue".equals(fieldDescriptor.getMessageType().getFullName())) {
                        return toListValue(value);
                    }
                    if (value instanceof Message) {
                        return value;
                    }
                    DynamicMessage nested = toDynamicMessage(value, fieldDescriptor.getMessageType());
                    if (nested == null) {
                        throw new IllegalArgumentException("Expected a map or protobuf message for "
                                + fieldDescriptor.getFullName());
                    }
                    return nested;

                default:
                    throw new IllegalArgumentException("Unsupported protobuf field type "
                            + fieldDescriptor.getType() + " for " + fieldDescriptor.getFullName());
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to convert value to protobuf field "
                    + fieldDescriptor.getFullName() + ", valueClass=" + value.getClass().getName(), e);
        }
    }

    private Object convertRepeatedValue(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return convertRepeatedScalarValue(value, fieldDescriptor);
        }
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                && !isWellKnownMessage(fieldDescriptor.getMessageType())) {
            DynamicMessage nested = toDynamicMessage(value, fieldDescriptor.getMessageType());
            if (nested == null) {
                throw new IllegalArgumentException("Expected a map or protobuf message for repeated field "
                        + fieldDescriptor.getFullName());
            }
            return nested;
        }
        if ("google.protobuf.Struct".equals(fieldDescriptor.getMessageType().getFullName())) {
            return toStruct(value);
        }
        if ("google.protobuf.ListValue".equals(fieldDescriptor.getMessageType().getFullName())) {
            return toListValue(value);
        }
        if ("google.protobuf.Value".equals(fieldDescriptor.getMessageType().getFullName())) {
            return toProtobufValue(value);
        }
        DynamicMessage nested = toDynamicMessage(value, fieldDescriptor.getMessageType());
        if (nested == null) {
            throw new IllegalArgumentException("Expected a map or protobuf message for repeated field "
                    + fieldDescriptor.getFullName());
        }
        return nested;
    }

    private Object convertRepeatedScalarValue(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getType()) {
            case BOOL:
                return value instanceof Boolean ? value : Boolean.parseBoolean(value.toString());
            case INT32:
            case SINT32:
            case SFIXED32:
            case UINT32:
            case FIXED32:
                return value instanceof Integer ? value : Integer.parseInt(value.toString());
            case INT64:
            case SINT64:
            case SFIXED64:
            case UINT64:
            case FIXED64:
                return value instanceof Long ? value : Long.parseLong(value.toString());
            case FLOAT:
                return value instanceof Float ? value : Float.parseFloat(value.toString());
            case DOUBLE:
                return value instanceof Double ? value : Double.parseDouble(value.toString());
            case STRING:
                return value.toString();
            case BYTES:
                return value instanceof byte[]
                        ? com.google.protobuf.ByteString.copyFrom((byte[]) value)
                        : com.google.protobuf.ByteString.copyFromUtf8(value.toString());
            case ENUM:
                if (value instanceof Descriptors.EnumValueDescriptor) {
                    return value;
                }
                Descriptors.EnumValueDescriptor enumValue = fieldDescriptor.getEnumType()
                        .findValueByName(value.toString());
                if (enumValue == null) {
                    throw new IllegalArgumentException("Unknown enum value " + value
                            + " for " + fieldDescriptor.getFullName());
                }
                return enumValue;
            default:
                throw new IllegalArgumentException("Unsupported repeated protobuf field type "
                        + fieldDescriptor.getType() + " for " + fieldDescriptor.getFullName());
        }
    }

    private DynamicMessage toDynamicMessage(Object value, Descriptors.Descriptor descriptor) {
        if (value instanceof DynamicMessage) {
            DynamicMessage message = (DynamicMessage) value;
            if (descriptor.equals(message.getDescriptorForType())) {
                return message;
            }
        }
        if (value instanceof Message) {
            Message message = (Message) value;
            if (descriptor.equals(message.getDescriptorForType())) {
                return DynamicMessage.newBuilder(descriptor).mergeFrom(message).build();
            }
        }
        if (!(value instanceof Map)) {
            return null;
        }

        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        Map<String, Object> fields = normalizeDataForDescriptor((Map<String, Object>) value, descriptor);
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            Descriptors.FieldDescriptor nestedField = descriptor.findFieldByName(sanitizeFieldName(entry.getKey()));
            if (nestedField == null || entry.getValue() == null) {
                continue;
            }
            Object converted = convertValue(entry.getValue(), nestedField);
            if (converted != null) {
                builder.setField(nestedField, converted);
            }
        }
        return builder.build();
    }

    private Map<String, Object> normalizeDataForDescriptor(Map<String, Object> data,
                                                           Descriptors.Descriptor descriptor) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        if (data == null) {
            return normalized;
        }
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String key = entry.getKey();
            if (key == null) {
                continue;
            }
            String sanitizedKey = sanitizeFieldName(key);
            if (key.contains(".")) {
                String rootKey = key.substring(0, key.indexOf('.'));
                if (descriptor.findFieldByName(sanitizeFieldName(rootKey)) != null
                        && data.containsKey(rootKey)) {
                    // The nested root value is authoritative; dotted aliases are
                    // read-side compatibility keys and must not replace arrays/maps.
                    continue;
                }
            }
            if (descriptor.findFieldByName(sanitizedKey) != null || !key.contains(".")) {
                normalized.put(key, entry.getValue());
            } else {
                putDottedValue(normalized, key, entry.getValue());
            }
        }
        return normalized;
    }

    @SuppressWarnings("unchecked")
    private void putDottedValue(Map<String, Object> target, String path, Object value) {
        String[] parts = path.split("\\.");
        Map<String, Object> current = target;
        for (int i = 0; i < parts.length - 1; i++) {
            Object existing = current.get(parts[i]);
            if (!(existing instanceof Map)) {
                existing = new LinkedHashMap<String, Object>();
                current.put(parts[i], existing);
            }
            current = (Map<String, Object>) existing;
        }
        String leaf = parts[parts.length - 1];
        Object existing = current.get(leaf);
        if (existing instanceof Map && value instanceof Map) {
            ((Map<String, Object>) existing).putAll((Map<String, Object>) value);
        } else {
            current.put(leaf, value);
        }
    }

    private List<DynamicMessage> convertMapValue(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Expected a map for " + fieldDescriptor.getFullName());
        }
        Descriptors.Descriptor entryDescriptor = fieldDescriptor.getMessageType();
        Descriptors.FieldDescriptor keyField = entryDescriptor.findFieldByName("key");
        Descriptors.FieldDescriptor valueField = entryDescriptor.findFieldByName("value");
        List<DynamicMessage> entries = new ArrayList<>();
        for (Map.Entry<?, ?> item : ((Map<?, ?>) value).entrySet()) {
            DynamicMessage.Builder entry = DynamicMessage.newBuilder(entryDescriptor);
            entry.setField(keyField, convertMapEntryValue(item.getKey(), keyField));
            Object convertedValue = convertMapEntryValue(item.getValue(), valueField);
            if (convertedValue != null) {
                entry.setField(valueField, convertedValue);
            }
            entries.add(entry.build());
        }
        return entries;
    }

    private Object convertMapEntryValue(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (value == null) {
            if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                    && "google.protobuf.Value".equals(fieldDescriptor.getMessageType().getFullName())) {
                return toProtobufValue(null);
            }
            return null;
        }
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return convertValue(value, fieldDescriptor);
        }
        return convertRepeatedScalarValue(value, fieldDescriptor);
    }

    private Struct toStruct(Object value) {
        if (value instanceof Struct) {
            return (Struct) value;
        }
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Expected a map for google.protobuf.Struct");
        }
        Struct.Builder builder = Struct.newBuilder();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            builder.putFields(String.valueOf(entry.getKey()), toProtobufValue(entry.getValue()));
        }
        return builder.build();
    }

    private ListValue toListValue(Object value) {
        if (value instanceof ListValue) {
            return (ListValue) value;
        }
        if (!(value instanceof Collection)) {
            throw new IllegalArgumentException("Expected a collection for google.protobuf.ListValue");
        }
        ListValue.Builder builder = ListValue.newBuilder();
        for (Object item : (Collection<?>) value) {
            builder.addValues(toProtobufValue(item));
        }
        return builder.build();
    }

    private Value toProtobufValue(Object value) {
        if (value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        if (value instanceof Value) {
            return (Value) value;
        }
        Value.Builder builder = Value.newBuilder();
        if (value instanceof Boolean) {
            return builder.setBoolValue((Boolean) value).build();
        }
        if (value instanceof Number) {
            Number numberValue = (Number) value;
            if (value instanceof Double || value instanceof Float) {
                double numericValue = numberValue.doubleValue();
                if (!Double.isFinite(numericValue)) {
                    return builder.setStringValue(value.toString()).build();
                }
                return builder.setNumberValue(numericValue).build();
            }
            BigDecimal number = new BigDecimal(value.toString());
            double numericValue = numberValue.doubleValue();
            boolean exactlyRepresented = Double.isFinite(numericValue)
                    && BigDecimal.valueOf(numericValue).compareTo(number) == 0;
            if (!exactlyRepresented) {
                // google.protobuf.Value stores numbers as double; use a string rather
                // than silently changing a high-precision integer or decimal.
                return builder.setStringValue(value.toString()).build();
            }
            return builder.setNumberValue(numericValue).build();
        }
        if (value instanceof Map) {
            return builder.setStructValue(toStruct(value)).build();
        }
        if (value instanceof Collection) {
            ListValue.Builder list = ListValue.newBuilder();
            for (Object item : (Collection<?>) value) {
                list.addValues(toProtobufValue(item));
            }
            return builder.setListValue(list.build()).build();
        }
        return builder.setStringValue(String.valueOf(value)).build();
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

    private String sanitizeFieldName(String name) {
        if (StringUtils.isBlank(name)) {
            return "field";
        }
        String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
        if (!Character.isLetter(sanitized.charAt(0)) && sanitized.charAt(0) != '_') {
            sanitized = "field_" + sanitized;
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
            addDottedAliases(result, fieldName, convertedValue);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private void addDottedAliases(Map<String, Object> target, String prefix, Object value) {
        if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                String path = prefix + "." + entry.getKey();
                Object child = entry.getValue();
                target.putIfAbsent(path, child);
                addDottedAliases(target, path, child);
            }
        } else if (value instanceof Collection) {
            Map<String, List<Object>> children = collectionChildren((Collection<?>) value);
            for (Map.Entry<String, List<Object>> entry : children.entrySet()) {
                String path = prefix + "." + entry.getKey();
                target.putIfAbsent(path, entry.getValue());
                addDottedAliases(target, path, entry.getValue());
            }
        }
    }

    private Map<String, List<Object>> collectionChildren(Collection<?> values) {
        Map<String, List<Object>> children = new LinkedHashMap<>();
        for (Object item : values) {
            if (item instanceof Map) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) item).entrySet()) {
                    children.computeIfAbsent(String.valueOf(entry.getKey()), key -> new ArrayList<>())
                            .add(entry.getValue());
                }
            } else if (item instanceof Collection) {
                Map<String, List<Object>> nested = collectionChildren((Collection<?>) item);
                for (Map.Entry<String, List<Object>> entry : nested.entrySet()) {
                    children.computeIfAbsent(entry.getKey(), key -> new ArrayList<>())
                            .add(entry.getValue());
                }
            }
        }
        return children;
    }

    /**
     * 将 Protobuf 值转换为 Java 对象
     */
    private Object convertProtobufValueToJava(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (value == null) {
            return null;
        }

        if (fieldDescriptor.isMapField()) {
            return convertProtobufMapToJava(value, fieldDescriptor);
        }

        // 处理重复字段（数组）
        if (fieldDescriptor.isRepeated()) {
            if (value instanceof Collection) {
                Collection<?> list = (Collection<?>) value;
                List<Object> result = new ArrayList<>(list.size());
                for (Object item : list) {
                    result.add(convertSingleProtobufValue(item, fieldDescriptor));
                }
                return result;
            }
        }

        return convertSingleProtobufValue(value, fieldDescriptor);
    }

    private Map<String, Object> convertProtobufMapToJava(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        Map<String, Object> result = new LinkedHashMap<>();
        Descriptors.Descriptor entryDescriptor = fieldDescriptor.getMessageType();
        Descriptors.FieldDescriptor keyField = entryDescriptor.findFieldByName("key");
        Descriptors.FieldDescriptor valueField = entryDescriptor.findFieldByName("value");
        if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                result.put(String.valueOf(entry.getKey()), convertSingleProtobufValue(entry.getValue(), valueField));
            }
            return result;
        }
        if (value instanceof Collection) {
            for (Object item : (Collection<?>) value) {
                if (item instanceof DynamicMessage) {
                    DynamicMessage mapEntry = (DynamicMessage) item;
                    result.put(String.valueOf(mapEntry.getField(keyField)),
                            convertSingleProtobufValue(mapEntry.getField(valueField), valueField));
                }
            }
        }
        return result;
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
                    DynamicMessage message = (DynamicMessage) value;
                    String messageType = message.getDescriptorForType().getFullName();
                    if ("google.protobuf.Value".equals(messageType)) {
                        return convertProtobufValueMessage(message);
                    }
                    if ("google.protobuf.Struct".equals(messageType)) {
                        return convertStructMessage(message);
                    }
                    if ("google.protobuf.ListValue".equals(messageType)) {
                        return convertListValueMessage(message);
                    }
                    return convertDynamicMessageToMap(message);
                }
                if (value instanceof Value) {
                    return convertGeneratedValue((Value) value);
                }
                if (value instanceof Struct) {
                    Map<String, Object> result = new LinkedHashMap<>();
                    ((Struct) value).getFieldsMap().forEach((key, item) -> result.put(key, convertGeneratedValue(item)));
                    return result;
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

    private Object convertProtobufValueMessage(DynamicMessage message) {
        Map<Descriptors.FieldDescriptor, Object> fields = message.getAllFields();
        if (fields.isEmpty()) {
            return null;
        }
        Map.Entry<Descriptors.FieldDescriptor, Object> field = fields.entrySet().iterator().next();
        if ("null_value".equals(field.getKey().getName())) {
            return null;
        }
        return convertSingleProtobufValue(field.getValue(), field.getKey());
    }

    private Map<String, Object> convertStructMessage(DynamicMessage message) {
        Descriptors.FieldDescriptor fields = message.getDescriptorForType().findFieldByName("fields");
        return convertProtobufMapToJava(message.getField(fields), fields);
    }

    private List<Object> convertListValueMessage(DynamicMessage message) {
        Descriptors.FieldDescriptor values = message.getDescriptorForType().findFieldByName("values");
        return (List<Object>) convertProtobufValueToJava(message.getField(values), values);
    }

    private Object convertGeneratedValue(Value value) {
        switch (value.getKindCase()) {
            case NULL_VALUE:
                return null;
            case NUMBER_VALUE:
                return value.getNumberValue();
            case STRING_VALUE:
                return value.getStringValue();
            case BOOL_VALUE:
                return value.getBoolValue();
            case STRUCT_VALUE:
                Map<String, Object> map = new LinkedHashMap<>();
                value.getStructValue().getFieldsMap().forEach((key, item) -> map.put(key, convertGeneratedValue(item)));
                return map;
            case LIST_VALUE:
                List<Object> list = new ArrayList<>();
                value.getListValue().getValuesList().forEach(item -> list.add(convertGeneratedValue(item)));
                return list;
            default:
                return null;
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
    private Map<String, Descriptors.FieldDescriptor> descriptorFieldsByPath(Descriptors.Descriptor descriptor) {
        Map<String, Descriptors.FieldDescriptor> result = new LinkedHashMap<>();
        for (FlattenedField field : flattenDescriptorFields(descriptor)) {
            result.put(field.path, field.field);
        }
        return result;
    }

    private String findSameNumberPath(Map<String, Descriptors.FieldDescriptor> fields,
                                      String newPath,
                                      Descriptors.FieldDescriptor newField) {
        String parentPath = parentPath(newPath);
        for (Map.Entry<String, Descriptors.FieldDescriptor> entry : fields.entrySet()) {
            if (Objects.equals(parentPath(entry.getKey()), parentPath)
                    && entry.getValue().getNumber() == newField.getNumber()) {
                return entry.getKey();
            }
        }
        return null;
    }

    private String parentPath(String path) {
        int index = path.lastIndexOf('.');
        return index < 0 ? "" : path.substring(0, index);
    }

    private void addRenameEvent(List<TapEvent> events, String topic, String before, String after, long referenceTime) {
        TapAlterFieldNameEvent rename = new TapAlterFieldNameEvent();
        rename.setTime(System.currentTimeMillis());
        rename.setTableId(topic);
        rename.setReferenceTime(referenceTime);
        rename.nameChange(ValueChange.create(before, after));
        events.add(rename);
    }

    private List<TapEvent> detectSchemaChangesFlat(String topic,
                                                   Descriptors.Descriptor oldDescriptor,
                                                   Descriptors.Descriptor newDescriptor,
                                                   long referenceTime) {
        Map<String, Descriptors.FieldDescriptor> oldFields = descriptorFieldsByPath(oldDescriptor);
        Map<String, Descriptors.FieldDescriptor> newFields = descriptorFieldsByPath(newDescriptor);
        Map<String, String> renames = new LinkedHashMap<>();
        List<TapEvent> events = new ArrayList<>();

        for (Map.Entry<String, Descriptors.FieldDescriptor> entry : newFields.entrySet()) {
            if (oldFields.containsKey(entry.getKey())) continue;
            String oldPath = findSameNumberPath(oldFields, entry.getKey(), entry.getValue());
            if (oldPath != null && !newFields.containsKey(oldPath)) {
                renames.put(entry.getKey(), oldPath);
                addRenameEvent(events, topic, oldPath, entry.getKey(), referenceTime);
            }
        }

        List<String> remainingDrops = oldFields.keySet().stream()
                .filter(path -> !newFields.containsKey(path) && !renames.containsValue(path))
                .collect(Collectors.toList());
        List<String> remainingAdds = newFields.keySet().stream()
                .filter(path -> !oldFields.containsKey(path) && !renames.containsKey(path))
                .collect(Collectors.toList());
        if (remainingDrops.size() == 1 && remainingAdds.size() == 1) {
            String before = remainingDrops.get(0);
            String after = remainingAdds.get(0);
            if (Objects.equals(parentPath(before), parentPath(after))
                    && Objects.equals(protobufPrimaryTypeName(oldFields.get(before)), protobufPrimaryTypeName(newFields.get(after)))) {
                renames.put(after, before);
                addRenameEvent(events, topic, before, after, referenceTime);
            }
        }

        List<TapField> added = new ArrayList<>();
        for (Map.Entry<String, Descriptors.FieldDescriptor> entry : newFields.entrySet()) {
            if (!oldFields.containsKey(entry.getKey()) && !renames.containsKey(entry.getKey())) {
                added.add(protobufFieldToTapField(entry.getKey(), entry.getValue()));
            }
        }
        if (!added.isEmpty()) {
            TapNewFieldEvent event = new TapNewFieldEvent();
            event.setTime(System.currentTimeMillis());
            event.setTableId(topic);
            event.setReferenceTime(referenceTime);
            event.setNewFields(added);
            events.add(event);
        }

        Set<String> renameSources = new HashSet<>(renames.values());
        for (String oldPath : oldFields.keySet()) {
            if (newFields.containsKey(oldPath) || renameSources.contains(oldPath)) continue;
            TapDropFieldEvent event = new TapDropFieldEvent();
            event.setTime(System.currentTimeMillis());
            event.setTableId(topic);
            event.setReferenceTime(referenceTime);
            event.fieldName(oldPath);
            events.add(event);
        }

        for (Map.Entry<String, Descriptors.FieldDescriptor> entry : newFields.entrySet()) {
            Descriptors.FieldDescriptor oldField = oldFields.get(entry.getKey());
            if (oldField == null) continue;
            TapAlterFieldAttributesEvent event = diffFieldAttributes(topic, entry.getKey(), oldField, entry.getValue(), referenceTime);
            if (event != null) events.add(event);
        }
        return events;
    }

    private List<TapEvent> detectSchemaChangesFromTableFlat(String topic,
                                                            TapTable tapTable,
                                                            Descriptors.Descriptor newDescriptor,
                                                            long referenceTime) {
        final Map<String, TapField> oldFields = tapTable.getNameFieldMap() == null
                ? Collections.emptyMap() : tapTable.getNameFieldMap();
        Map<String, Descriptors.FieldDescriptor> newFields = descriptorFieldsByPath(newDescriptor);
        List<TapEvent> events = new ArrayList<>();
        Map<String, String> renames = new LinkedHashMap<>();

        List<String> remainingDrops = oldFields.keySet().stream()
                .filter(path -> !newFields.containsKey(path))
                .collect(Collectors.toList());
        List<String> remainingAdds = newFields.keySet().stream()
                .filter(path -> !oldFields.containsKey(path))
                .collect(Collectors.toList());
        if (remainingDrops.size() == 1 && remainingAdds.size() == 1
                && Objects.equals(parentPath(remainingDrops.get(0)), parentPath(remainingAdds.get(0)))) {
            String before = remainingDrops.get(0);
            String after = remainingAdds.get(0);
            String oldType = oldFields.get(before).getDataType();
            String newType = protobufPrimaryTypeName(newFields.get(after));
            if (oldType != null && Objects.equals(StringKit.removeParentheses(oldType), newType)) {
                renames.put(after, before);
                addRenameEvent(events, topic, before, after, referenceTime);
            }
        }
        List<TapField> added = new ArrayList<>();

        for (Map.Entry<String, Descriptors.FieldDescriptor> entry : newFields.entrySet()) {
            if (!oldFields.containsKey(entry.getKey()) && !renames.containsKey(entry.getKey())) {
                added.add(protobufFieldToTapField(entry.getKey(), entry.getValue()));
            }
        }
        if (!added.isEmpty()) {
            TapNewFieldEvent event = new TapNewFieldEvent();
            event.setTime(System.currentTimeMillis());
            event.setTableId(topic);
            event.setReferenceTime(referenceTime);
            event.setNewFields(added);
            events.add(event);
        }

        Set<String> renameSources = new HashSet<>(renames.values());
        for (String oldPath : oldFields.keySet()) {
            if (newFields.containsKey(oldPath) || renameSources.contains(oldPath)) continue;
            TapDropFieldEvent event = new TapDropFieldEvent();
            event.setTime(System.currentTimeMillis());
            event.setTableId(topic);
            event.setReferenceTime(referenceTime);
            event.fieldName(oldPath);
            events.add(event);
        }

        for (Map.Entry<String, Descriptors.FieldDescriptor> entry : newFields.entrySet()) {
            TapField oldField = oldFields.get(entry.getKey());
            if (oldField == null) continue;
            TapAlterFieldAttributesEvent event = diffFieldAttributesFromTapField(topic, entry.getKey(), oldField, entry.getValue(), referenceTime);
            if (event != null) events.add(event);
        }
        return events;
    }

    private TapAlterFieldAttributesEvent diffFieldAttributes(String topic,
                                                             String fieldName,
                                                             Descriptors.FieldDescriptor oldField,
                                                             Descriptors.FieldDescriptor newField,
                                                             long referenceTime) {
        String oldType = protobufFieldSignature(oldField);
        String newType = protobufFieldSignature(newField);
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
        attr.fieldName(fieldName);
        if (dataTypeChanged) {
            attr.dataType(ValueChange.create(protobufPrimaryTypeName(oldField), protobufPrimaryTypeName(newField)));
        }
        if (nullableChanged) {
            attr.nullable(ValueChange.create(oldNullable, newNullable));
        }
        if (defaultChanged) {
            attr.defaultChange(ValueChange.create(oldDefault, newDefault));
        }
        return attr;
    }

    private TapAlterFieldAttributesEvent diffFieldAttributesFromTapField(String topic,
                                                                         String fieldName,
                                                                         TapField oldField,
                                                                         Descriptors.FieldDescriptor newField,
                                                                         long referenceTime) {
        String oldType = tapFieldSignature(oldField);
        String newType = protobufFieldSignature(newField);
        boolean hasDetailedOldType = StringUtils.isNotBlank(oldField.getPureDataType());
        if (!hasDetailedOldType) {
            oldType = StringKit.removeParentheses(StringUtils.defaultString(oldField.getDataType()));
            newType = protobufPrimaryTypeName(newField);
        }
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
        attr.fieldName(fieldName);
        if (dataTypeChanged) {
            attr.dataType(ValueChange.create(
                    hasDetailedOldType ? oldField.getDataType() : oldType,
                    protobufPrimaryTypeName(newField)));
        }
        if (nullableChanged) {
            attr.nullable(ValueChange.create(oldNullable, newNullable));
        }
        if (defaultChanged) {
            attr.defaultChange(ValueChange.create(oldDefault, newDefault));
        }
        return attr;
    }

    private String tapFieldSignature(TapField field) {
        if (field == null) {
            return null;
        }
        return StringUtils.isNotBlank(field.getPureDataType())
                ? field.getPureDataType()
                : StringKit.removeParentheses(StringUtils.defaultString(field.getDataType()));
    }

    private String protobufFieldSignature(Descriptors.FieldDescriptor field) {
        if (field == null) {
            return null;
        }
        if (field.isMapField()) {
            Descriptors.Descriptor entry = field.getMessageType();
            return MAP_TYPE_PREFIX + mapTypeToken(entry.findFieldByName("key")) + "|"
                    + mapTypeToken(entry.findFieldByName("value"));
        }
        if (field.isRepeated()) {
            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
                return REPEATED_ENUM_PREFIX + field.getEnumType().getFullName();
            }
            if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                return REPEATED_SCALAR_PREFIX + field.getType().name();
            }
            return field.getMessageType().getFullName() + "[]";
        }
        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return isWellKnownMessageName(field.getMessageType().getFullName())
                    ? field.getMessageType().getFullName()
                    : protobufPrimaryTypeName(field);
        }
        return field.getType().name();
    }

    private TapField protobufFieldToTapField(Descriptors.FieldDescriptor f) {
        return protobufFieldToTapField(f.getName(), f);
    }

    private TapField protobufFieldToTapField(String fieldName, Descriptors.FieldDescriptor f) {
        TapField field = new TapField();
        field.setName(fieldName);
        field.setDataType(protobufPrimaryTypeName(f));
        field.setNullable(!f.isRequired());
        field.setDefaultValue(f.hasDefaultValue() ? f.getDefaultValue() : null);
        setProtobufMessageType(field, f);
        return field;
    }

    /**
     * 返回 Protobuf 字段的主要类型名（与 {@link #dynamicMessageToTapTable} 处理一致）。
     */
    private String protobufPrimaryTypeName(Descriptors.FieldDescriptor f) {
        if (f == null) {
            return "STRING";
        }
        if (f.isMapField()) {
            return "MAP";
        }
        if (f.isRepeated()) {
            return "ARRAY";
        }
        switch (f.getJavaType()) {
            case INT:
                return "INTEGER";
            case LONG:
                return "LONG";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case BYTE_STRING:
                return "BINARY";
            case MESSAGE:
                if ("google.protobuf.ListValue".equals(f.getMessageType().getFullName())) {
                    return "ARRAY";
                }
                return "OBJECT";
            case ENUM:
            case STRING:
            default:
                return "STRING";
        }
    }


}
