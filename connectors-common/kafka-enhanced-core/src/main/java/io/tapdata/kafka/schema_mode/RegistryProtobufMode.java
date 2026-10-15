package io.tapdata.kafka.schema_mode;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
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
    private static final int MAX_CACHED_DESCRIPTORS = 256;
    private final Map<String, Descriptors.Descriptor> descriptorCache = new LinkedHashMap<String, Descriptors.Descriptor>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Descriptors.Descriptor> eldest) {
            return size() > MAX_CACHED_DESCRIPTORS;
        }
    };
    // Retain small schema fingerprints after descriptor eviction to reject unsafe wire-schema changes.
    private final Map<String, String> schemaKeysByTable = new ConcurrentHashMap<>();
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
        validateWritableSchema(tapTable);
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
                if (fieldDescriptor == null && value != null) {
                    throw new IllegalArgumentException("Field '" + fieldName
                            + "' is not present in the Registry Protobuf target schema");
                }
                if (fieldDescriptor != null && value != null) {
                    if (value instanceof Map || value instanceof Collection
                            || (value.getClass().isArray()
                            && fieldDescriptor.getType() != Descriptors.FieldDescriptor.Type.BYTES)) {
                        throw new UnsupportedOperationException("Registry Protobuf target cannot encode structured value for field '"
                                + fieldName + "' as " + fieldDescriptor.getType());
                    }
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
        validateWritableSchema(tapTable);
        String tableId = tapTable.getId();
        String schemaKey = descriptorSchemaKey(tapTable);
        synchronized (descriptorCache) {
            String previousKey = schemaKeysByTable.get(tableId);
            if (previousKey != null && !schemaKey.equals(previousKey)) {
                throw new IllegalStateException("Registry Protobuf target schema changed for table '" + tableId
                        + "'; writing with the cached descriptor could lose fields or change wire tags");
            }
            Descriptors.Descriptor cached = descriptorCache.get(tableId);
            if (cached == null) {
                try {
                    cached = buildProtobufDescriptor(tapTable);
                    descriptorCache.put(tableId, cached);
                    schemaKeysByTable.put(tableId, schemaKey);
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new RuntimeException("Failed to build Protobuf descriptor for table: " + tableId, e);
                }
            }
            return cached;
        }
    }

    // The target writer only supports flat scalar fields. Source-side TapTables may
    // describe nested/repeated/map Protobuf fields, which cannot be reconstructed
    // from this writer's flat descriptor without changing the wire contract.
    private void validateWritableSchema(TapTable table) {
        if (table == null || table.getNameFieldMap() == null) return;
        for (Map.Entry<String, TapField> entry : table.getNameFieldMap().entrySet()) {
            String name = entry.getKey();
            TapField field = entry.getValue();
            String type = field == null ? null : StringKit.removeParentheses(field.getDataType());
            String normalizedType = StringUtils.defaultString(type).trim().toUpperCase(Locale.ROOT);
            boolean structuredType = normalizedType.equals("ARRAY") || normalizedType.startsWith("ARRAY<")
                    || normalizedType.equals("MAP") || normalizedType.startsWith("MAP<")
                    || normalizedType.equals("OBJECT") || normalizedType.startsWith("OBJECT<")
                    || normalizedType.endsWith("[]");
            if (name == null || !name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                throw new UnsupportedOperationException("Registry Protobuf target requires flat protobuf field names; unsupported field '"
                        + name + "'");
            }
            if (StringUtils.isBlank(type) || structuredType
                    || field.getPureDataType() != null && field.getPureDataType().startsWith("__protobuf_")) {
                throw new UnsupportedOperationException("Registry Protobuf target cannot reconstruct field '"
                        + name + "' of type '" + field.getDataType()
                        + "' from TapTable without changing the protobuf wire schema");
            }
        }
    }

    private String descriptorSchemaKey(TapTable table) {
        StringBuilder key = new StringBuilder();
        if (table.getNameFieldMap() != null) {
            for (Map.Entry<String, TapField> entry : table.getNameFieldMap().entrySet()) {
                TapField field = entry.getValue();
                appendSchemaKeyPart(key, entry.getKey());
                appendSchemaKeyPart(key, field.getDataType());
                appendSchemaKeyPart(key, field.getPureDataType());
                appendSchemaKeyPart(key, field.getDefaultValue());
                appendSchemaKeyPart(key, field.getNullable());
            }
        }
        return key.toString();
    }

    private void appendSchemaKeyPart(StringBuilder key, Object value) {
        if (value == null) {
            key.append("-1:");
        } else {
            String text = String.valueOf(value);
            key.append(text.length()).append(':').append(text);
        }
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

    private String protobufPureType(TapField field) {
        return field == null ? null : field.getPureDataType();
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
        LinkedHashSet<String> keys = new LinkedHashSet<>();
        for (Object item : values) {
            if (item instanceof Map) {
                for (Object key : ((Map<?, ?>) item).keySet()) {
                    keys.add(String.valueOf(key));
                }
            } else if (item instanceof Collection) {
                keys.addAll(collectionChildren((Collection<?>) item).keySet());
            }
        }

        for (String key : keys) {
            List<Object> valuesForKey = new ArrayList<>(values.size());
            for (Object item : values) {
                if (item instanceof Map) {
                    valuesForKey.add(((Map<?, ?>) item).get(key));
                } else if (item instanceof Collection) {
                    valuesForKey.add(collectionChildren((Collection<?>) item).get(key));
                } else {
                    valuesForKey.add(null);
                }
            }
            children.put(key, valuesForKey);
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
            attr.dataType(ValueChange.create(protobufEventType(oldField), protobufEventType(newField)));
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
        boolean hasDetailedOldType = StringUtils.isNotBlank(protobufPureType(oldField));
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
                    hasDetailedOldType ? tapFieldEventType(oldField) : oldType,
                    protobufEventType(newField)));
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
        return StringUtils.isNotBlank(protobufPureType(field))
                ? protobufPureType(field)
                : StringKit.removeParentheses(StringUtils.defaultString(field.getDataType()));
    }

    private String tapFieldEventType(TapField field) {
        if (field == null || StringUtils.isBlank(protobufPureType(field))) {
            return field == null ? null : field.getDataType();
        }
        return eventTypeFromSignature(protobufPureType(field), field.getDataType());
    }

    private String protobufEventType(Descriptors.FieldDescriptor field) {
        return eventTypeFromSignature(protobufFieldSignature(field), protobufPrimaryTypeName(field));
    }

    private String eventTypeFromSignature(String signature, String fallback) {
        if (signature == null) {
            return fallback;
        }
        if (signature.startsWith(REPEATED_SCALAR_PREFIX)) {
            return "ARRAY<" + tapTypeFromProtobufType(signature.substring(REPEATED_SCALAR_PREFIX.length())) + ">";
        }
        if (signature.startsWith(REPEATED_ENUM_PREFIX)) {
            return "ARRAY<STRING>";
        }
        if (signature.startsWith(MAP_TYPE_PREFIX)) {
            return "MAP";
        }
        if (signature.endsWith("[]")) {
            return "ARRAY<OBJECT>";
        }
        return fallback;
    }

    private String tapTypeFromProtobufType(String type) {
        switch (type) {
            case "BOOL": return "BOOLEAN";
            case "INT32":
            case "SINT32":
            case "SFIXED32":
            case "UINT32":
            case "FIXED32": return "INTEGER";
            case "INT64":
            case "SINT64":
            case "SFIXED64":
            case "UINT64":
            case "FIXED64": return "LONG";
            case "FLOAT": return "FLOAT";
            case "DOUBLE": return "DOUBLE";
            case "BYTES": return "BINARY";
            default: return "STRING";
        }
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
