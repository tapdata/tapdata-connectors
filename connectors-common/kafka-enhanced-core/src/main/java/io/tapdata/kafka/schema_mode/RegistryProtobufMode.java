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
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;
import com.google.protobuf.SourceContext;
import com.google.protobuf.Struct;
import com.google.protobuf.Type;
import com.google.protobuf.Value;
import com.google.protobuf.Timestamp;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.BoolValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.BytesValue;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
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
import java.math.BigInteger;
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
    private static final String MAP_COMPONENT_SEPARATOR = "~";
    private static final String ENUM_PREFIX = "__protobuf_enum__:";
    private static final String WIRE_TYPE_PREFIX = "__protobuf_wire_type__:";
    private static final String GENERATED_ENUM_VALUE_PREFIX = "TAPDATA_ENUM_VALUE__";
    private static final String ENCODED_ENUM_VALUE_PREFIX = GENERATED_ENUM_VALUE_PREFIX + GENERATED_ENUM_VALUE_PREFIX;
    private static final String SYNTHETIC_ENUM_ZERO_PREFIX = "TAPDATA_PROTO3_ZERO";

    // 缓存表的 Protobuf Descriptor，避免重复构建
    private final Map<String, Descriptors.Descriptor> descriptorCache = new ConcurrentHashMap<>();
    private final Map<String, String> descriptorSchemaKeys = new ConcurrentHashMap<>();
    private final Map<String, Object> descriptorLocks = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Integer>> fieldNumbersBySubject = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Set<Integer>>> reservedFieldNumbersBySubject = new ConcurrentHashMap<>();
    private final Set<String> fieldNumbersInitialized = ConcurrentHashMap.newKeySet();
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
        if (descriptor == null) {
            return;
        }
        if (depth > 32) {
            tapLogger.warn("Skip Protobuf descriptor beyond maximum depth 32, messageType={}, parentPath={}",
                    descriptor.getFullName(), parentPath);
            return;
        }
        if (!descriptorPath.add(descriptor)) {
            tapLogger.warn("Skip recursive Protobuf descriptor reference, messageType={}, parentPath={}",
                    descriptor.getFullName(), parentPath);
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
        // Keep well-known messages opaque so their original wire type can be rebuilt.
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
            case "google.protobuf.Timestamp":
            case "google.protobuf.DoubleValue":
            case "google.protobuf.FloatValue":
            case "google.protobuf.Int32Value":
            case "google.protobuf.Int64Value":
            case "google.protobuf.UInt32Value":
            case "google.protobuf.UInt64Value":
            case "google.protobuf.BoolValue":
            case "google.protobuf.StringValue":
            case "google.protobuf.BytesValue":
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
            String outputTopic = topic(tapTable, tapEvent);
            // 获取或创建 Protobuf Descriptor
            Descriptors.Descriptor descriptor = getOrCreateDescriptor(tapTable, outputTopic);

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
            ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(outputTopic, computePartition(createKafkaKey(data, tapTable), kafkaService.getConfig().getNodePartitionSize()),
                    tapEvent.getTime(), keyValue, message, new RecordHeaders().add("op", op.name().getBytes()));

            return Collections.singletonList(producerRecord);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert TapEvent to Protobuf message for table: " + tapTable.getId(), e);
        }
    }

    /**
     * 获取或创建表的 Protobuf Descriptor
     */
    private Descriptors.Descriptor getOrCreateDescriptor(TapTable tapTable, String outputTopic) throws Descriptors.DescriptorValidationException {
        String tableId = tapTable.getId();
        Object lock = descriptorLocks.computeIfAbsent(tableId, key -> new Object());
        synchronized (lock) {
            initializeFieldNumbers(tapTable, outputTopic + "-value");
            String schemaKey = outputTopic + "|" + descriptorSchemaKey(tapTable);
            Descriptors.Descriptor cached = descriptorCache.get(tableId);
            if (cached != null && schemaKey.equals(descriptorSchemaKeys.get(tableId))) {
                return cached;
            }
            try {
                Descriptors.Descriptor rebuilt = buildProtobufDescriptor(tapTable, outputTopic);
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

    private final Map<String, List<Descriptors.Descriptor>> wireHistoryBySubject = new ConcurrentHashMap<>();

    private void initializeFieldNumbers(TapTable tapTable, String subject) {
        String tableId = tapTable.getId();
        if (fieldNumbersInitialized.contains(subject)) return;
        synchronized (fieldNumbersInitialized) {
            if (fieldNumbersInitialized.contains(subject)) return;
            try {
                Descriptors.Descriptor registered = latestRegisteredDescriptor(subject);
                Map<String, Integer> numbers = fieldNumbersBySubject.computeIfAbsent(subject,
                        key -> new ConcurrentHashMap<>());
                if (registered != null) {
                    // Discard any fallback assignments made while Registry was unavailable.
                    // Rebuild active mappings from the latest schema and reserve every number
                    // that appeared in this subject's history, including deleted fields.
                    Map<String, Set<Integer>> reserved = reservedFieldNumbersBySubject.computeIfAbsent(subject,
                            key -> new ConcurrentHashMap<>());
                    synchronized (numbers) {
                        numbers.clear();
                        reserved.clear();
                        loadHistoricalFieldNumbers(subject, reserved);
                        for (FlattenedField flattened : flattenDescriptorFields(registered)) {
                            numbers.put(flattened.path, flattened.field.getNumber());
                            reserveFieldNumber(reserved, flattened.path, flattened.field.getNumber());
                        }
                    }
                    rememberWireDescriptor(subject, registered);
                    // A descriptor may have been built while Registry was unavailable.
                    descriptorCache.remove(tableId);
                    descriptorSchemaKeys.remove(tableId);
                }
                // Null means the subject is absent or uses a non-Protobuf schema.
                // Transient lookup failures throw and leave this unset for a retry.
                fieldNumbersInitialized.add(subject);
            } catch (Exception e) {
                tapLogger.warn("Failed to initialize Protobuf field-number mapping for subject '{}'; will retry: {}",
                        subject, e.getMessage());
                throw new IllegalStateException("Cannot safely build Protobuf descriptor while Registry state for subject '"
                        + subject + "' is unavailable", e);
            }
        }
    }

    private String sanitizePath(String path) {
        return Arrays.stream(path.split("\\."))
                .map(this::sanitizeFieldName)
                .collect(Collectors.joining("."));
    }

    private Descriptors.Descriptor latestRegisteredDescriptor(String subject) throws Exception {
        SchemaMetadata metadata;
        try {
            metadata = kafkaService.getSchemaRegistryClient().getLatestSchemaMetadata(subject);
        } catch (RestClientException e) {
            if (e.getStatus() == 404 || e.getErrorCode() == 40401) return null;
            throw e;
        }
        if (metadata == null) return null;
        if (!ProtobufSchema.TYPE.equalsIgnoreCase(metadata.getSchemaType())) {
            throw new IllegalStateException("Registry subject " + subject + " is not a Protobuf schema");
        }
        ParsedSchema schema = kafkaService.getSchemaRegistryClient()
                .getSchemaBySubjectAndId(subject, metadata.getId());
        if (!(schema instanceof ProtobufSchema)) {
            throw new IllegalStateException("Registry did not return a Protobuf schema for subject " + subject);
        }
        return ((ProtobufSchema) schema).toDescriptor();
    }

    private int fieldNumberFor(String subject, String path) {
        Map<String, Integer> numbers = fieldNumbersBySubject.computeIfAbsent(subject,
                key -> new ConcurrentHashMap<>());
        Map<String, Set<Integer>> reserved = reservedFieldNumbersBySubject.computeIfAbsent(subject,
                key -> new ConcurrentHashMap<>());
        synchronized (numbers) {
            Integer existing = numbers.get(path);
            if (existing != null) return existing;
            String scope = parentPath(path);
            int max = 0;
            for (Map.Entry<String, Integer> entry : numbers.entrySet()) {
                if (Objects.equals(parentPath(entry.getKey()), scope)) {
                    max = Math.max(max, entry.getValue());
                }
            }
            for (Integer reservedNumber : reserved.getOrDefault(scope, Collections.emptySet())) {
                max = Math.max(max, reservedNumber);
            }
            int next = max + 1;
            if (next >= 19000 && next <= 19999) next = 20000;
            if (next > 536870911) {
                throw new IllegalStateException("Protobuf field number limit exceeded in " + scope);
            }
            numbers.put(path, next);
            reserveFieldNumber(reserved, path, next);
            return next;
        }
    }

    private void loadHistoricalFieldNumbers(String subject, Map<String, Set<Integer>> reserved) throws Exception {
        List<Integer> versions = kafkaService.getSchemaRegistryClient().getAllVersions(subject);
        if (versions == null || versions.isEmpty()) {
            throw new IllegalStateException("Registry returned no schema history for subject " + subject);
        }
        for (Integer version : versions) {
            if (version == null) continue;
            SchemaMetadata metadata = kafkaService.getSchemaRegistryClient().getSchemaMetadata(subject, version);
            if (metadata == null || !ProtobufSchema.TYPE.equalsIgnoreCase(metadata.getSchemaType())) continue;
            ParsedSchema schema = kafkaService.getSchemaRegistryClient()
                    .getSchemaBySubjectAndId(subject, metadata.getId());
            if (!(schema instanceof ProtobufSchema)) {
                throw new IllegalStateException("Registry returned no Protobuf schema for subject " + subject
                        + " version " + version);
            }
            Descriptors.Descriptor historical = ((ProtobufSchema) schema).toDescriptor();
            // Older versions contribute reserved field numbers only. They can
            // contain numbering drift from schemas produced before stable field
            // numbers were introduced, so comparing them by path would reject
            // otherwise valid upgrades. Wire compatibility is checked against
            // the latest registered descriptor and descriptors built this run.
            for (FlattenedField flattened : flattenDescriptorFields(historical)) {
                reserveFieldNumber(reserved, flattened.path, flattened.field.getNumber());
            }
        }
    }

    private void reserveFieldNumber(Map<String, Set<Integer>> reserved, String path, int number) {
        reserved.computeIfAbsent(parentPath(path), key -> ConcurrentHashMap.newKeySet()).add(number);
    }

    private void validateAndRememberWireDescriptor(String subject, Descriptors.Descriptor current) {
        List<Descriptors.Descriptor> knownDescriptors = wireHistoryBySubject.computeIfAbsent(subject,
                key -> Collections.synchronizedList(new ArrayList<>()));
        synchronized (knownDescriptors) {
            for (Descriptors.Descriptor registered : knownDescriptors) {
                validateWireCompatibility(registered, current);
            }
            knownDescriptors.add(current);
        }
    }

    private void validateWireCompatibility(Descriptors.Descriptor registered, Descriptors.Descriptor current) {
        Map<String, Descriptors.FieldDescriptor> oldFields = descriptorFieldsByPath(registered);
        Map<String, Descriptors.FieldDescriptor> newFields = descriptorFieldsByPath(current);
        Map<String, Descriptors.FieldDescriptor> oldFieldsByScopeAndNumber = new HashMap<>();
        for (Map.Entry<String, Descriptors.FieldDescriptor> entry : oldFields.entrySet()) {
            oldFieldsByScopeAndNumber.put(parentPath(entry.getKey()) + "#" + entry.getValue().getNumber(),
                    entry.getValue());
        }
        for (Map.Entry<String, Descriptors.FieldDescriptor> entry : newFields.entrySet()) {
            Descriptors.FieldDescriptor oldField = oldFields.get(entry.getKey());
            Descriptors.FieldDescriptor newField = entry.getValue();
            if (oldField == null) {
                oldField = oldFieldsByScopeAndNumber.get(parentPath(entry.getKey()) + "#" + newField.getNumber());
            }
            if (oldField == null) continue;
            boolean changed = oldField.getNumber() != newField.getNumber()
                    || oldField.getType() != newField.getType()
                    || oldField.isRepeated() != newField.isRepeated()
                    || oldField.isRequired() != newField.isRequired()
                    || (oldField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                    && !Objects.equals(oldField.getMessageType().getFullName(),
                    newField.getMessageType().getFullName()));
            if (changed) {
                throw new IllegalArgumentException("Protobuf schema change for existing field '"
                        + entry.getKey() + "' is not wire-compatible with the registered subject; "
                        + "use a new subject or migrate the existing schema first");
            }
            if (!wireFieldCompatible(oldField, newField, new HashSet<>())) {
                throw new IllegalArgumentException("Protobuf nested field or enum values for existing field '"
                        + entry.getKey() + "' changed in the registered subject; "
                        + "use a new subject or migrate the existing schema first");
            }
        }
    }

    private void rememberWireDescriptor(String subject, Descriptors.Descriptor descriptor) {
        List<Descriptors.Descriptor> known = wireHistoryBySubject.computeIfAbsent(subject,
                key -> Collections.synchronizedList(new ArrayList<>()));
        synchronized (known) {
            known.add(descriptor);
        }
    }

    private boolean wireFieldCompatible(Descriptors.FieldDescriptor oldField,
                                        Descriptors.FieldDescriptor newField,
                                        Set<String> comparedMessagePairs) {
        if (oldField == null || newField == null
                || oldField.getType() != newField.getType()
                || oldField.isRepeated() != newField.isRepeated()
                || oldField.isRequired() != newField.isRequired()
                || oldField.isMapField() != newField.isMapField()) {
            return false;
        }
        if (oldField.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
            return enumValuesAreWireCompatible(oldField.getEnumType(), newField.getEnumType());
        }
        if (oldField.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) return true;
        if (oldField.isMapField()) {
            Descriptors.FieldDescriptor oldKey = oldField.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor newKey = newField.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor oldValue = oldField.getMessageType().findFieldByName("value");
            Descriptors.FieldDescriptor newValue = newField.getMessageType().findFieldByName("value");
            return wireFieldCompatible(oldKey, newKey, comparedMessagePairs)
                    && wireFieldCompatible(oldValue, newValue, comparedMessagePairs);
        }
        return messageWireCompatible(oldField.getMessageType(), newField.getMessageType(), comparedMessagePairs);
    }

    private boolean messageWireCompatible(Descriptors.Descriptor oldMessage,
                                          Descriptors.Descriptor newMessage,
                                          Set<String> comparedMessagePairs) {
        if (!Objects.equals(oldMessage.getFullName(), newMessage.getFullName())) return false;
        String pair = oldMessage.getFullName() + "->" + newMessage.getFullName();
        if (!comparedMessagePairs.add(pair)) return true;
        for (Descriptors.FieldDescriptor oldField : oldMessage.getFields()) {
            Descriptors.FieldDescriptor newField = newMessage.findFieldByNumber(oldField.getNumber());
            if (newField != null && !wireFieldCompatible(oldField, newField, comparedMessagePairs)) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Integer> enumValueNumbers(Descriptors.EnumDescriptor enumType) {
        Map<String, Integer> values = new LinkedHashMap<>();
        for (Descriptors.EnumValueDescriptor value : enumType.getValues()) {
            String name = value.getName();
            name = sourceEnumValueName(enumType.getName(), name);
            values.put(name, value.getNumber());
        }
        return values;
    }

    private boolean enumValuesAreWireCompatible(Descriptors.EnumDescriptor oldType,
                                                Descriptors.EnumDescriptor newType) {
        Map<String, Integer> oldValues = enumValueNumbers(oldType);
        Map<String, Integer> newValues = enumValueNumbers(newType);
        for (Map.Entry<String, Integer> oldValue : oldValues.entrySet()) {
            if (!Objects.equals(oldValue.getValue(), newValues.get(oldValue.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 根据 TapTable 构建 Protobuf Descriptor
     */
    private Descriptors.Descriptor buildProtobufDescriptor(TapTable tapTable) throws Descriptors.DescriptorValidationException {
        // Kept as a local descriptor helper for callers that only need the shape.
        // Production sends go through getOrCreateDescriptor with the real output topic.
        return buildProtobufDescriptor(tapTable, tapTable.getId(), false);
    }

    private Descriptors.Descriptor buildProtobufDescriptor(TapTable tapTable, String outputTopic)
            throws Descriptors.DescriptorValidationException {
        return buildProtobufDescriptor(tapTable, outputTopic, true);
    }

    private Descriptors.Descriptor buildProtobufDescriptor(TapTable tapTable,
                                                            String outputTopic,
                                                            boolean loadRegistryState)
            throws Descriptors.DescriptorValidationException {
        String messageName = sanitizeMessageName(tapTable.getId());
        String subject = outputTopic + "-value";
        if (loadRegistryState) initializeFieldNumbers(tapTable, subject);

        DescriptorProtos.DescriptorProto.Builder messageBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName(messageName);

        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (nameFieldMap != null && nameFieldMap.keySet().stream().anyMatch(name -> name.contains("."))) {
            return buildNestedProtobufDescriptor(tapTable, messageName, subject);
        }
        if (nameFieldMap != null && !nameFieldMap.isEmpty()) {
            validateUniqueFieldNames(nameFieldMap.keySet());
            // A Registry path mapping wins. New subjects use deterministic ordering;
            // new paths on an existing subject are appended by fieldNumberFor.
            List<Map.Entry<String, TapField>> entries = new ArrayList<>(nameFieldMap.entrySet());
            entries.sort(Map.Entry.comparingByKey());
            for (Map.Entry<String, TapField> entry : entries) {
                String fieldName = sanitizeFieldName(entry.getKey());
                TapField tapField = entry.getValue();
                String dataType = tapField.getDataType();

                if (StringUtils.isBlank(dataType)) {
                    continue;
                }

                DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName(fieldName)
                        .setNumber(fieldNumberFor(subject, fieldName))
                        .setLabel(labelForTapType(dataType))
                        .setType(mapTapTypeToProtobufType(dataType));
                String normalizedType = normalizeTapType(dataType);
                String repeatedScalarType = repeatedScalarType(tapField);
                String repeatedEnumType = repeatedEnumType(tapField);
                String enumType = enumType(tapField);
                String wireType = wireType(tapField);
                String explicitMessageType = protobufMessageType(tapField);
                if (repeatedScalarType != null) {
                    fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                            .setType(protobufTypeFromName(repeatedScalarType));
                } else if (repeatedEnumType != null) {
                    fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                            .setTypeName("." + messageName + "." + addEnumType(messageBuilder, fieldName, repeatedEnumType));
                } else if (enumType != null) {
                    fieldBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                            .setTypeName("." + messageName + "." + addEnumType(messageBuilder, fieldName, enumType));
                } else if (wireType != null) {
                    fieldBuilder.setType(protobufTypeFromName(wireType));
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
                    messageBuilder.addNestedType(buildMapEntryDescriptor(messageBuilder, messageName,
                            entryName, tapField));
                    fieldBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName("." + messageName + "." + entryName);
                }
                if (EmptyKit.isNotNull(tapField.getDefaultValue()) && applyDefault
                        && canApplyProtobufDefault(tapField, normalizedType)) {
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

        Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(messageName);
        validateAndRememberWireDescriptor(subject, descriptor);
        return descriptor;
    }

    private Descriptors.Descriptor buildNestedProtobufDescriptor(TapTable tapTable,
                                                                 String messageName,
                                                                 String subject)
            throws Descriptors.DescriptorValidationException {
        DescriptorTreeNode root = descriptorTree(tapTable);
        DescriptorProtos.DescriptorProto rootProto = buildNestedMessageProto(root, messageName, messageName,
                subject, "");
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
        Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(messageName);
        validateAndRememberWireDescriptor(subject, descriptor);
        return descriptor;
    }

    private DescriptorTreeNode descriptorTree(TapTable tapTable) {
        DescriptorTreeNode root = new DescriptorTreeNode("", null);
        for (Map.Entry<String, TapField> entry : tapTable.getNameFieldMap().entrySet()) {
            DescriptorTreeNode current = root;
            String[] segments = entry.getKey().split("\\.");
            for (String segment : segments) {
                current = current.children.computeIfAbsent(segment, key -> new DescriptorTreeNode(key, null));
            }
            if (current.field != null) {
                throw new IllegalArgumentException("Duplicate Protobuf field path: " + entry.getKey());
            }
            current.field = entry.getValue();
        }
        validateDescriptorTree(root, "");
        return root;
    }

    private void validateDescriptorTree(DescriptorTreeNode node, String path) {
        if (node.field != null && !node.children.isEmpty()) {
            String type = normalizeTapType(node.field.getDataType());
            if (!"OBJECT".equals(type) && !"ARRAY".equals(type)) {
                throw new IllegalArgumentException("Protobuf field path has children but is not a container: "
                        + (path.isEmpty() ? node.name : path + "." + node.name));
            }
        }
        for (DescriptorTreeNode child : node.children.values()) {
            validateDescriptorTree(child, path.isEmpty() ? child.name : path + "." + child.name);
        }
    }

    private DescriptorProtos.DescriptorProto buildNestedMessageProto(DescriptorTreeNode node,
                                                                     String messageName,
                                                                     String fullName,
                                                                     String subject,
                                                                     String parentPath) {
        DescriptorProtos.DescriptorProto.Builder message = DescriptorProtos.DescriptorProto.newBuilder()
                .setName(messageName);
        Set<String> sanitizedNames = new HashSet<>();
        List<DescriptorTreeNode> children = new ArrayList<>(node.children.values());
        children.sort(Comparator.comparing(child -> child.name));
        for (DescriptorTreeNode child : children) {
            TapField tapField = child.field;
            String normalizedType = tapField == null ? "OBJECT" : normalizeTapType(tapField.getDataType());
            String fieldName = sanitizeFieldName(child.name);
            String fieldPath = parentPath.isEmpty() ? fieldName : parentPath + "." + fieldName;
            if (!sanitizedNames.add(fieldName)) {
                throw new IllegalArgumentException("Protobuf field name collision after sanitization: " + child.name);
            }
            DescriptorProtos.FieldDescriptorProto.Builder field = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName(fieldName)
                    .setNumber(fieldNumberFor(subject, fieldPath))
                    .setLabel(labelForTapType(normalizedType));

            String repeatedScalarType = repeatedScalarType(tapField);
            String repeatedEnumType = repeatedEnumType(tapField);
            String enumType = enumType(tapField);
            String wireType = wireType(tapField);
            String explicitMessageType = protobufMessageType(tapField);
            if (!child.children.isEmpty() && ("OBJECT".equals(normalizedType) || "ARRAY".equals(normalizedType))) {
                String nestedName = sanitizeMessageName(child.name + "Message");
                String nestedFullName = fullName + "." + nestedName;
                message.addNestedType(buildNestedMessageProto(child, nestedName, nestedFullName,
                        subject, fieldPath));
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("." + nestedFullName);
            } else if (repeatedScalarType != null) {
                field.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setType(protobufTypeFromName(repeatedScalarType));
            } else if (repeatedEnumType != null) {
                field.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                        .setTypeName("." + fullName + "." + addEnumType(message, fieldName, repeatedEnumType));
            } else if (enumType != null) {
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                        .setTypeName("." + fullName + "." + addEnumType(message, fieldName, enumType));
            } else if (wireType != null) {
                field.setType(protobufTypeFromName(wireType));
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
                message.addNestedType(buildMapEntryDescriptor(message, fullName,
                        entryName, tapField));
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("." + fullName + "." + entryName);
            } else {
                field.setType(mapTapTypeToProtobufType(normalizedType));
            }
            if (tapField != null && EmptyKit.isNotNull(tapField.getDefaultValue()) && applyDefault
                    && canApplyProtobufDefault(tapField, normalizedType)) {
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

    private String encodedEnumValueName(String enumName, String sourceName) {
        return ENCODED_ENUM_VALUE_PREFIX + enumName + "__" + sourceName;
    }

    private String sourceEnumValueName(String enumName, String descriptorValueName) {
        String prefix = ENCODED_ENUM_VALUE_PREFIX + enumName + "__";
        return descriptorValueName.startsWith(prefix)
                ? descriptorValueName.substring(prefix.length()) : descriptorValueName;
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

    private void validateUniqueFieldNames(Collection<String> fieldNames) {
        Map<String, String> originalsBySanitizedName = new HashMap<>();
        for (String fieldName : fieldNames) {
            String sanitized = sanitizeFieldName(fieldName);
            String previous = originalsBySanitizedName.putIfAbsent(sanitized, fieldName);
            if (previous != null && !Objects.equals(previous, fieldName)) {
                throw new IllegalArgumentException("Protobuf field name collision after sanitization: "
                        + previous + " and " + fieldName);
            }
        }
    }

    private String addEnumType(DescriptorProtos.DescriptorProto.Builder message,
                               String fieldName,
                               String signature) {
        String[] parts = StringUtils.defaultString(signature).split("\\|", 2);
        String enumBaseName = parts.length == 0 ? fieldName : parts[0];
        int lastDot = enumBaseName.lastIndexOf('.');
        enumBaseName = lastDot < 0 ? enumBaseName : enumBaseName.substring(lastDot + 1);
        String baseEnumName = sanitizeMessageName(enumBaseName + "Enum");
        String enumName = baseEnumName;
        int collision = 0;
        while (true) {
            DescriptorProtos.EnumDescriptorProto existing = null;
            for (DescriptorProtos.EnumDescriptorProto candidate : message.getEnumTypeList()) {
                if (candidate.getName().equals(enumName)) {
                    existing = candidate;
                    break;
                }
            }
            if (existing == null) {
                break;
            }
            if (enumSignatureMatches(existing, signature)) {
                return enumName;
            }
            enumName = sanitizeMessageName(baseEnumName + "_"
                    + Integer.toUnsignedString(signature.hashCode()) + "_" + ++collision);
        }
        DescriptorProtos.EnumDescriptorProto.Builder enumBuilder = DescriptorProtos.EnumDescriptorProto.newBuilder()
                .setName(enumName);
        if (parts.length == 2) {
            List<String[]> values = new ArrayList<>();
            for (String value : parts[1].split(",")) {
                if (StringUtils.isBlank(value)) {
                    continue;
                }
                String[] valueParts = value.split("=", 2);
                if (valueParts.length == 2) {
                    values.add(valueParts);
                }
            }
            boolean hasZero = values.stream().anyMatch(value -> Integer.parseInt(value[1]) == 0);
            if (hasZero) {
                values.sort(Comparator.comparingInt(value -> Integer.parseInt(value[1]) == 0 ? 0 : 1));
            } else {
                String syntheticZero = SYNTHETIC_ENUM_ZERO_PREFIX;
                boolean nameInUse;
                do {
                    nameInUse = false;
                    for (String[] value : values) {
                        if (value[0].equals(syntheticZero)) {
                            nameInUse = true;
                            break;
                        }
                    }
                    if (!nameInUse) break;
                    syntheticZero += "_";
                } while (true);
                values.add(0, new String[]{syntheticZero, "0"});
            }
            Set<String> usedValueNames = new HashSet<>();
            for (DescriptorProtos.EnumDescriptorProto existing : message.getEnumTypeList()) {
                for (DescriptorProtos.EnumValueDescriptorProto existingValue : existing.getValueList()) {
                    usedValueNames.add(existingValue.getName());
                }
            }
            boolean prefixValues = values.stream().anyMatch(value -> usedValueNames.contains(value[0])
                    || value[0].startsWith(GENERATED_ENUM_VALUE_PREFIX));
            Set<Integer> numbers = new HashSet<>();
            for (String[] value : values) {
                int number = Integer.parseInt(value[1]);
                if (!numbers.add(number)) {
                    enumBuilder.setOptions(DescriptorProtos.EnumOptions.newBuilder().setAllowAlias(true));
                }
                enumBuilder.addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                        .setName(prefixValues ? encodedEnumValueName(enumName, value[0]) : value[0])
                        .setNumber(number));
            }
        }
        message.addEnumType(enumBuilder.build());
        return enumName;
    }

    private boolean enumSignatureMatches(DescriptorProtos.EnumDescriptorProto existing, String signature) {
        String[] parts = StringUtils.defaultString(signature).split("\\|", 2);
        if (parts.length != 2) {
            return false;
        }
        String[] values = parts[1].split(",");
        boolean hasSourceZero = Arrays.stream(values).anyMatch(value -> {
            String[] partsForValue = value.split("=", 2);
            return partsForValue.length == 2 && "0".equals(partsForValue[1]);
        });
        int syntheticZeroCount = !hasSourceZero && existing.getValueCount() > 0
                && existing.getValue(0).getName().startsWith(SYNTHETIC_ENUM_ZERO_PREFIX)
                && existing.getValue(0).getNumber() == 0 ? 1 : 0;
        if (existing.getValueCount() - syntheticZeroCount != values.length) {
            return false;
        }
        for (int i = 0; i < values.length; i++) {
            String[] valueParts = values[i].split("=", 2);
            if (valueParts.length != 2
                    || !(existing.getValue(i + syntheticZeroCount).getName().equals(valueParts[0])
                    || existing.getValue(i + syntheticZeroCount).getName()
                    .equals(encodedEnumValueName(existing.getName(), valueParts[0])))
                    || existing.getValue(i + syntheticZeroCount).getNumber() != Integer.parseInt(valueParts[1])) {
                return false;
            }
        }
        return true;
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

    private boolean canApplyProtobufDefault(TapField tapField, String normalizedType) {
        if ("ARRAY".equals(normalizedType) || "MAP".equals(normalizedType)
                || "OBJECT".equals(normalizedType)) {
            return false;
        }
        return StringUtils.isBlank(protobufMessageType(tapField));
    }

    private void setProtobufMessageType(TapField tapField, Descriptors.FieldDescriptor field) {
        if (field.isMapField()) {
            Descriptors.Descriptor entry = field.getMessageType();
            tapField.setPureDataType(MAP_TYPE_PREFIX
                    + mapTypeToken(entry.findFieldByName("key")) + MAP_COMPONENT_SEPARATOR
                    + mapTypeToken(entry.findFieldByName("value")));
        } else if (field.isRepeated()) {
            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
                tapField.setPureDataType(REPEATED_ENUM_PREFIX + enumSignature(field.getEnumType()));
            } else if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                tapField.setPureDataType(REPEATED_SCALAR_PREFIX + field.getType().name());
            } else if (isWellKnownMessage(field.getMessageType())) {
                tapField.setPureDataType(field.getMessageType().getFullName() + "[]");
            }
        } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                && isWellKnownMessage(field.getMessageType())) {
            tapField.setPureDataType(field.getMessageType().getFullName());
        } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
            tapField.setPureDataType(ENUM_PREFIX + enumSignature(field.getEnumType()));
        } else if (isNonStandardWireType(field.getType())) {
            tapField.setPureDataType(WIRE_TYPE_PREFIX + field.getType().name());
        }
    }

    private String enumSignature(Descriptors.EnumDescriptor enumType) {
        StringBuilder signature = new StringBuilder(enumType.getFullName()).append('|');
        for (Descriptors.EnumValueDescriptor value : enumType.getValues()) {
            if (signature.charAt(signature.length() - 1) != '|') {
                signature.append(',');
            }
            signature.append(value.getName()).append('=').append(value.getNumber());
        }
        return signature.toString();
    }

    private boolean isNonStandardWireType(Descriptors.FieldDescriptor.Type type) {
        switch (type) {
            case SINT32:
            case SINT64:
            case UINT32:
            case UINT64:
            case FIXED32:
            case FIXED64:
            case SFIXED32:
            case SFIXED64:
                return true;
            default:
                return false;
        }
    }

    private String mapTypeToken(Descriptors.FieldDescriptor field) {
        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return "MESSAGE:" + field.getMessageType().getFullName();
        }
        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
            return "ENUM:" + enumSignature(field.getEnumType());
        }
        return "SCALAR:" + field.getType().name();
    }

    private String mapTypeToken(TapField tapField, int index) {
        String pureType = tapField == null ? null : tapField.getPureDataType();
        if (pureType == null || !pureType.startsWith(MAP_TYPE_PREFIX)) {
            return null;
        }
        String encoded = pureType.substring(MAP_TYPE_PREFIX.length());
        String[] types = encoded.contains(MAP_COMPONENT_SEPARATOR)
                ? encoded.split(MAP_COMPONENT_SEPARATOR, -1)
                : encoded.split("\\|", 2);
        return types.length > index ? types[index] : null;
    }

    private DescriptorProtos.DescriptorProto buildMapEntryDescriptor(DescriptorProtos.DescriptorProto.Builder enumOwner,
                                                                      String ownerFullName,
                                                                      String entryName,
                                                                      TapField tapField) {
        String keyType = mapTypeToken(tapField, 0);
        String valueType = mapTypeToken(tapField, 1);
        if (keyType == null) {
            keyType = "SCALAR:STRING";
        }
        if (valueType == null) {
            valueType = "MESSAGE:google.protobuf.Value";
        }
        DescriptorProtos.DescriptorProto.Builder entry = DescriptorProtos.DescriptorProto.newBuilder()
                .setName(entryName)
                .setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true).build());
        entry.addField(mapEntryField(enumOwner, ownerFullName, "key", 1, keyType));
        entry.addField(mapEntryField(enumOwner, ownerFullName, "value", 2, valueType));
        return entry.build();
    }

    private DescriptorProtos.FieldDescriptorProto mapEntryField(DescriptorProtos.DescriptorProto.Builder enumOwner,
                                                                String ownerFullName,
                                                                String name,
                                                                int number,
                                                                String token) {
        DescriptorProtos.FieldDescriptorProto.Builder field = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        if (token.startsWith("SCALAR:")) {
            field.setType(protobufTypeFromName(token.substring("SCALAR:".length())));
        } else if (token.startsWith("ENUM:")) {
            String enumName = addEnumType(enumOwner, name, token.substring("ENUM:".length()));
            field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                    .setTypeName("." + ownerFullName + "." + enumName);
        } else if (token.startsWith("MESSAGE:")) {
            String messageType = token.substring("MESSAGE:".length());
            if (isWellKnownMessageName(messageType) && wellKnownDependency(messageType) != null) {
                field.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("." + messageType);
            } else {
                throw new IllegalArgumentException("Cannot rebuild custom map value message type "
                        + messageType + " from TapField metadata");
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

    private String enumType(TapField tapField) {
        String pureType = tapField == null ? null : tapField.getPureDataType();
        return pureType != null && pureType.startsWith(ENUM_PREFIX)
                ? pureType.substring(ENUM_PREFIX.length()) : null;
    }

    private String wireType(TapField tapField) {
        String pureType = tapField == null ? null : tapField.getPureDataType();
        return pureType != null && pureType.startsWith(WIRE_TYPE_PREFIX)
                ? pureType.substring(WIRE_TYPE_PREFIX.length()) : null;
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
            case "google.protobuf.Timestamp":
            case "google.protobuf.DoubleValue":
            case "google.protobuf.FloatValue":
            case "google.protobuf.Int32Value":
            case "google.protobuf.Int64Value":
            case "google.protobuf.UInt32Value":
            case "google.protobuf.UInt64Value":
            case "google.protobuf.BoolValue":
            case "google.protobuf.StringValue":
            case "google.protobuf.BytesValue":
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
                return Api.getDescriptor().getFile();
            case "google.protobuf.Option":
            case "google.protobuf.Type":
                return Type.getDescriptor().getFile();
            case "google.protobuf.DescriptorProto":
                return DescriptorProtos.getDescriptor();
            case "google.protobuf.Enum":
            case "google.protobuf.EnumValue":
                return Type.getDescriptor().getFile();
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
            case "google.protobuf.Timestamp":
                return Timestamp.getDescriptor().getFile();
            case "google.protobuf.DoubleValue":
            case "google.protobuf.FloatValue":
            case "google.protobuf.Int32Value":
            case "google.protobuf.Int64Value":
            case "google.protobuf.UInt32Value":
            case "google.protobuf.UInt64Value":
            case "google.protobuf.BoolValue":
            case "google.protobuf.StringValue":
            case "google.protobuf.BytesValue":
                return DoubleValue.getDescriptor().getFile();
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
                    // Protobuf repeated fields cannot represent null elements.
                    // Omit nulls instead of failing the whole record.
                    if (item != null) {
                        converted.add(convertRepeatedValue(item, fieldDescriptor));
                    }
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
                    return parseIntValue(value);

                case FIXED32:
                    return value instanceof Integer ? value : parseUnsignedIntValue(value);

                case UINT32:
                    if (value instanceof Integer) {
                        return value;
                    }
                    return parseUnsignedIntValue(value);

                case INT64:
                case SINT64:
                case SFIXED64:
                    if (value instanceof Long) {
                        return value;
                    }
                    return parseLongValue(value);

                case FIXED64:
                    return value instanceof Long ? value : parseUnsignedLongValue(value);

                case UINT64:
                    if (value instanceof Long) {
                        return value;
                    }
                    return parseUnsignedLongValue(value);

                case FLOAT:
                    if (value instanceof Float) {
                        return value;
                    }
                    return value instanceof Number ? ((Number) value).floatValue()
                            : Float.parseFloat(value.toString());

                case DOUBLE:
                    if (value instanceof Double) {
                        return value;
                    }
                    return value instanceof Number ? ((Number) value).doubleValue()
                            : Double.parseDouble(value.toString());

                case STRING:
                    return value.toString();

                case BYTES:
                    if (value instanceof byte[]) {
                        return com.google.protobuf.ByteString.copyFrom((byte[]) value);
                    }
                    return com.google.protobuf.ByteString.copyFromUtf8(value.toString());

                case ENUM:
                    if (value instanceof Descriptors.EnumValueDescriptor) {
                        return value;
                    }
                    Descriptors.EnumValueDescriptor enumValue = findEnumValue(fieldDescriptor, value);
                    if (enumValue == null) {
                        throw new IllegalArgumentException("Unknown enum value " + value
                                + " for " + fieldDescriptor.getFullName());
                    }
                    return enumValue;

                case MESSAGE:
                    if (isWrapperMessageName(fieldDescriptor.getMessageType().getFullName())) {
                        return toWrapperMessage(value, fieldDescriptor.getMessageType());
                    }
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
        if (!isWellKnownMessage(fieldDescriptor.getMessageType())) {
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
        if (isWrapperMessageName(fieldDescriptor.getMessageType().getFullName())) {
            return toWrapperMessage(value, fieldDescriptor.getMessageType());
        }
        DynamicMessage nested = toDynamicMessage(value, fieldDescriptor.getMessageType());
        if (nested == null) {
            throw new IllegalArgumentException("Expected a map or protobuf message for repeated field "
                    + fieldDescriptor.getFullName());
        }
        return nested;
    }

    private DynamicMessage toWrapperMessage(Object value, Descriptors.Descriptor descriptor) {
        if (value instanceof Message && descriptor.equals(((Message) value).getDescriptorForType())) {
            return DynamicMessage.newBuilder(descriptor).mergeFrom((Message) value).build();
        }
        Descriptors.FieldDescriptor valueField = descriptor.getFields().get(0);
        DynamicMessage.Builder wrapper = DynamicMessage.newBuilder(descriptor);
        Object converted = convertValue(value, valueField);
        if (converted != null) {
            wrapper.setField(valueField, converted);
        }
        return wrapper.build();
    }

    private Object convertRepeatedScalarValue(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (value == null) {
            return null;
        }
        switch (fieldDescriptor.getType()) {
            case BOOL:
                return value instanceof Boolean ? value : Boolean.parseBoolean(value.toString());
            case INT32:
            case SINT32:
            case SFIXED32:
                return value instanceof Integer ? value : parseIntValue(value);
            case FIXED32:
                return value instanceof Integer ? value : parseUnsignedIntValue(value);
            case UINT32:
                return value instanceof Integer ? value : parseUnsignedIntValue(value);
            case INT64:
            case SINT64:
            case SFIXED64:
                return value instanceof Long ? value : parseLongValue(value);
            case FIXED64:
                return value instanceof Long ? value : parseUnsignedLongValue(value);
            case UINT64:
                return value instanceof Long ? value : parseUnsignedLongValue(value);
            case FLOAT:
                return value instanceof Float ? value : value instanceof Number
                        ? ((Number) value).floatValue() : Float.parseFloat(value.toString());
            case DOUBLE:
                return value instanceof Double ? value : value instanceof Number
                        ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
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
                Descriptors.EnumValueDescriptor enumValue = findEnumValue(fieldDescriptor, value);
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

    private int parseIntValue(Object value) {
        return new BigDecimal(value.toString()).intValueExact();
    }

    private long parseLongValue(Object value) {
        return new BigDecimal(value.toString()).longValueExact();
    }

    private int parseUnsignedIntValue(Object value) {
        BigInteger number = new BigDecimal(value.toString()).toBigIntegerExact();
        if (number.signum() < 0 || number.compareTo(new BigInteger("4294967295")) > 0) {
            throw new NumberFormatException("Value out of range for uint32: " + value);
        }
        return number.intValue();
    }

    private long parseUnsignedLongValue(Object value) {
        BigInteger number = new BigDecimal(value.toString()).toBigIntegerExact();
        if (number.signum() < 0 || number.compareTo(new BigInteger("18446744073709551615")) > 0) {
            throw new NumberFormatException("Value out of range for uint64: " + value);
        }
        return number.longValue();
    }

    private Descriptors.EnumValueDescriptor findEnumValue(Descriptors.FieldDescriptor field, Object value) {
        if (value instanceof Number) {
            return field.getEnumType().findValueByNumber(new BigDecimal(value.toString()).intValueExact());
        }
        String name = value.toString();
        Descriptors.EnumValueDescriptor exact = field.getEnumType().findValueByName(name);
        if (exact != null) {
            return exact;
        }
        Descriptors.EnumValueDescriptor match = null;
        for (Descriptors.EnumValueDescriptor candidate : field.getEnumType().getValues()) {
            if (candidate.getName().equals(encodedEnumValueName(field.getEnumType().getName(), name))) {
                if (match != null) {
                    return null;
                }
                match = candidate;
            }
        }
        return match;
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
            if (key != null && !key.contains(".")) {
                normalized.put(key, entry.getValue());
            }
        }
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String key = entry.getKey();
            if (key == null || !key.contains(".")) continue;
            String rootKey = key.substring(0, key.indexOf('.'));
            Object root = normalized.get(rootKey);
            if (root instanceof Map) {
                Map<String, Object> merged = new LinkedHashMap<>((Map<String, Object>) root);
                putDottedValueIfAbsent(merged, key.substring(rootKey.length() + 1), entry.getValue());
                normalized.put(rootKey, merged);
            } else if (root != null) {
                tapLogger.warn("Ignore dotted Protobuf field '{}' because root '{}' is not an object", key, rootKey);
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

    @SuppressWarnings("unchecked")
    private void putDottedValueIfAbsent(Map<String, Object> target, String path, Object value) {
        String[] parts = path.split("\\.");
        Map<String, Object> current = target;
        for (int i = 0; i < parts.length - 1; i++) {
            Object existing = current.get(parts[i]);
            if (existing instanceof Map) {
                Map<String, Object> copy = new LinkedHashMap<>((Map<String, Object>) existing);
                current.put(parts[i], copy);
                current = copy;
            } else if (existing == null) {
                Map<String, Object> nested = new LinkedHashMap<>();
                current.put(parts[i], nested);
                current = nested;
            } else {
                return;
            }
        }
        current.putIfAbsent(parts[parts.length - 1], value);
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
            if (item.getKey() == null) {
                tapLogger.warn("Skip Protobuf map entry with null key: {}", fieldDescriptor.getFullName());
                continue;
            }
            DynamicMessage.Builder entry = DynamicMessage.newBuilder(entryDescriptor);
            entry.setField(keyField, convertMapEntryValue(item.getKey(), keyField));
            if (item.getValue() == null
                    && !(valueField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                    && "google.protobuf.Value".equals(valueField.getMessageType().getFullName()))) {
                tapLogger.warn("Skip Protobuf map entry with null value: {}", fieldDescriptor.getFullName());
                continue;
            }
            entry.setField(valueField, convertMapEntryValue(item.getValue(), valueField));
            entries.add(entry.build());
        }
        return entries;
    }

    private Object convertMapEntryValue(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            if (value == null && "google.protobuf.Value".equals(fieldDescriptor.getMessageType().getFullName())) {
                return toProtobufValue(null);
            }
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
        if (value instanceof byte[]) {
            return builder.setStringValue(Base64.getEncoder().encodeToString((byte[]) value)).build();
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
                if (item instanceof Message) {
                    Message mapEntry = (Message) item;
                    Descriptors.FieldDescriptor entryKey = mapEntry.getDescriptorForType().findFieldByName("key");
                    Descriptors.FieldDescriptor entryValue = mapEntry.getDescriptorForType().findFieldByName("value");
                    result.put(String.valueOf(mapEntry.getField(entryKey)),
                            convertSingleProtobufValue(mapEntry.getField(entryValue), entryValue));
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
                    if (isWrapperMessageName(messageType)) {
                        Descriptors.FieldDescriptor wrapped = message.getDescriptorForType().findFieldByName("value");
                        return convertSingleProtobufValue(message.getField(wrapped), wrapped);
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
                if (value instanceof Message) {
                    Map<String, Object> result = new LinkedHashMap<>();
                    Message generated = (Message) value;
                    if (isWrapperMessageName(generated.getDescriptorForType().getFullName())) {
                        Descriptors.FieldDescriptor wrapped = generated.getDescriptorForType().findFieldByName("value");
                        return convertSingleProtobufValue(generated.getField(wrapped), wrapped);
                    }
                    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : generated.getAllFields().entrySet()) {
                        result.put(entry.getKey().getName(),
                                convertProtobufValueToJava(entry.getValue(), entry.getKey()));
                    }
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
                    Descriptors.EnumValueDescriptor enumValue = (Descriptors.EnumValueDescriptor) value;
                    return sourceEnumValueName(enumValue.getType().getName(), enumValue.getName());
                }
                return value.toString();

            case BOOL:
            case INT32:
            case INT64:
                return value;
            case UINT32:
            case FIXED32:
                return value instanceof Integer ? Integer.toUnsignedLong((Integer) value) : value;
            case UINT64:
            case FIXED64:
                return value instanceof Long ? Long.toUnsignedString((Long) value) : value;
            case SINT32:
            case SINT64:
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
        Object converted = convertSingleProtobufValue(field.getValue(), field.getKey());
        return field.getKey().getType() == Descriptors.FieldDescriptor.Type.DOUBLE
                ? normalizeGoogleValueNumber((Double) converted) : converted;
    }

    private Map<String, Object> convertStructMessage(DynamicMessage message) {
        Descriptors.FieldDescriptor fields = message.getDescriptorForType().findFieldByName("fields");
        return convertProtobufMapToJava(message.getField(fields), fields);
    }

    private List<Object> convertListValueMessage(DynamicMessage message) {
        Descriptors.FieldDescriptor values = message.getDescriptorForType().findFieldByName("values");
        return (List<Object>) convertProtobufValueToJava(message.getField(values), values);
    }

    private boolean isWrapperMessageName(String messageType) {
        switch (StringUtils.defaultString(messageType)) {
            case "google.protobuf.DoubleValue":
            case "google.protobuf.FloatValue":
            case "google.protobuf.Int32Value":
            case "google.protobuf.Int64Value":
            case "google.protobuf.UInt32Value":
            case "google.protobuf.UInt64Value":
            case "google.protobuf.BoolValue":
            case "google.protobuf.StringValue":
            case "google.protobuf.BytesValue":
                return true;
            default:
                return false;
        }
    }

    private Object convertGeneratedValue(Value value) {
        switch (value.getKindCase()) {
            case NULL_VALUE:
                return null;
            case NUMBER_VALUE:
                return normalizeGoogleValueNumber(value.getNumberValue());
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

    private Number normalizeGoogleValueNumber(double number) {
        if (Double.isFinite(number) && number == Math.rint(number)) {
            if (number >= Integer.MIN_VALUE && number <= Integer.MAX_VALUE) {
                return (int) number;
            }
            if (number >= Long.MIN_VALUE && number <= Long.MAX_VALUE) {
                return (long) number;
            }
        }
        return number;
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
                registerRenameWithChildren(events, renames, oldFields, newFields,
                        oldPath, entry.getKey(), topic, referenceTime);
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
                registerRenameWithChildren(events, renames, oldFields, newFields,
                        before, after, topic, referenceTime);
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
        registerBaselineSubtreeRenames(events, renames, oldFields, newFields, topic, referenceTime);

        List<String> remainingDrops = oldFields.keySet().stream()
                .filter(path -> !newFields.containsKey(path))
                .filter(path -> !renames.containsValue(path))
                .collect(Collectors.toList());
        List<String> remainingAdds = newFields.keySet().stream()
                .filter(path -> !oldFields.containsKey(path))
                .filter(path -> !renames.containsKey(path))
                .collect(Collectors.toList());
        if (remainingDrops.size() == 1 && remainingAdds.size() == 1
                && Objects.equals(parentPath(remainingDrops.get(0)), parentPath(remainingAdds.get(0)))) {
            String before = remainingDrops.get(0);
            String after = remainingAdds.get(0);
            String oldType = oldFields.get(before).getDataType();
            String newType = protobufPrimaryTypeName(newFields.get(after));
            if (oldType != null && Objects.equals(StringKit.removeParentheses(oldType), newType)) {
                registerBaselineRenameWithChildren(events, renames, oldFields, newFields,
                        before, after, topic, referenceTime);
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

    private void registerRenameWithChildren(List<TapEvent> events,
                                            Map<String, String> renames,
                                            Map<String, Descriptors.FieldDescriptor> oldFields,
                                            Map<String, Descriptors.FieldDescriptor> newFields,
                                            String before,
                                            String after,
                                            String topic,
                                            long referenceTime) {
        if (renames.containsKey(after) || renames.containsValue(before)) {
            return;
        }
        renames.put(after, before);
        addRenameEvent(events, topic, before, after, referenceTime);
        String prefix = after + ".";
        for (String newPath : newFields.keySet()) {
            if (!newPath.startsWith(prefix)) {
                continue;
            }
            String oldPath = before + newPath.substring(after.length());
            if (oldFields.containsKey(oldPath)
                    && Objects.equals(protobufPrimaryTypeName(oldFields.get(oldPath)),
                    protobufPrimaryTypeName(newFields.get(newPath)))) {
                renames.put(newPath, oldPath);
                addRenameEvent(events, topic, oldPath, newPath, referenceTime);
            }
        }
    }

    private void registerBaselineSubtreeRenames(List<TapEvent> events,
                                                Map<String, String> renames,
                                                Map<String, TapField> oldFields,
                                                Map<String, Descriptors.FieldDescriptor> newFields,
                                                String topic,
                                                long referenceTime) {
        List<String> oldRoots = oldFields.keySet().stream()
                .filter(path -> !path.contains("."))
                .filter(path -> !newFields.containsKey(path))
                .collect(Collectors.toList());
        List<String> newRoots = newFields.keySet().stream()
                .filter(path -> !path.contains("."))
                .filter(path -> !oldFields.containsKey(path))
                .collect(Collectors.toList());
        for (String oldRoot : oldRoots) {
            List<String> candidates = newRoots.stream()
                    .filter(newRoot -> baselineSubtreeMatches(oldRoot, newRoot, oldFields, newFields))
                    .collect(Collectors.toList());
            if (candidates.size() == 1) {
                registerBaselineRenameWithChildren(events, renames, oldFields, newFields,
                        oldRoot, candidates.get(0), topic, referenceTime);
            }
        }
    }

    private boolean baselineSubtreeMatches(String oldRoot,
                                           String newRoot,
                                           Map<String, TapField> oldFields,
                                           Map<String, Descriptors.FieldDescriptor> newFields) {
        if (!Objects.equals(normalizedTapFieldType(oldFields.get(oldRoot)),
                protobufPrimaryTypeName(newFields.get(newRoot)))) {
            return false;
        }
        boolean hasMatchingChild = false;
        for (Map.Entry<String, TapField> entry : oldFields.entrySet()) {
            if (!entry.getKey().startsWith(oldRoot + ".")) {
                continue;
            }
            String newPath = newRoot + entry.getKey().substring(oldRoot.length());
            Descriptors.FieldDescriptor newField = newFields.get(newPath);
            if (newField == null || !Objects.equals(normalizedTapFieldType(entry.getValue()),
                    protobufPrimaryTypeName(newField))) {
                return false;
            }
            hasMatchingChild = true;
        }
        return hasMatchingChild;
    }

    private void registerBaselineRenameWithChildren(List<TapEvent> events,
                                                    Map<String, String> renames,
                                                    Map<String, TapField> oldFields,
                                                    Map<String, Descriptors.FieldDescriptor> newFields,
                                                    String before,
                                                    String after,
                                                    String topic,
                                                    long referenceTime) {
        if (renames.containsKey(after) || renames.containsValue(before)) {
            return;
        }
        renames.put(after, before);
        addRenameEvent(events, topic, before, after, referenceTime);
        String prefix = after + ".";
        for (String newPath : newFields.keySet()) {
            if (!newPath.startsWith(prefix)) {
                continue;
            }
            String oldPath = before + newPath.substring(after.length());
            if (oldFields.containsKey(oldPath)
                    && Objects.equals(normalizedTapFieldType(oldFields.get(oldPath)),
                    protobufPrimaryTypeName(newFields.get(newPath)))) {
                renames.put(newPath, oldPath);
                addRenameEvent(events, topic, oldPath, newPath, referenceTime);
            }
        }
    }

    private TapAlterFieldAttributesEvent diffFieldAttributes(String topic,
                                                             String fieldName,
                                                             Descriptors.FieldDescriptor oldField,
                                                             Descriptors.FieldDescriptor newField,
                                                             long referenceTime) {
        String oldType = protobufFieldSignature(oldField);
        String newType = protobufFieldSignature(newField);
        String oldEventType = protobufPrimaryTypeName(oldField);
        String newEventType = protobufPrimaryTypeName(newField);
        boolean oldNullable = !oldField.isRequired();
        boolean newNullable = !newField.isRequired();
        Object oldDefault = oldField.hasDefaultValue() ? oldField.getDefaultValue() : null;
        Object newDefault = newField.hasDefaultValue() ? newField.getDefaultValue() : null;
        boolean dataTypeChanged = !Objects.equals(oldType, newType);
        boolean nullableChanged = oldNullable != newNullable;
        boolean defaultChanged = !Objects.equals(oldDefault, newDefault);
        boolean representableTypeChange = dataTypeChanged && !Objects.equals(oldEventType, newEventType);
        if (!representableTypeChange && !nullableChanged && !defaultChanged) {
            return null;
        }
        TapAlterFieldAttributesEvent attr = new TapAlterFieldAttributesEvent();
        attr.setTime(System.currentTimeMillis());
        attr.setTableId(topic);
        attr.setReferenceTime(referenceTime);
        attr.fieldName(fieldName);
        if (representableTypeChange) {
            attr.dataType(ValueChange.create(oldEventType, newEventType));
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
        String oldType = normalizedTapFieldType(oldField);
        String newType = protobufPrimaryTypeName(newField);
        String oldPureType = oldField.getPureDataType();
        String newPureType = protobufPureDataType(newField);
        String oldEventType = normalizedTapFieldType(oldField);
        String newEventType = protobufPrimaryTypeName(newField);
        boolean oldNullable = !Boolean.FALSE.equals(oldField.getNullable());
        boolean newNullable = !newField.isRequired();
        Object oldDefault = oldField.getDefaultValue();
        Object newDefault = newField.hasDefaultValue() ? newField.getDefaultValue() : null;
        boolean legacyElementType = StringUtils.isBlank(oldPureType)
                && newField.isRepeated()
                && Objects.equals(oldType, protobufElementPrimaryType(newField));
        boolean dataTypeChanged = !Objects.equals(oldType, newType)
                || (StringUtils.isNotBlank(oldPureType) && !Objects.equals(oldPureType, newPureType));
        if (legacyElementType) {
            dataTypeChanged = false;
        }
        boolean nullableChanged = oldNullable != newNullable;
        boolean defaultChanged = !Objects.equals(oldDefault, newDefault);
        boolean representableTypeChange = dataTypeChanged && !Objects.equals(oldEventType, newEventType);
        if (!representableTypeChange && !nullableChanged && !defaultChanged) {
            return null;
        }
        TapAlterFieldAttributesEvent attr = new TapAlterFieldAttributesEvent();
        attr.setTime(System.currentTimeMillis());
        attr.setTableId(topic);
        attr.setReferenceTime(referenceTime);
        attr.fieldName(fieldName);
        if (representableTypeChange) {
            attr.dataType(ValueChange.create(oldEventType, newEventType));
        }
        if (nullableChanged) {
            attr.nullable(ValueChange.create(oldNullable, newNullable));
        }
        if (defaultChanged) {
            attr.defaultChange(ValueChange.create(oldDefault, newDefault));
        }
        return attr;
    }

    private String normalizedTapFieldType(TapField field) {
        if (field == null) {
            return null;
        }
        String type = StringKit.removeParentheses(StringUtils.defaultString(field.getDataType())).trim().toUpperCase(Locale.ROOT);
        switch (type) {
            case "INT":
            case "INTEGER":
                return "INTEGER";
            case "LONG":
                return "LONG";
            case "BOOL":
            case "BOOLEAN":
                return "BOOLEAN";
            case "BYTE_STRING":
            case "BINARY":
                return "BINARY";
            case "ENUM":
                return "STRING";
            case "MESSAGE":
                return "OBJECT";
            default:
                return type;
        }
    }

    private String protobufPureDataType(Descriptors.FieldDescriptor field) {
        TapField projected = new TapField(field == null ? "field" : field.getName(),
                protobufPrimaryTypeName(field));
        if (field != null) {
            setProtobufMessageType(projected, field);
        }
        return projected.getPureDataType();
    }

    private String protobufElementPrimaryType(Descriptors.FieldDescriptor field) {
        if (field == null) {
            return null;
        }
        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
            return "STRING";
        }
        switch (field.getJavaType()) {
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
                return "OBJECT";
            default:
                return "STRING";
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
                return REPEATED_ENUM_PREFIX + enumSignature(field.getEnumType());
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
        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
            return ENUM_PREFIX + enumSignature(field.getEnumType());
        }
        return field.getType().name();
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
        if (f.getType() == Descriptors.FieldDescriptor.Type.UINT32
                || f.getType() == Descriptors.FieldDescriptor.Type.FIXED32) {
            return "LONG";
        }
        if (f.getType() == Descriptors.FieldDescriptor.Type.UINT64
                || f.getType() == Descriptors.FieldDescriptor.Type.FIXED64) {
            return "STRING";
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
