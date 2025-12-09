package io.tapdata.kafka.schema_mode;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
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
                        primaryKeys.addAll(((Map<String, Object>) TapSimplify.fromJson(record.key())).keySet());
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
            ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic(tapTable, tapEvent), null,
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
                if (null != tapField.getDefaultValue()) {
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
}
