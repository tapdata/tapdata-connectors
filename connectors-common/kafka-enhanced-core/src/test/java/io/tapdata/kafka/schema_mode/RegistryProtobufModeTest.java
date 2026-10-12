package io.tapdata.kafka.schema_mode;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.KafkaConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RegistryProtobufModeTest {

    private RegistryProtobufMode mode;

    @BeforeEach
    void setUp() {
        IKafkaService kafkaService = mock(IKafkaService.class);
        KafkaConfig config = mock(KafkaConfig.class);
        when(kafkaService.getLog()).thenReturn(mock(Log.class));
        when(kafkaService.getConfig()).thenReturn(config);
        when(config.getNodeApplyDefault()).thenReturn(false);
        mode = new RegistryProtobufMode(kafkaService);
    }

    @Test
    void discoveryFlattensNestedMessagesIntoDottedFields() throws Exception {
        DescriptorProtos.DescriptorProto address = nestedMessage("Address", field("street", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        Descriptors.Descriptor root = message("Root", field("address", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".Root.Address"), address);
        DynamicMessage message = DynamicMessage.newBuilder(root).build();
        TapTable table = new TapTable("topic");

        invoke("dynamicMessageToTapTable", new Class[]{TapTable.class, DynamicMessage.class, java.util.List.class}, table, message, Collections.emptyList());

        assertNotNull(table.getNameFieldMap().get("address"));
        assertEquals("OBJECT", table.getNameFieldMap().get("address").getDataType());
        assertEquals("STRING", table.getNameFieldMap().get("address.street").getDataType());
    }

    @Test
    void descriptorUsesRepeatedAndMapEntryTypes() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("items", "ARRAY"));
        table.add(new TapField("attributes", "MAP"));
        table.add(new TapField("payload", "OBJECT"));

        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);

        assertTrue(descriptor.findFieldByName("items").isRepeated());
        assertEquals(Descriptors.FieldDescriptor.JavaType.MESSAGE, descriptor.findFieldByName("items").getJavaType());
        assertTrue(descriptor.findFieldByName("attributes").isMapField());
        assertEquals("google.protobuf.Struct", descriptor.findFieldByName("payload").getMessageType().getFullName());
    }

    @Test
    void mapValuesAreConvertedToMapsInsteadOfEntryLists() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("attributes", "MAP"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor mapField = descriptor.findFieldByName("attributes");

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("region", "cn");
        source.put("retries", 3);
        Object protobufValue = invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class}, source, mapField);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).setField(mapField, protobufValue).build();

        Map<String, Object> converted = invoke("convertDynamicMessageToMap", new Class[]{DynamicMessage.class}, message);
        assertEquals("cn", ((Map<?, ?>) converted.get("attributes")).get("region"));
        assertEquals(3.0d, ((Map<?, ?>) converted.get("attributes")).get("retries"));
    }

    @Test
    void arrayAndObjectValuesRoundTripThroughDynamicMessage() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("items", "ARRAY"));
        table.add(new TapField("payload", "OBJECT"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("enabled", true);
        payload.put("name", "demo");
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("items", Arrays.asList("a", 2));
        source.put("payload", payload);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            Descriptors.FieldDescriptor field = descriptor.findFieldByName(entry.getKey());
            builder.setField(field, invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class}, entry.getValue(), field));
        }

        Map<String, Object> converted = invoke("convertDynamicMessageToMap", new Class[]{DynamicMessage.class}, builder.build());
        assertEquals(Arrays.asList("a", 2.0d), converted.get("items"));
        assertEquals(payload, converted.get("payload"));
    }

    @Test
    void nestedObjectIsSerializedAsItsCustomMessageType() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.street", "STRING"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor address = descriptor.findFieldByName("address");

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("street", "Main Street");
        Object converted = invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class}, source, address);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).setField(address, converted).build();

        DynamicMessage nested = (DynamicMessage) message.getField(address);
        assertEquals("Main Street", nested.getField(nested.getDescriptorForType().findFieldByName("street")));
    }

    @Test
    void dottedKeysAreNormalizedIntoNestedObjectsBeforeSerialization() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.street", "STRING"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("address.street", "Main Street");
        Map<String, Object> normalized = invoke("normalizeDataForDescriptor",
                new Class[]{Map.class, Descriptors.Descriptor.class}, source, descriptor);

        assertEquals(Collections.singletonMap("street", "Main Street"), normalized.get("address"));
    }

    @Test
    void repeatedNestedObjectsAreSerializedAsTheirCustomMessageType() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("items", "ARRAY"));
        table.add(new TapField("items.id", "INTEGER"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor items = descriptor.findFieldByName("items");

        Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", 7);
        Object converted = invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                Collections.singletonList(item), items);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).setField(items, converted).build();

        List<?> nestedItems = (List<?>) message.getField(items);
        assertEquals(1, nestedItems.size());
        assertTrue(nestedItems.get(0) instanceof DynamicMessage);
        DynamicMessage nested = (DynamicMessage) nestedItems.get(0);
        assertEquals(7, nested.getField(nested.getDescriptorForType().findFieldByName("id")));
    }

    @Test
    void wellKnownMessagesRemainLeafFieldsDuringDiscovery() throws Exception {
        Descriptors.Descriptor root = messageWithDependency("Root", Struct.getDescriptor().getFile(),
                field("payload", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        ".google.protobuf.Struct"));
        TapTable table = new TapTable("topic");

        invoke("dynamicMessageToTapTable", new Class[]{TapTable.class, DynamicMessage.class, java.util.List.class},
                table, DynamicMessage.newBuilder(root).build(), Collections.emptyList());

        assertNotNull(table.getNameFieldMap().get("payload"));
        assertNull(table.getNameFieldMap().get("payload.fields"));
    }

    @Test
    void dottedTapFieldsBuildNestedProtobufDescriptor() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.street", "STRING"));
        table.add(new TapField("address.zip", "INTEGER"));

        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor address = descriptor.findFieldByName("address");

        assertEquals(Descriptors.FieldDescriptor.JavaType.MESSAGE, address.getJavaType());
        assertEquals("street", address.getMessageType().findFieldByName("street").getName());
        assertEquals(Descriptors.FieldDescriptor.Type.INT32,
                address.getMessageType().findFieldByName("zip").getType());
    }

    @Test
    void repeatedNestedMessagesAreFlattenedWithElementFields() throws Exception {
        DescriptorProtos.DescriptorProto item = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Item")
                .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
                .addField(field("name", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProtos.FieldDescriptorProto items = field(
                "items", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, ".Root.Item");
        Descriptors.Descriptor root = message("Root", items, item);
        TapTable table = new TapTable("topic");

        invoke("dynamicMessageToTapTable", new Class[]{TapTable.class, DynamicMessage.class, java.util.List.class},
                table, DynamicMessage.newBuilder(root).build(), Collections.emptyList());

        assertEquals("ARRAY", table.getNameFieldMap().get("items").getDataType());
        assertEquals("INTEGER", table.getNameFieldMap().get("items.id").getDataType());
        assertEquals("STRING", table.getNameFieldMap().get("items.name").getDataType());
    }

    @Test
    void restartBaselineDoesNotDropExistingNestedFields() throws Exception {
        DescriptorProtos.DescriptorProto address = nestedMessage("Address",
                field("street", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        Descriptors.Descriptor root = message("Root",
                field("address", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".Root.Address"), address);
        TapTable table = new TapTable("topic");
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.street", "STRING"));

        List<TapEvent> events = invoke("detectSchemaChangesFromTableFlat",
                new Class[]{String.class, TapTable.class, Descriptors.Descriptor.class, long.class},
                "topic", table, root, 1L);

        assertTrue(events.isEmpty(), "unchanged nested fields must not generate restart DDL");
    }

    @Test
    void nestedFieldAdditionProducesDottedNewFieldEvent() throws Exception {
        DescriptorProtos.DescriptorProto address = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Address")
                .addField(field("street", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("city", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        Descriptors.Descriptor root = message("Root",
                field("address", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".Root.Address"), address);
        TapTable table = new TapTable("topic");
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.street", "STRING"));

        List<TapEvent> events = invoke("detectSchemaChangesFromTableFlat",
                new Class[]{String.class, TapTable.class, Descriptors.Descriptor.class, long.class},
                "topic", table, root, 1L);

        assertEquals(1, events.size());
        assertTrue(events.get(0) instanceof TapNewFieldEvent);
        assertEquals("address.city", ((TapNewFieldEvent) events.get(0)).getNewFields().get(0).getName());
    }

    @Test
    void restartBaselineRecognizesSingleNestedFieldRename() throws Exception {
        DescriptorProtos.DescriptorProto address = nestedMessage("Address",
                field("city", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        Descriptors.Descriptor root = message("Root",
                field("address", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".Root.Address"), address);
        TapTable table = new TapTable("topic");
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.street", "STRING"));

        List<TapEvent> events = invoke("detectSchemaChangesFromTableFlat",
                new Class[]{String.class, TapTable.class, Descriptors.Descriptor.class, long.class},
                "topic", table, root, 1L);

        assertEquals(1, events.size());
        assertTrue(events.get(0) instanceof TapAlterFieldNameEvent);
        assertEquals("address.street", ((TapAlterFieldNameEvent) events.get(0)).getNameChange().getBefore());
        assertEquals("address.city", ((TapAlterFieldNameEvent) events.get(0)).getNameChange().getAfter());
    }

    @SuppressWarnings("unchecked")
    private <T> T invoke(String name, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = RegistryProtobufMode.class.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return (T) method.invoke(mode, args);
    }

    private static DescriptorProtos.FieldDescriptorProto field(String name, int number,
                                                                DescriptorProtos.FieldDescriptorProto.Type type) {
        return field(name, number, type, null);
    }

    private static DescriptorProtos.FieldDescriptorProto field(String name, int number,
                                                                DescriptorProtos.FieldDescriptorProto.Type type,
                                                                String typeName) {
        return field(name, number, type,
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, typeName);
    }

    private static DescriptorProtos.FieldDescriptorProto field(String name, int number,
                                                                DescriptorProtos.FieldDescriptorProto.Type type,
                                                                DescriptorProtos.FieldDescriptorProto.Label label,
                                                                String typeName) {
        DescriptorProtos.FieldDescriptorProto.Builder builder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setLabel(label)
                .setType(type);
        if (typeName != null) {
            builder.setTypeName(typeName);
        }
        return builder.build();
    }

    private static Descriptors.Descriptor message(String name, DescriptorProtos.FieldDescriptorProto field,
                                                   DescriptorProtos.DescriptorProto... nested) throws Exception {
        DescriptorProtos.DescriptorProto.Builder builder = DescriptorProtos.DescriptorProto.newBuilder().setName(name).addField(field);
        builder.addAllNestedType(Arrays.asList(nested));
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName(name + ".proto")
                        .setSyntax("proto3")
                        .addMessageType(builder.build())
                        .build(), new Descriptors.FileDescriptor[0]);
        return file.findMessageTypeByName(name);
    }

    private static Descriptors.Descriptor messageWithDependency(String name,
                                                                Descriptors.FileDescriptor dependency,
                                                                DescriptorProtos.FieldDescriptorProto field) throws Exception {
        DescriptorProtos.DescriptorProto root = DescriptorProtos.DescriptorProto.newBuilder()
                .setName(name)
                .addField(field)
                .build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName(name + ".proto")
                        .setSyntax("proto3")
                        .addDependency(dependency.getName())
                        .addMessageType(root)
                        .build(), new Descriptors.FileDescriptor[]{dependency});
        return file.findMessageTypeByName(name);
    }

    private static DescriptorProtos.DescriptorProto nestedMessage(String name,
                                                                   DescriptorProtos.FieldDescriptorProto field) {
        return DescriptorProtos.DescriptorProto.newBuilder().setName(name).addField(field).build();
    }
}
