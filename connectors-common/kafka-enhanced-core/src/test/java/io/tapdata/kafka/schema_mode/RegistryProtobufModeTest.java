package io.tapdata.kafka.schema_mode;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
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
    void declaredFieldsAndNestedValuesComeFromTheOriginalDescriptor() throws Exception {
        DescriptorProtos.DescriptorProto profile = nestedMessage("Profile",
                field("name", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        DescriptorProtos.DescriptorProto item = nestedMessage("Item",
                field("sku", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        DescriptorProtos.DescriptorProto attributesEntry = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("AttributesEntry")
                .setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
                .addField(field("key", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("value", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addNestedType(profile).addNestedType(item).addNestedType(attributesEntry)
                .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("tags", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, null))
                .addField(field("profile", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        ".Root.Profile"))
                .addField(field("items", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, ".Root.Item"))
                .addField(field("attributes", 5, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, ".Root.AttributesEntry"))
                .addField(field("declared_but_unset", 6, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        Descriptors.Descriptor root = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("SourceSchema.proto").setSyntax("proto3")
                        .addMessageType(rootProto).build(), new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Root");
        Descriptors.Descriptor profileType = root.findNestedTypeByName("Profile");
        Descriptors.Descriptor itemType = root.findNestedTypeByName("Item");
        Descriptors.Descriptor mapEntryType = root.findNestedTypeByName("AttributesEntry");
        DynamicMessage message = DynamicMessage.newBuilder(root)
                .setField(root.findFieldByName("id"), "event-1")
                .setField(root.findFieldByName("tags"), Arrays.asList("first", "second"))
                .setField(root.findFieldByName("profile"), DynamicMessage.newBuilder(profileType)
                        .setField(profileType.findFieldByName("name"), "Ada").build())
                .setField(root.findFieldByName("items"), Arrays.asList(
                        DynamicMessage.newBuilder(itemType)
                                .setField(itemType.findFieldByName("sku"), "sku-1").build(),
                        DynamicMessage.newBuilder(itemType).build()))
                .setField(root.findFieldByName("attributes"), Collections.singletonList(
                        DynamicMessage.newBuilder(mapEntryType)
                                .setField(mapEntryType.findFieldByName("key"), "region")
                                .setField(mapEntryType.findFieldByName("value"), "cn").build()))
                .build();

        TapTable table = new TapTable("topic");
        invoke("dynamicMessageToTapTable", new Class[]{TapTable.class, DynamicMessage.class, List.class},
                table, message, Collections.emptyList());
        assertEquals("ARRAY", table.getNameFieldMap().get("tags").getDataType());
        assertEquals("OBJECT", table.getNameFieldMap().get("profile").getDataType());
        assertEquals("STRING", table.getNameFieldMap().get("profile.name").getDataType());
        assertEquals("ARRAY", table.getNameFieldMap().get("items").getDataType());
        assertEquals("STRING", table.getNameFieldMap().get("items.sku").getDataType());
        assertEquals("MAP", table.getNameFieldMap().get("attributes").getDataType());
        assertEquals("STRING", table.getNameFieldMap().get("declared_but_unset").getDataType());

        Map<String, Object> decoded = invoke("convertDynamicMessageToMap",
                new Class[]{DynamicMessage.class}, message);
        assertEquals(Arrays.asList("first", "second"), decoded.get("tags"));
        assertEquals("Ada", ((Map<?, ?>) decoded.get("profile")).get("name"));
        assertEquals(Arrays.asList("sku-1", null), decoded.get("items.sku"));
        assertEquals("cn", ((Map<?, ?>) decoded.get("attributes")).get("region"));
        assertFalse(decoded.containsKey("declared_but_unset"));
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
    void repeatedElementTypeChangeProducesAttributeEvent() throws Exception {
        Descriptors.FileDescriptor oldFile = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("OldRepeated.proto")
                        .setSyntax("proto3")
                        .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                                .setName("Root")
                                .addField(field("tags", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, null))
                                .build())
                        .build(), new Descriptors.FileDescriptor[0]);
        Descriptors.FileDescriptor newFile = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("NewRepeated.proto")
                        .setSyntax("proto3")
                        .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                                .setName("Root")
                                .addField(field("tags", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, null))
                                .build())
                        .build(), new Descriptors.FileDescriptor[0]);

        List<TapEvent> events = invoke("detectSchemaChangesFlat",
                new Class[]{String.class, Descriptors.Descriptor.class, Descriptors.Descriptor.class, long.class},
                "topic", oldFile.findMessageTypeByName("Root"), newFile.findMessageTypeByName("Root"), 1L);
        assertEquals(1, events.size());
        assertTrue(events.get(0) instanceof TapAlterFieldAttributesEvent);
        TapAlterFieldAttributesEvent attribute = (TapAlterFieldAttributesEvent) events.get(0);
        assertEquals("ARRAY<STRING>", attribute.getDataTypeChange().getBefore());
        assertEquals("ARRAY<INTEGER>", attribute.getDataTypeChange().getAfter());
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
