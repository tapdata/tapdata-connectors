package io.tapdata.kafka.schema_mode;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Any;
import com.google.protobuf.Duration;
import com.google.protobuf.Struct;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.KafkaConfig;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RegistryProtobufModeTest {

    private RegistryProtobufMode mode;
    private IKafkaService kafkaService;

    @BeforeEach
    void setUp() {
        kafkaService = mock(IKafkaService.class);
        KafkaConfig config = mock(KafkaConfig.class);
        when(kafkaService.getLog()).thenReturn(mock(Log.class));
        when(kafkaService.getConfig()).thenReturn(config);
        when(config.getNodeApplyDefault()).thenReturn(false);
        when(config.buildProducerConfig()).thenReturn(new Properties());
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
    void fieldNumbersRemainStableWhenTapTableOrderChanges() throws Exception {
        TapTable first = new TapTable("topic");
        first.add(new TapField("first", "STRING"));
        first.add(new TapField("second", "INTEGER"));
        TapTable reordered = new TapTable("topic");
        reordered.add(new TapField("second", "INTEGER"));
        reordered.add(new TapField("first", "STRING"));

        Descriptors.Descriptor firstDescriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, first);
        Descriptors.Descriptor reorderedDescriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, reordered);

        assertEquals(firstDescriptor.findFieldByName("first").getNumber(),
                reorderedDescriptor.findFieldByName("first").getNumber());
        assertEquals(firstDescriptor.findFieldByName("second").getNumber(),
                reorderedDescriptor.findFieldByName("second").getNumber());
    }

    @Test
    void changingFieldWireTypeWithinOneModeInstanceIsRejected() throws Exception {
        TapTable integerTable = new TapTable("topic");
        integerTable.add(new TapField("x", "INTEGER"));
        invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, integerTable);

        TapTable stringTable = new TapTable("topic");
        stringTable.add(new TapField("x", "STRING"));
        java.lang.reflect.InvocationTargetException error = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, stringTable));
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
    }

    @Test
    void int32ToSint32IsRejectedAsWireIncompatible() throws Exception {
        Descriptors.Descriptor registered = messageWithFields("Topic",
                field("x", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32));
        Descriptors.Descriptor changed = messageWithFields("Topic",
                field("x", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32));

        java.lang.reflect.InvocationTargetException error = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> invoke("validateWireCompatibility",
                        new Class[]{Descriptors.Descriptor.class, Descriptors.Descriptor.class},
                        registered, changed));

        assertInstanceOf(IllegalArgumentException.class, error.getCause());
    }

    @Test
    void readdingDeletedFieldWithDifferentWireTypeIsRejected() throws Exception {
        TapTable original = new TapTable("topic");
        original.add(new TapField("x", "INTEGER"));
        invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, original);

        // Building a descriptor without x records the deletion during this mode's lifetime.
        invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, new TapTable("topic"));

        TapTable readded = new TapTable("topic");
        readded.add(new TapField("x", "STRING"));
        java.lang.reflect.InvocationTargetException error = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, readded));
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
    }

    @Test
    void historicalFieldNumberDriftDoesNotBlockBuildingLatestRegisteredShape() throws Exception {
        Descriptors.Descriptor version1 = messageWithFields("Topic",
                field("a", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING),
                field("b", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING),
                field("c", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        Descriptors.Descriptor version2 = messageWithFields("Topic",
                field("a", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING),
                field("c", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        configureRegistryHistory(version1, version2);

        TapTable table = new TapTable("topic");
        table.add(new TapField("a", "STRING"));
        table.add(new TapField("c", "STRING"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        assertEquals(1, descriptor.findFieldByName("a").getNumber());
        assertEquals(2, descriptor.findFieldByName("c").getNumber());
    }

    @Test
    void readdingDeletedFieldWithSameTypeUsesNumberAfterAllHistoricalReservations() throws Exception {
        Descriptors.Descriptor version1 = messageWithFields("Topic",
                field("x", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32));
        Descriptors.Descriptor version2 = messageWithFields("Topic",
                field("y", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        configureRegistryHistory(version1, version2);

        TapTable table = new TapTable("topic");
        table.add(new TapField("x", "INTEGER"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        assertEquals(3, descriptor.findFieldByName("x").getNumber());
        assertNotEquals(1, descriptor.findFieldByName("x").getNumber());
    }

    @Test
    void registryDescriptorIdentityAndOneofSurviveIncrementalEvolution() throws Exception {
        DescriptorProtos.DescriptorProto registeredRoot = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Order")
                .addOneofDecl(DescriptorProtos.OneofDescriptorProto.newBuilder().setName("identity"))
                .addField(field("email", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, null).toBuilder()
                        .setOneofIndex(0).build())
                .addField(field("id", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .build();
        Descriptors.FileDescriptor registeredFile = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("orders.proto").setPackage("sales.v1").setSyntax("proto3")
                        .addMessageType(registeredRoot).build(), new Descriptors.FileDescriptor[0]);
        Descriptors.Descriptor registered = registeredFile.findMessageTypeByName("Order");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("email", "STRING"));
        table.add(new TapField("id", "INTEGER"));
        table.add(new TapField("createdAt", "STRING"));
        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        assertEquals("sales.v1.Order", evolved.getFullName());
        assertEquals("orders.proto", evolved.getFile().getName());
        assertEquals(registered.getOneofs().get(0).getName(), evolved.getOneofs().get(0).getName());
        assertEquals(0, evolved.findFieldByName("email").getContainingOneof().getIndex());
        assertEquals(3, evolved.findFieldByName("createdAt").getNumber());
        assertTrue(new ProtobufSchema(evolved).isBackwardCompatible(new ProtobufSchema(registered)).isEmpty());
    }

    @Test
    void addingFieldUnderTopLevelSiblingMessageIsMergedIntoRegisteredType() throws Exception {
        DescriptorProtos.DescriptorProto address = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Address")
                .addField(field("city", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProtos.DescriptorProto order = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Order")
                .addField(field("address", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".sales.Address"))
                .build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder().setName("sales.proto")
                        .setPackage("sales").setSyntax("proto3")
                        .addMessageType(order).addMessageType(address).build(), new Descriptors.FileDescriptor[0]);
        Descriptors.Descriptor registered = file.findMessageTypeByName("Order");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.city", "STRING"));
        table.add(new TapField("address.zip", "STRING"));
        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        Descriptors.Descriptor evolvedAddress = evolved.findFieldByName("address").getMessageType();
        assertEquals("sales.Address", evolvedAddress.getFullName());
        assertNotNull(evolvedAddress.findFieldByName("city"));
        assertEquals(2, evolvedAddress.findFieldByName("zip").getNumber());
        assertTrue(new ProtobufSchema(evolved).isBackwardCompatible(new ProtobufSchema(registered)).isEmpty());
    }

    @Test
    void importedMessageIsPreservedWhenRegistryBaselineHasNoChanges() throws Exception {
        Descriptors.Descriptor registered = orderWithImportedAddress();
        configureRegistry(registered);
        TapTable table = importedAddressTable(false);

        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        assertEquals("sales.Order", evolved.getFullName());
        assertEquals("common.Address", evolved.findFieldByName("address").getMessageType().getFullName());
        assertEquals("common.Geo", evolved.findFieldByName("address").getMessageType()
                .findFieldByName("geo").getMessageType().getFullName());
        Object converted = invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                Collections.singletonMap("geo", Collections.singletonMap("city", "Main")),
                evolved.findFieldByName("address"));
        DynamicMessage message = DynamicMessage.newBuilder(evolved)
                .setField(evolved.findFieldByName("address"), converted).build();
        DynamicMessage address = (DynamicMessage) message.getField(evolved.findFieldByName("address"));
        DynamicMessage geo = (DynamicMessage) address.getField(address.getDescriptorForType().findFieldByName("geo"));
        assertEquals("Main", geo.getField(geo.getDescriptorForType().findFieldByName("city")));
    }

    @Test
    void addingFieldUnderImportedMessageFailsWithClearError() throws Exception {
        configureRegistry(orderWithImportedAddress());
        TapTable table = importedAddressTable(true);

        java.lang.reflect.InvocationTargetException error = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> invoke("getOrCreateDescriptor", new Class[]{TapTable.class, String.class}, table, "topic"));

        assertInstanceOf(IllegalArgumentException.class, error.getCause());
        assertTrue(error.getCause().getMessage().contains("Cannot add field 'address.zip'"));
        assertTrue(error.getCause().getMessage().contains("imported Protobuf message 'common.Address'"));
    }

    @Test
    void restoredDescriptorWithImportedMessageCanStillBuildAndWrite() throws Exception {
        when(kafkaService.getSchemaRegistryClient()).thenReturn(mock(SchemaRegistryClient.class));
        TapTable sampled = importedAddressTable(false);
        Descriptors.Descriptor source = orderWithImportedAddress();
        invoke("persistDescriptor", new Class[]{TapTable.class, Descriptors.Descriptor.class}, sampled, source);

        TapTable restoredTable = importedAddressTable(false);
        restoredTable.setTableAttr(new java.util.HashMap<>(sampled.getTableAttr()));
        mode = new RegistryProtobufMode(kafkaService);
        Descriptors.Descriptor restored = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, restoredTable, "topic");

        assertEquals("sales.Order", restored.getFullName());
        assertEquals("common.Address", restored.findFieldByName("address").getMessageType().getFullName());
        Object converted = invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                Collections.singletonMap("geo", Collections.singletonMap("city", "Main")),
                restored.findFieldByName("address"));
        assertNotNull(DynamicMessage.newBuilder(restored)
                .setField(restored.findFieldByName("address"), converted).build());
    }

    private TapTable importedAddressTable(boolean includeNewField) {
        TapTable table = new TapTable("topic");
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.geo", "OBJECT"));
        table.add(new TapField("address.geo.city", "STRING"));
        if (includeNewField) table.add(new TapField("address.zip", "STRING"));
        return table;
    }

    private Descriptors.Descriptor orderWithImportedAddress() throws Exception {
        DescriptorProtos.FileDescriptorProto geoProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("common/geo.proto").setPackage("common").setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Geo")
                        .addField(field("city", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)))
                .build();
        Descriptors.FileDescriptor geoFile = Descriptors.FileDescriptor.buildFrom(
                geoProto, new Descriptors.FileDescriptor[0]);
        DescriptorProtos.FileDescriptorProto addressProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("common/address.proto").setPackage("common").setSyntax("proto3")
                .addDependency("common/geo.proto")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Address")
                        .addField(field("geo", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                ".common.Geo")))
                .build();
        Descriptors.FileDescriptor addressFile = Descriptors.FileDescriptor.buildFrom(
                addressProto, new Descriptors.FileDescriptor[]{geoFile});
        DescriptorProtos.FileDescriptorProto orderProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("sales/order.proto").setPackage("sales").setSyntax("proto3")
                .addDependency("common/address.proto")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Order")
                        .addField(field("address", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                ".common.Address")))
                .build();
        return Descriptors.FileDescriptor.buildFrom(orderProto, new Descriptors.FileDescriptor[]{addressFile})
                .findMessageTypeByName("Order");
    }

    @Test
    void recursiveRegisteredTypeRemainsRecursiveAfterEvolution() throws Exception {
        DescriptorProtos.DescriptorProto root = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Node")
                .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .addField(field("next", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".tree.Node"))
                .build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder().setName("tree.proto")
                        .setPackage("tree").setSyntax("proto3").addMessageType(root).build(),
                new Descriptors.FileDescriptor[0]);
        Descriptors.Descriptor registered = file.findMessageTypeByName("Node");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("id", "INTEGER"));
        table.add(new TapField("next", "OBJECT"));
        table.add(new TapField("next.createdAt", "STRING"));
        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        assertEquals("tree.Node", evolved.getFullName());
        assertEquals(evolved, evolved.findFieldByName("next").getMessageType());
        assertNotNull(evolved.findFieldByName("createdAt"));
        assertEquals(3, evolved.findFieldByName("createdAt").getNumber());
        assertTrue(new ProtobufSchema(evolved).isBackwardCompatible(new ProtobufSchema(registered)).isEmpty());
    }

    @Test
    void ancestorReferenceCycleDoesNotLoseNestedDdlFields() throws Exception {
        DescriptorProtos.DescriptorProto child = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Child")
                .addField(field("parent", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".tree.Root"))
                .build();
        DescriptorProtos.DescriptorProto root = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addNestedType(child)
                .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .addField(field("child", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        ".tree.Root.Child"))
                .build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder().setName("tree-cycle.proto")
                        .setPackage("tree").setSyntax("proto3").addMessageType(root).build(),
                new Descriptors.FileDescriptor[0]);
        Descriptors.Descriptor registered = file.findMessageTypeByName("Root");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("id", "INTEGER"));
        table.add(new TapField("child", "OBJECT"));
        table.add(new TapField("child.code", "STRING"));
        table.add(new TapField("child.parent", "OBJECT"));
        table.add(new TapField("child.parent.createdAt", "STRING"));
        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        assertNotNull(evolved.findFieldByName("createdAt"));
        assertEquals(3, evolved.findFieldByName("createdAt").getNumber());
        assertNotNull(evolved.findFieldByName("child").getMessageType().findFieldByName("code"));
        assertTrue(new ProtobufSchema(evolved).isBackwardCompatible(new ProtobufSchema(registered)).isEmpty());
    }

    @Test
    void nestedMessageSelfReferenceEvolvesTheNestedTypeItself() throws Exception {
        DescriptorProtos.DescriptorProto child = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Child")
                .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .addField(field("next", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        ".tree.Root.Child"))
                .build();
        DescriptorProtos.DescriptorProto root = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root").addNestedType(child)
                .addField(field("child", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        ".tree.Root.Child"))
                .build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder().setName("tree-nested-cycle.proto")
                        .setPackage("tree").setSyntax("proto3").addMessageType(root).build(),
                new Descriptors.FileDescriptor[0]);
        Descriptors.Descriptor registered = file.findMessageTypeByName("Root");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("child", "OBJECT"));
        table.add(new TapField("child.id", "INTEGER"));
        table.add(new TapField("child.next", "OBJECT"));
        table.add(new TapField("child.next.createdAt", "STRING"));
        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");
        Descriptors.Descriptor evolvedChild = evolved.findFieldByName("child").getMessageType();

        assertNotNull(evolvedChild.findFieldByName("createdAt"));
        assertEquals(3, evolvedChild.findFieldByName("createdAt").getNumber());
        assertNull(evolved.findFieldByName("createdAt"));
        assertTrue(new ProtobufSchema(evolved).isBackwardCompatible(new ProtobufSchema(registered)).isEmpty());
    }

    @Test
    void registeredCustomMessageMapValueSurvivesIncrementalEvolution() throws Exception {
        DescriptorProtos.DescriptorProto value = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Value")
                .addField(field("label", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProtos.DescriptorProto entry = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("AttributesEntry").setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
                .addField(field("key", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("value", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".maps.Value"))
                .build();
        DescriptorProtos.DescriptorProto root = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Record")
                .addNestedType(entry)
                .addField(field("attributes", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, ".maps.Record.AttributesEntry"))
                .addField(field("id", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder().setName("maps.proto")
                        .setPackage("maps").setSyntax("proto3").addMessageType(root).addMessageType(value).build(),
                new Descriptors.FileDescriptor[0]);
        Descriptors.Descriptor registered = file.findMessageTypeByName("Record");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("attributes", "MAP"));
        table.add(new TapField("id", "INTEGER"));
        table.add(new TapField("createdAt", "STRING"));
        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");
        Descriptors.FieldDescriptor attributes = evolved.findFieldByName("attributes");

        assertEquals("maps.Value", attributes.getMessageType().findFieldByName("value")
                .getMessageType().getFullName());
        Map<String, Object> row = Collections.singletonMap("label", "kept");
        Object converted = invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                Collections.singletonMap("one", row), attributes);
        DynamicMessage message = DynamicMessage.newBuilder(evolved).setField(attributes, converted).build();
        DynamicMessage storedValue = (DynamicMessage) ((List<?>) message.getField(attributes)).get(0);
        Descriptors.FieldDescriptor valueField = storedValue.getDescriptorForType().findFieldByName("value");
        DynamicMessage storedMapValue = (DynamicMessage) storedValue.getField(valueField);
        assertEquals("kept", storedMapValue.getField(storedMapValue.getDescriptorForType().findFieldByName("label")));
    }

    @Test
    void booleanNumericInputsAndUnsignedRangesAreValidated() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("enabled", "BOOLEAN"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor enabled = descriptor.findFieldByName("enabled");
        assertEquals(true, invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class}, 1, enabled));
        assertEquals(false, invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class}, "0", enabled));

        Descriptors.Descriptor unsignedDescriptor = messageWithFields("Unsigned",
                field("value", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32));
        Descriptors.FieldDescriptor unsigned = unsignedDescriptor.findFieldByName("value");
        java.lang.reflect.InvocationTargetException error = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class}, -1, unsigned));
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
    }

    @Test
    void protobufMessageValuesMustMatchTheFieldMessageType() throws Exception {
        Descriptors.Descriptor descriptor = messageWithDependency("Root", Struct.getDescriptor().getFile(),
                field("payload", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        ".google.protobuf.Struct"));
        java.lang.reflect.InvocationTargetException error = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                        Any.getDefaultInstance(), descriptor.findFieldByName("payload")));
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
    }

    private void configureRegistryHistory(Descriptors.Descriptor version1,
                                          Descriptors.Descriptor version2) throws Exception {
        SchemaRegistryClient registry = mock(SchemaRegistryClient.class);
        when(kafkaService.getSchemaRegistryClient()).thenReturn(registry);
        SchemaMetadata metadata1 = new SchemaMetadata(101, 1, ProtobufSchema.TYPE, Collections.emptyList(), "");
        SchemaMetadata metadata2 = new SchemaMetadata(102, 2, ProtobufSchema.TYPE, Collections.emptyList(), "");
        when(registry.getLatestSchemaMetadata("topic-value")).thenReturn(metadata2);
        when(registry.getAllVersions("topic-value")).thenReturn(Arrays.asList(1, 2));
        when(registry.getSchemaMetadata("topic-value", 1)).thenReturn(metadata1);
        when(registry.getSchemaMetadata("topic-value", 2)).thenReturn(metadata2);
        when(registry.getSchemaBySubjectAndId("topic-value", 101)).thenReturn(new ProtobufSchema(version1));
        when(registry.getSchemaBySubjectAndId("topic-value", 102)).thenReturn(new ProtobufSchema(version2));
    }

    @Test
    void newFieldNumberAppendsAfterRegisteredAndReservedNumbers() throws Exception {
        java.lang.reflect.Field mappingsField = RegistryProtobufMode.class.getDeclaredField("fieldNumbersBySubject");
        java.lang.reflect.Field reservedField = RegistryProtobufMode.class.getDeclaredField("reservedFieldNumbersBySubject");
        mappingsField.setAccessible(true);
        reservedField.setAccessible(true);
        Map<String, Map<String, Integer>> mappings = (Map<String, Map<String, Integer>>) mappingsField.get(mode);
        Map<String, Map<String, Set<Integer>>> reserved =
                (Map<String, Map<String, Set<Integer>>>) reservedField.get(mode);
        mappings.put("topic-value", new ConcurrentHashMap<>(Collections.singletonMap("current", 2)));
        Map<String, Set<Integer>> rootReservations = new ConcurrentHashMap<>();
        rootReservations.put("", new HashSet<>(Collections.singleton(1)));
        reserved.put("topic-value", rootReservations);

        int next = (Integer) invoke("fieldNumberFor", new Class[]{String.class, String.class},
                "topic-value", "discount");

        assertEquals(3, next);
    }

    @Test
    void restoredDescriptorSeedsFieldNumbersBeforeSchemaChanges() throws Exception {
        when(kafkaService.getSchemaRegistryClient()).thenReturn(mock(SchemaRegistryClient.class));
        TapTable table = new TapTable("topic");
        table.add(new TapField("a", "STRING"));
        table.add(new TapField("b", "STRING"));
        Descriptors.Descriptor source = messageWithFields("Topic",
                field("a", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING),
                field("b", 7, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        invoke("persistDescriptor", new Class[]{TapTable.class, Descriptors.Descriptor.class}, table, source);

        TapTable changedTable = new TapTable("topic");
        changedTable.setTableAttr(new java.util.HashMap<>(table.getTableAttr()));
        changedTable.add(new TapField("a", "STRING"));
        changedTable.add(new TapField("b", "STRING"));
        changedTable.add(new TapField("c", "STRING"));
        mode = new RegistryProtobufMode(kafkaService); // Simulate task restart.
        Descriptors.Descriptor restored = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, changedTable, "topic");
        assertEquals(3, restored.findFieldByName("a").getNumber());
        assertEquals(7, restored.findFieldByName("b").getNumber());
        assertEquals("Topic", restored.getFullName());
        assertEquals(8, restored.findFieldByName("c").getNumber());
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
        assertEquals(3, ((Map<?, ?>) converted.get("attributes")).get("retries"));
    }

    @Test
    void mapKeyAndValueTypesArePreservedWhenRebuildingDescriptor() throws Exception {
        TapTable table = new TapTable("topic");
        TapField attributes = new TapField("attributes", "MAP");
        attributes.setPureDataType("__protobuf_map__:SCALAR:INT64|SCALAR:STRING");
        table.add(attributes);

        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor mapField = descriptor.findFieldByName("attributes");
        Descriptors.Descriptor entry = mapField.getMessageType();
        assertEquals(Descriptors.FieldDescriptor.Type.INT64, entry.findFieldByName("key").getType());
        assertEquals(Descriptors.FieldDescriptor.Type.STRING, entry.findFieldByName("value").getType());

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("42", "answer");
        Object converted = invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class}, source, mapField);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).setField(mapField, converted).build();
        Map<String, Object> decoded = invoke("convertDynamicMessageToMap", new Class[]{DynamicMessage.class}, message);
        assertEquals("answer", ((Map<?, ?>) decoded.get("attributes")).get("42"));
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
        assertEquals(Arrays.asList("a", 2), converted.get("items"));
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
    void nestedReadKeepsDocumentShapeWithoutDottedAliases() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.street", "STRING"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor address = descriptor.findFieldByName("address");
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("street", "Main Street");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(address, invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                        source, address))
                .build();

        Map<String, Object> decoded = invoke("convertDynamicMessageToMap", new Class[]{DynamicMessage.class}, message);
        assertEquals("Main Street", ((Map<?, ?>) decoded.get("address")).get("street"));
        assertNull(decoded.get("address.street"));
    }

    @Test
    void nullMapValuesRoundTripAsNull() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("attributes", "MAP"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor mapField = descriptor.findFieldByName("attributes");
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("region", null);

        Object converted = invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class}, source, mapField);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).setField(mapField, converted).build();
        Map<String, Object> decoded = invoke("convertDynamicMessageToMap", new Class[]{DynamicMessage.class}, message);

        assertTrue(((Map<?, ?>) decoded.get("attributes")).containsKey("region"));
        assertNull(((Map<?, ?>) decoded.get("attributes")).get("region"));
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
    void additionalWellKnownTypesKeepDependenciesAndRepeatedWireTypes() throws Exception {
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addField(field("durations", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED,
                        ".google.protobuf.Duration"))
                .addField(field("event", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        ".google.protobuf.Any"))
                .build();
        Descriptors.FileDescriptor anyFile = Any.getDescriptor().getFile();
        Descriptors.FileDescriptor durationFile = Duration.getDescriptor().getFile();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("RootAdditionalWkt.proto")
                        .setSyntax("proto3")
                        .addDependency(anyFile.getName())
                        .addDependency(durationFile.getName())
                        .addMessageType(rootProto)
                        .build(), new Descriptors.FileDescriptor[]{anyFile, durationFile});
        TapTable table = new TapTable("topic");
        invoke("dynamicMessageToTapTable", new Class[]{TapTable.class, DynamicMessage.class, java.util.List.class},
                table, DynamicMessage.newBuilder(file.findMessageTypeByName("Root")).build(), Collections.emptyList());

        Descriptors.Descriptor rebuilt = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor durations = rebuilt.findFieldByName("durations");
        assertEquals("google.protobuf.Duration", durations.getMessageType().getFullName());
        assertTrue(durations.isRepeated());

        Map<String, Object> duration = new LinkedHashMap<>();
        duration.put("seconds", 1L);
        duration.put("nanos", 2);
        Object converted = invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                Collections.singletonList(duration), durations);
        DynamicMessage message = DynamicMessage.newBuilder(rebuilt).setField(durations, converted).build();
        assertEquals("google.protobuf.Duration",
                ((DynamicMessage) ((List<?>) message.getField(durations)).get(0))
                        .getDescriptorForType().getFullName());
    }

    @Test
    void repeatedScalarElementTypesArePreservedWhenRebuildingDescriptor() throws Exception {
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addField(field("tags", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, null))
                .addField(field("ids", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, null))
                .build();
        Descriptors.FileDescriptor rootFile = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("RootRepeated.proto")
                        .setSyntax("proto3")
                        .addMessageType(rootProto)
                        .build(), new Descriptors.FileDescriptor[0]);
        Descriptors.Descriptor root = rootFile.findMessageTypeByName("Root");
        TapTable table = new TapTable("topic");
        invoke("dynamicMessageToTapTable", new Class[]{TapTable.class, DynamicMessage.class, java.util.List.class},
                table, DynamicMessage.newBuilder(root).build(), Collections.emptyList());

        Descriptors.Descriptor rebuilt = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        assertEquals(Descriptors.FieldDescriptor.Type.STRING, rebuilt.findFieldByName("tags").getType());
        assertEquals(Descriptors.FieldDescriptor.Type.INT32, rebuilt.findFieldByName("ids").getType());
        assertTrue(rebuilt.findFieldByName("tags").isRepeated());
        assertTrue(rebuilt.findFieldByName("ids").isRepeated());
    }

    @Test
    void nullInsideStructRemainsNullAfterDynamicRoundTrip() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("payload", "OBJECT"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Descriptors.FieldDescriptor payload = descriptor.findFieldByName("payload");
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("value", null);

        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(payload, invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                        source, payload))
                .build();
        Map<String, Object> decoded = invoke("convertDynamicMessageToMap", new Class[]{DynamicMessage.class}, message);
        assertTrue(decoded.get("payload") instanceof Map);
        assertNull(((Map<?, ?>) decoded.get("payload")).get("value"));
    }

    @Test
    void repeatedEnumsPreserveEnumWireTypeWhenRebuildingDescriptor() throws Exception {
        DescriptorProtos.EnumDescriptorProto status = DescriptorProtos.EnumDescriptorProto.newBuilder()
                .setName("Status")
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("UNKNOWN").setNumber(0))
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("READY").setNumber(1))
                .build();
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addEnumType(status)
                .addField(field("statuses", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, ".Root.Status"))
                .build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("RootEnum.proto")
                        .setSyntax("proto3")
                        .addMessageType(rootProto)
                        .build(), new Descriptors.FileDescriptor[0]);
        TapTable table = new TapTable("topic");
        invoke("dynamicMessageToTapTable", new Class[]{TapTable.class, DynamicMessage.class, java.util.List.class},
                table, DynamicMessage.newBuilder(file.findMessageTypeByName("Root")).build(), Collections.emptyList());

        Descriptors.Descriptor rebuilt = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        assertEquals(Descriptors.FieldDescriptor.Type.ENUM, rebuilt.findFieldByName("statuses").getType());
        assertTrue(rebuilt.findFieldByName("statuses").isRepeated());
        assertEquals("READY", rebuilt.findFieldByName("statuses").getEnumType().findValueByName("READY").getName());
    }

    @Test
    void repeatedElementTypeChangeDoesNotEmitUnrepresentableAttributeEvent() throws Exception {
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
        assertTrue(events.isEmpty());
    }

    @Test
    void addingEnumValueIsWireCompatibleButRenumberingExistingValueIsNot() throws Exception {
        Descriptors.FieldDescriptor oldField = enumField("OldEnum.proto", 1, false);
        Descriptors.FieldDescriptor addedValueField = enumField("AddedEnum.proto", 1, true);
        Descriptors.FieldDescriptor renumberedField = enumField("RenumberedEnum.proto", 2, true);

        assertTrue((Boolean) invoke("wireFieldCompatible",
                new Class[]{Descriptors.FieldDescriptor.class, Descriptors.FieldDescriptor.class, java.util.Set.class},
                oldField, addedValueField, new java.util.HashSet<>()));
        assertFalse((Boolean) invoke("wireFieldCompatible",
                new Class[]{Descriptors.FieldDescriptor.class, Descriptors.FieldDescriptor.class, java.util.Set.class},
                oldField, renumberedField, new java.util.HashSet<>()));
    }

    private Descriptors.FieldDescriptor enumField(String fileName, int readyNumber, boolean includeAddedValue)
            throws Exception {
        DescriptorProtos.EnumDescriptorProto.Builder status = DescriptorProtos.EnumDescriptorProto.newBuilder()
                .setName("Status")
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("UNKNOWN").setNumber(0))
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("READY").setNumber(readyNumber));
        if (includeAddedValue) {
            status.addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                    .setName("NEW_VALUE").setNumber(readyNumber + 1));
        }
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addEnumType(status)
                .addField(field("status", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, ".Root.Status"))
                .build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName(fileName)
                        .setSyntax("proto3")
                        .addMessageType(rootProto)
                        .build(), new Descriptors.FileDescriptor[0]);
        return file.findMessageTypeByName("Root").findFieldByName("status");
    }

    @Test
    void largeNestedNumbersFallBackToStringsBeforeDoubleConversion() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("payload", "OBJECT"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 9007199254740993L);
        Descriptors.FieldDescriptor field = descriptor.findFieldByName("payload");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(field, invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                        payload, field))
                .build();

        Map<String, Object> decoded = invoke("convertDynamicMessageToMap", new Class[]{DynamicMessage.class}, message);
        assertEquals("9007199254740993", ((Map<?, ?>) decoded.get("payload")).get("id"));
    }

    @Test
    void highPrecisionAndNonFiniteNestedNumbersUseStringValues() throws Exception {
        TapTable table = new TapTable("topic");
        table.add(new TapField("payload", "OBJECT"));
        Descriptors.Descriptor descriptor = invoke("buildProtobufDescriptor", new Class[]{TapTable.class}, table);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("precise", new BigDecimal("0.10000000000000001"));
        payload.put("nan", Double.NaN);
        payload.put("infinity", Double.POSITIVE_INFINITY);
        Descriptors.FieldDescriptor field = descriptor.findFieldByName("payload");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(field, invoke("convertValue", new Class[]{Object.class, Descriptors.FieldDescriptor.class},
                        payload, field))
                .build();

        Map<String, Object> decoded = invoke("convertDynamicMessageToMap", new Class[]{DynamicMessage.class}, message);
        Map<?, ?> result = (Map<?, ?>) decoded.get("payload");
        assertEquals("0.10000000000000001", result.get("precise"));
        assertEquals("NaN", result.get("nan"));
        assertEquals("Infinity", result.get("infinity"));
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
    void restartBaselineDoesNotAlterScalarFields() throws Exception {
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
                .addField(field("enabled", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
                .addField(field("payload", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES))
                .build();
        Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("RootScalar.proto")
                        .setSyntax("proto3")
                        .addMessageType(rootProto)
                        .build(), new Descriptors.FileDescriptor[0]);
        TapTable table = new TapTable("topic");
        table.add(new TapField("id", "INTEGER"));
        table.add(new TapField("enabled", "BOOLEAN"));
        table.add(new TapField("payload", "BINARY"));

        List<TapEvent> events = invoke("detectSchemaChangesFromTableFlat",
                new Class[]{String.class, TapTable.class, Descriptors.Descriptor.class, long.class},
                "topic", table, file.findMessageTypeByName("Root"), 1L);
        assertTrue(events.isEmpty(), "unchanged scalar fields must not generate restart DDL");
    }

    @Test
    void restartBaselineDoesNotAlterRepeatedCustomMessages() throws Exception {
        DescriptorProtos.DescriptorProto item = nestedMessage("Item",
                field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32));
        Descriptors.Descriptor root = message("Root", field("items", 1,
                DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, ".Root.Item"), item);
        TapTable table = new TapTable("topic");
        table.add(new TapField("items", "ARRAY"));
        table.add(new TapField("items.id", "INTEGER"));

        List<TapEvent> events = invoke("detectSchemaChangesFromTableFlat",
                new Class[]{String.class, TapTable.class, Descriptors.Descriptor.class, long.class},
                "topic", table, root, 1L);
        assertTrue(events.isEmpty(), "unchanged repeated messages must not generate restart DDL");
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

    // ---------------------------------------------------------------------
    // 方案 A 回归：已存在 subject 时以 Registry 注册 descriptor 为身份基线做增量演进。
    // ---------------------------------------------------------------------

    @Test
    void existingSubjectReusesRegisteredSchemaIdentityWithoutRebuild() throws Exception {
        DescriptorProtos.DescriptorProto addressProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Address")
                .addField(field("street", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("address", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        ".com.example.Root.Address"))
                .addNestedType(addressProto)
                .build();
        Descriptors.Descriptor registered = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("source.proto")
                        .setSyntax("proto3")
                        .setPackage("com.example")
                        .addMessageType(rootProto)
                        .build(), new Descriptors.FileDescriptor[0]).findMessageTypeByName("Root");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("id", "STRING"));
        table.add(new TapField("address", "OBJECT"));
        table.add(new TapField("address.street", "STRING"));

        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        // package/根消息全名/嵌套类型名都来自 Registry，重启后不会因重建而改变身份。
        assertEquals("com.example.Root", evolved.getFullName());
        assertEquals("com.example", evolved.getFile().getPackage());
        assertEquals("com.example.Root.Address", evolved.findFieldByName("address").getMessageType().getFullName());
        // 与注册 schema 保持 wire 兼容（真实兼容校验不允许抛异常）。
        invoke("validateWireCompatibility",
                new Class[]{Descriptors.Descriptor.class, Descriptors.Descriptor.class}, registered, evolved);
    }

    @Test
    void existingSubjectPreservesOneofWhenEvolving() throws Exception {
        DescriptorProtos.FieldDescriptorProto aField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("a").setNumber(1)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setOneofIndex(0).build();
        DescriptorProtos.FieldDescriptorProto bField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("b").setNumber(2)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                .setOneofIndex(0).build();
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addOneofDecl(DescriptorProtos.OneofDescriptorProto.newBuilder().setName("choice"))
                .addField(aField)
                .addField(bField)
                .addField(field("c", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        Descriptors.Descriptor registered = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("OneofRoot.proto").setSyntax("proto3")
                        .addMessageType(rootProto).build(),
                new Descriptors.FileDescriptor[0]).findMessageTypeByName("Root");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("a", "STRING"));
        table.add(new TapField("b", "INTEGER"));
        table.add(new TapField("c", "STRING"));

        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        assertEquals(1, evolved.getOneofs().size());
        assertNotNull(evolved.findFieldByName("a").getContainingOneof());
        assertNotNull(evolved.findFieldByName("b").getContainingOneof());
        assertNull(evolved.findFieldByName("c").getContainingOneof());
    }

    @Test
    void existingSubjectPreservesRecursiveMessageField() throws Exception {
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Node")
                .addField(field("value", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("next", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".Node"))
                .build();
        Descriptors.Descriptor registered = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("Node.proto").setSyntax("proto3")
                        .addMessageType(rootProto).build(),
                new Descriptors.FileDescriptor[0]).findMessageTypeByName("Node");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("value", "STRING"));
        table.add(new TapField("next", "OBJECT"));

        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        Descriptors.FieldDescriptor next = evolved.findFieldByName("next");
        assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, next.getType());
        assertEquals("Node", next.getMessageType().getFullName());
        assertNotEquals("google.protobuf.Struct", next.getMessageType().getFullName());
    }

    @Test
    void existingSubjectPreservesCustomMapValueMessage() throws Exception {
        DescriptorProtos.DescriptorProto money = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Money")
                .addField(field("amount", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .build();
        DescriptorProtos.DescriptorProto entry = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("PricesEntry")
                .setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
                .addField(field("key", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("value", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, ".Root.Money"))
                .build();
        DescriptorProtos.DescriptorProto rootProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Root")
                .addNestedType(money)
                .addNestedType(entry)
                .addField(field("prices", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, ".Root.PricesEntry"))
                .build();
        Descriptors.Descriptor registered = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("MapRoot.proto").setSyntax("proto3")
                        .addMessageType(rootProto).build(),
                new Descriptors.FileDescriptor[0]).findMessageTypeByName("Root");
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("prices", "MAP"));

        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        Descriptors.FieldDescriptor prices = evolved.findFieldByName("prices");
        assertTrue(prices.isMapField());
        assertEquals("Root.Money",
                prices.getMessageType().findFieldByName("value").getMessageType().getFullName());
    }

    @Test
    void existingSubjectAppendsNewFieldsAfterRegisteredNumbers() throws Exception {
        Descriptors.Descriptor registered = messageWithFields("Root",
                field("a", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING),
                field("b", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        configureRegistry(registered);

        TapTable table = new TapTable("topic");
        table.add(new TapField("a", "STRING"));
        table.add(new TapField("b", "STRING"));
        table.add(new TapField("c", "STRING"));

        Descriptors.Descriptor evolved = invoke("getOrCreateDescriptor",
                new Class[]{TapTable.class, String.class}, table, "topic");

        assertEquals(1, evolved.findFieldByName("a").getNumber());
        assertEquals(2, evolved.findFieldByName("b").getNumber());
        assertEquals(3, evolved.findFieldByName("c").getNumber());
    }

    private void configureRegistry(Descriptors.Descriptor... versions) throws Exception {
        SchemaRegistryClient registry = mock(SchemaRegistryClient.class);
        when(kafkaService.getSchemaRegistryClient()).thenReturn(registry);
        int latestVersion = versions.length;
        SchemaMetadata latest = new SchemaMetadata(1000 + latestVersion, latestVersion,
                ProtobufSchema.TYPE, Collections.emptyList(), "");
        when(registry.getLatestSchemaMetadata("topic-value")).thenReturn(latest);
        List<Integer> allVersions = new java.util.ArrayList<>();
        for (int i = 0; i < versions.length; i++) {
            allVersions.add(i + 1);
        }
        when(registry.getAllVersions("topic-value")).thenReturn(allVersions);
        for (int i = 0; i < versions.length; i++) {
            int version = i + 1;
            SchemaMetadata metadata = new SchemaMetadata(1000 + version, version,
                    ProtobufSchema.TYPE, Collections.emptyList(), "");
            when(registry.getSchemaMetadata("topic-value", version)).thenReturn(metadata);
            when(registry.getSchemaBySubjectAndId("topic-value", 1000 + version))
                    .thenReturn(new ProtobufSchema(versions[i]));
        }
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

    private static Descriptors.Descriptor messageWithFields(String name,
                                                              DescriptorProtos.FieldDescriptorProto... fields)
            throws Exception {
        DescriptorProtos.DescriptorProto.Builder builder = DescriptorProtos.DescriptorProto.newBuilder().setName(name);
        builder.addAllField(Arrays.asList(fields));
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
