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
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.KafkaConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
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
