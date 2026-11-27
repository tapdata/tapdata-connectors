package io.tapdata.mongodb;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapNumber;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.bson.types.Decimal128;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.EncoderContext;
import io.tapdata.mongodb.codecs.TapdataBigDecimalCodec;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MongodbDecimalCodecTest {
    @Test
    void decimal128SurvivesTargetAndInspectionRegistryBoundaries() {
        BigDecimal exact = new BigDecimal("9007199254740993.123456789012345678");
        TapCodecsRegistry source = TapCodecsRegistry.create();
        new MongodbConnector().registerCapabilities(new ConnectorFunctions(), source);
        Object value = source.getCustomToTapValueCodec(Decimal128.class).toTapValue(new Decimal128(exact), null);
        assertInstanceOf(TapRawValue.class, value);
        Map<String, Object> targetEvent = new LinkedHashMap<>();
        targetEvent.put("amount", value);
        TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(targetEvent);
        assertEquals(exact, targetEvent.get("amount"));

        // Encode the portable target value using MongoDB's actual BSON codec.
        BsonDocument document = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(document);
        writer.writeStartDocument();
        writer.writeName("amount");
        new TapdataBigDecimalCodec().encode(writer, (BigDecimal) targetEvent.get("amount"),
                EncoderContext.builder().build());
        writer.writeEndDocument();
        assertEquals(new Decimal128(exact), document.getDecimal128("amount").getValue());

        // Inspection reads BSON through the source registry and unwraps through defaults.
        Map<String, Object> inspection = new LinkedHashMap<>();
        inspection.put("amount", source.getCustomToTapValueCodec(Decimal128.class)
                .toTapValue(document.getDecimal128("amount").getValue(), null));
        TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(inspection);
        assertEquals(exact, inspection.get("amount"));
    }
    @Test
    void fullFilterChainPreservesExactNestedAndExtremeDecimals() {
        TapCodecsRegistry source = TapCodecsRegistry.create();
        new MongodbConnector().registerCapabilities(new ConnectorFunctions(), source);
        for (String text : Arrays.asList("9007199254740993", "123.4500",
                "9007199254740993.123456789012345678", "1E+6144", "1E-6176")) {
            BigDecimal exact = new BigDecimal(text);
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("amount", new Decimal128(exact));
            Map<String, Object> nested = new LinkedHashMap<>();
            nested.put("amount", new Decimal128(exact));
            event.put("nested", nested);
            event.put("items", new ArrayList<>(Arrays.asList(new Decimal128(exact))));
            Map<String, TapField> schema = Collections.singletonMap("amount",
                    new TapField("amount", "decimal").tapType(new TapNumber()));
            TapCodecsFilterManager.create(source).transformToTapValueMap(event, schema);
            assertInstanceOf(TapRawValue.class, event.get("amount"));
            TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(event);
            // Decimal128 normalizes the exponent for extreme values; compare its exact BigDecimal.
            BigDecimal expected = new Decimal128(exact).bigDecimalValue();
            assertEquals(expected, event.get("amount"));
            assertEquals(expected, nested.get("amount"));
            assertEquals(expected, ((java.util.List<?>) event.get("items")).get(0));
        }
    }

    @Test
    void nonFiniteDecimalsKeepExistingDoubleFallback() {
        TapCodecsRegistry source = TapCodecsRegistry.create();
        new MongodbConnector().registerCapabilities(new ConnectorFunctions(), source);
        for (Decimal128 special : Arrays.asList(Decimal128.NaN, Decimal128.POSITIVE_INFINITY,
                Decimal128.NEGATIVE_INFINITY, Decimal128.NEGATIVE_ZERO)) {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("amount", source.getCustomToTapValueCodec(Decimal128.class).toTapValue(special, null));
            TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(event);
            assertEquals(special.doubleValue(), event.get("amount"));
        }
    }

}
