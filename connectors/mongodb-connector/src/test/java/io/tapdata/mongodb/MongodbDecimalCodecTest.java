package io.tapdata.mongodb;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.value.TapDecimalValue;
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
        assertInstanceOf(TapDecimalValue.class, value);
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
}
