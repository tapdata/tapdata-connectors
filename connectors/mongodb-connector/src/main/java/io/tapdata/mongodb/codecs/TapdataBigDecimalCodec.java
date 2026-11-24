package io.tapdata.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.Decimal128;

import java.math.BigDecimal;

public final class TapdataBigDecimalCodec implements Codec<BigDecimal> {

    @Override
    public void encode(final BsonWriter writer, final BigDecimal value, final EncoderContext encoderContext) {
        if (value == null) {
            writer.writeNull();
            return;
        }
        try {
            writer.writeDecimal128(new Decimal128(value));
        } catch (NumberFormatException outsideDecimal128) {
            // BSON cannot represent every source decimal numerically. Preserve its
            // exact digits/exponent as text instead of silently rounding the value.
            writer.writeString(value.toString());
        }
    }

    @Override
    public BigDecimal decode(final BsonReader reader, final DecoderContext decoderContext) {
        if (reader.getCurrentBsonType() == BsonType.NULL) {
            reader.readNull();
            return null;
        }
        if (reader.getCurrentBsonType() == BsonType.STRING) return new BigDecimal(reader.readString());
        return reader.readDecimal128().bigDecimalValue();
    }

    @Override
    public Class<BigDecimal> getEncoderClass() {
        return BigDecimal.class;
    }
}
