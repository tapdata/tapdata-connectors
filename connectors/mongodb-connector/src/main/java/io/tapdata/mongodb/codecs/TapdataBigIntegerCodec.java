package io.tapdata.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.math.BigInteger;

public class TapdataBigIntegerCodec implements Codec<BigInteger> {

    @Override
    public BigInteger decode(BsonReader reader, DecoderContext decoderContext) {
        if (reader.getCurrentBsonType() == BsonType.NULL) {
            reader.readNull();
            return null;
        }
        if (reader.getCurrentBsonType() == BsonType.STRING) return new java.math.BigDecimal(reader.readString()).toBigIntegerExact();
        if (reader.getCurrentBsonType() == BsonType.DECIMAL128) return reader.readDecimal128().bigDecimalValue().toBigIntegerExact();
        return BigInteger.valueOf(reader.readInt64());
    }

    @Override
    public void encode(BsonWriter writer, BigInteger value, EncoderContext encoderContext) {
        if (value == null) {
            writer.writeNull();
            return;
        }
        if (value.bitLength() <= 63) writer.writeInt64(value.longValue());
        else new TapdataBigDecimalCodec().encode(writer, new java.math.BigDecimal(value), encoderContext);
    }

    @Override
    public Class<BigInteger> getEncoderClass() {
        return BigInteger.class;
    }

}
