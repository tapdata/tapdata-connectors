package io.tapdata.mongodb.codecs;

import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;

import java.math.BigDecimal;

/** Exact numeric result for a contract whose legacy TapNumberValue holds only a Double. */
public final class ExactDecimalValue extends TapValue<BigDecimal, TapNumber> {
    public ExactDecimalValue(BigDecimal value) {
        this.value = value;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapNumber().precision(value.precision()).scale(value.scale());
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapNumber.class;
    }
}
