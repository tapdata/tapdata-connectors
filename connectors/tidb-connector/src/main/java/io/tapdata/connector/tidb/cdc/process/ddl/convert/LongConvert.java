package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import java.math.BigDecimal;

public class LongConvert implements Convert {
    boolean unsigned;

    public LongConvert(boolean unsigned) {
        this.unsigned = unsigned;
    }

    @Override
    public Object convert(Object fromValue) {
        if (fromValue instanceof String) {
            return parse((String) fromValue);
        } else if (fromValue instanceof Number) {
            return ((Number) fromValue).longValue();
        }
        return null;
    }

    protected Object parse(String value) {
        try {
            if (unsigned) {
                return new BigDecimal(value);
            }
            return Long.parseLong(value);
        } catch (Exception e) {
            return 0L;
        }
    }
}
