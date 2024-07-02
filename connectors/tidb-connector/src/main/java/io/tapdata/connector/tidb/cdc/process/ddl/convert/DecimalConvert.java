package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import java.math.BigDecimal;

public class DecimalConvert implements Convert {
    int precision;
    int scale;
    public DecimalConvert(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }
    public DecimalConvert(String precision, String scale) {
        this.precision = parseInt(precision, 1);
        this.scale = parseInt(scale, 0);
    }
    @Override
    public Object convert(Object fromValue) {
        if(null == fromValue) return null;
        if (fromValue instanceof String) {
            return new BigDecimal((String)fromValue);
        } else if (fromValue instanceof BigDecimal) {
            return fromValue;
        } else if (fromValue instanceof Number) {
            return new BigDecimal(String.valueOf(fromValue));
        }
        return new BigDecimal(0);
    }
}
