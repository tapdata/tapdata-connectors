package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class IntegerConvert implements Convert {
    boolean unsigned;

    public IntegerConvert(boolean unsigned) {
        this.unsigned = unsigned;
    }

    @Override
    public Object convert(Object fromValue) {
        if (fromValue instanceof String) {
            return parse((String) fromValue);
        } else if (fromValue instanceof Number) {
            return ((Number) fromValue).intValue();
        }
        return null;
    }

    protected Object parse(String value) {
        try {
            if (unsigned) {
                return Long.parseUnsignedLong(value);
            }
            return Integer.parseInt(value);
        } catch (Exception e) {
            return 0L;
        }
    }
}
