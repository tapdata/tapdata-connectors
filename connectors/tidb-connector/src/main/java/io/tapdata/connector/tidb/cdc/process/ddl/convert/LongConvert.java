package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class LongConvert implements Convert {
    boolean unsigned;

    public LongConvert(boolean unsigned) {
        this.unsigned = unsigned;
    }

    @Override
    public Object convert(Object fromValue) {
        if (fromValue instanceof String) {
            return parseLong((String) fromValue);
        } else if (fromValue instanceof Number) {
            return ((Number) fromValue).longValue();
        }
        return null;
    }

    protected Long parseLong(String value) {
        try {
            return unsigned ? Long.parseUnsignedLong(value) : Long.parseLong(value);
        } catch (Exception e) {
            return 0L;
        }
    }
}
