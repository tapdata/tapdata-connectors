package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class TinyIntConvert implements Convert {
    boolean unsigned;

    public TinyIntConvert(boolean unsigned) {
        this.unsigned = unsigned;
    }

    @Override
    public Object convert(Object fromValue) {
        if (null == fromValue) return null;
        if (fromValue instanceof String) {
            return parse((String) fromValue);
        } else if (fromValue instanceof Number) {
            return unsigned ? ((Number) fromValue).shortValue() : ((Number) fromValue).byteValue();
        }
        return 0;
    }

    protected Object parse(String value) {
        try {
            return unsigned ? Short.parseShort(value) : Byte.parseByte(value);
        } catch (Exception e) {
            return 0;
        }
    }
}
