package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class SmallIntConvert implements Convert {
    boolean unsigned;
    public SmallIntConvert(boolean unsigned) {
        this.unsigned = unsigned;
    }

    @Override
    public Object convert(Object fromValue) {
        if (null == fromValue) return null;
        if (fromValue instanceof String) {
            return parse((String) fromValue);
        } else if (fromValue instanceof Short) {
            return fromValue;
        }
        return 0;
    }

    protected Object parse(String value) {
        try {
            if (unsigned) {
                return Integer.parseInt(value);
            }
            return Short.parseShort(value);
        } catch (Exception e) {
            return 0;
        }
    }
}
