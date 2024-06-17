package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class YearConvert implements Convert {
    boolean unsigned;
    public YearConvert(boolean unsigned) {
        this.unsigned = unsigned;
    }
    @Override
    public Object convert(Object fromValue) {
        if (null == fromValue) return null;
        if (fromValue instanceof String) {
            return parse((String) fromValue);
        } else if (fromValue instanceof Number) {
            return ((Number) fromValue).intValue();
        }
        return 1901;
    }
    protected Object parse(String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return 1901;
        }
    }
}
