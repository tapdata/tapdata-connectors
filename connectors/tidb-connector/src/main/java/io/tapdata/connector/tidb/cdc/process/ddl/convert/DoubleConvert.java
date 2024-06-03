package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class DoubleConvert implements Convert {
    boolean unsigned;
    int precision;
    int scale;
    public DoubleConvert(boolean unsigned) {
        this.unsigned = unsigned;
        this.precision = 1;
        this.scale = 0;
    }

    public DoubleConvert(boolean unsigned, int precision, int scale) {
        this.unsigned = unsigned;
        this.scale = scale;
        this.precision = precision;
    }

    public DoubleConvert(boolean unsigned, String precision, String scale) {
        this.unsigned = unsigned;
        this.scale = parseInt(scale, 0);
        this.precision = parseInt(precision, 1);
    }

    @Override
    public Object convert(Object fromValue) {
        if (null == fromValue) return null;
        if (fromValue instanceof String) {
            return parse((String) fromValue);
        } else if (fromValue instanceof Number) {
            return ((Number)fromValue).doubleValue();
        }
        return 0D;
    }

    protected Object parse(String value) {
        try {
            return Double.parseDouble(value);
        } catch (Exception e) {
            return 0D;
        }
    }
}
