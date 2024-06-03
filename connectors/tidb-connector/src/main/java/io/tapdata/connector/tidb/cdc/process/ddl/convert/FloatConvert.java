package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class FloatConvert implements Convert {
    boolean unsigned;
    int precision;
    int scale;
    public FloatConvert(boolean unsigned) {
        this.unsigned = unsigned;
        this.precision = 1;
        this.scale = 0;
    }

    public FloatConvert(boolean unsigned, int precision, int scale) {
        this.unsigned = unsigned;
        this.scale = scale;
        this.precision = precision;
    }

    public FloatConvert(boolean unsigned, String precision, String scale) {
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
            return ((Number)fromValue).floatValue();
        }
        return 0F;
    }

    protected Object parse(String value) {
        try {
            return Float.parseFloat(value);
        } catch (Exception e) {
            return 0F;
        }
    }
}
