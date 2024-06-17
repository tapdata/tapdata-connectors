package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class EnumConvert implements Convert {
    @Override
    public Object convert(Object fromValue) {
        if (null == fromValue) return null;
        if (fromValue instanceof String) {
            return fromValue;
        }
        return String.valueOf(fromValue);
    }
}
