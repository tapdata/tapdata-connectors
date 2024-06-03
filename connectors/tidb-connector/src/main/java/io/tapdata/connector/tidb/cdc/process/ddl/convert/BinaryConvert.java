package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class BinaryConvert implements Convert {
    @Override
    public Object convert(Object fromValue) {
        if (null == fromValue) return null;
        if (fromValue instanceof byte[]) {
            return fromValue;
        }
        return String.valueOf(fromValue).getBytes();
    }
}
