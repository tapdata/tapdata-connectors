package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class BitConvert implements Convert {
    @Override
    public Object convert(Object fromValue) {
        if (null == fromValue) return null;
        if(fromValue instanceof String) {
            return ((String) fromValue).getBytes();
        } else if (fromValue instanceof byte[]) {
            return fromValue;
        }
        return 0;
    }
}
