package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class VarCharConvert implements Convert {
    public VarCharConvert() { }

    @Override
    public Object convert(Object fromValue) {
        if (null == fromValue) return null;
        return fromValue instanceof String ? fromValue : String.valueOf(fromValue);
    }
}
