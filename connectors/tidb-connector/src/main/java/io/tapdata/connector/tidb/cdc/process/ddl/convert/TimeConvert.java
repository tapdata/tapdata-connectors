package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class TimeConvert implements Convert {
    @Override
    public Object convert(Object fromValue) {
        return fromValue;
    }
}
