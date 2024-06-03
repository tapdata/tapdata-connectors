package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class DateConvert implements Convert {
    @Override
    public Object convert(Object fromValue) {
        return fromValue;
    }
}
