package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import java.util.TimeZone;

public class TimestampConvert implements Convert {
    int precision;
    TimeZone timezone;
    public TimestampConvert(String precision, TimeZone timezone) {
        this.precision = parseInt(precision, 0);
        this.timezone = timezone;
    }

    @Override
    public Object convert(Object fromValue) {
        return covertToDateTime(fromValue, precision, "yyyy-MM-dd HH:mm:ss", timezone);
    }
}
