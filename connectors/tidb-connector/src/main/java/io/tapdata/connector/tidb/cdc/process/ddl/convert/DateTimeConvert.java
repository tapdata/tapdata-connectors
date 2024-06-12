package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import java.util.TimeZone;

public class DateTimeConvert implements Convert {
    int precision;
    TimeZone timezone;

    public DateTimeConvert(String precision, TimeZone timezone) {
        this.precision = parseInt(precision, 0);
        this.timezone = timezone;
    }

    @Override
    public Object convert(Object fromValue) {
        return covertToDateTime(fromValue, precision, "yyyy-MM-dd hh:mm:ss%s", timezone);
    }
}
