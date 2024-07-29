package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import java.util.TimeZone;

public class TimeConvert implements Convert {
    int precision;
    TimeZone timezone;
    public TimeConvert(String precision, TimeZone timezone) {
        this.precision = parseInt(precision, 0);
        this.timezone = timezone;
    }

    @Override
    public Object convert(Object fromValue) {
        return covertToTime(fromValue, precision, "hh:mm:ss", timezone);
    }
}
