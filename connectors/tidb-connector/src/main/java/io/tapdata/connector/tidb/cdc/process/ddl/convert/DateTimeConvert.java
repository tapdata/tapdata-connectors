package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import java.time.LocalDateTime;
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
        Object datetime = covertToDateTime(fromValue, precision, "yyyy-MM-dd HH:mm:ss", timezone);
        if (datetime instanceof LocalDateTime) {
            return ((LocalDateTime)datetime).minusHours(timezone.getRawOffset() / 1000 / 60 / 60);
        }
        return datetime;
    }
}
