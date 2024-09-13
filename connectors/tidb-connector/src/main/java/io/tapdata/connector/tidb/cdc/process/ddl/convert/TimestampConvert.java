package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
        Object timestamp = covertToDateTime(fromValue, precision, "yyyy-MM-dd HH:mm:ss", timezone);
        if (timestamp instanceof LocalDateTime) {
            return ((LocalDateTime) timestamp).atZone(ZoneOffset.UTC);
        } else if (timestamp instanceof Instant) {
            return ((Instant) timestamp).atZone(ZoneOffset.UTC);
        }
        return timestamp;
    }
}
