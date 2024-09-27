package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.TimeZone;

public class TimestampConvert implements Convert {
    int precision;
    TimeZone timezone;
    TimeZone dbTimezone;

    public TimestampConvert(String precision, TimeZone timezone, TimeZone dbTimezone) {
        this.precision = parseInt(precision, 0);
        this.timezone = timezone;
        this.dbTimezone = dbTimezone;
    }

    @Override
    public Object convert(Object fromValue) {
        Object timestamp = covertToDateTime(fromValue, precision, "yyyy-MM-dd HH:mm:ss", timezone);
        if (timestamp instanceof LocalDateTime) {
            return ((LocalDateTime) timestamp).plusSeconds((dbTimezone.getRawOffset() - TimeZone.getDefault().getRawOffset()) / 1000).atZone(ZoneOffset.UTC);
        } else if (timestamp instanceof Instant) {
            return ((Instant) timestamp).plusMillis(dbTimezone.getRawOffset() - TimeZone.getDefault().getRawOffset()).atZone(ZoneOffset.UTC);
        }
        return timestamp;
    }
}
