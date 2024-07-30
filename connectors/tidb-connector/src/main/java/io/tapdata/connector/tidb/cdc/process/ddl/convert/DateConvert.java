package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import java.util.TimeZone;

public class DateConvert implements Convert {
    TimeZone timezone;
    public DateConvert(TimeZone timezone) {
        this.timezone = timezone;
    }
    @Override
    public Object convert(Object fromValue) {
        return covertToDate(fromValue, 0, "yyyy-MM-dd", timezone);
    }
}
