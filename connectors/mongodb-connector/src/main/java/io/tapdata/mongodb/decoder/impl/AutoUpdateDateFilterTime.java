package io.tapdata.mongodb.decoder.impl;

import io.tapdata.entity.error.CoreException;
import io.tapdata.mongodb.decoder.CustomSQLObject;
import io.tapdata.util.DateUtil;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

public class AutoUpdateDateFilterTime implements CustomSQLObject<Object, Map<String, Object>> {
    public static final String DYNAMIC_DATE = "$dynamicDate";
    public static final String FORMAT = "format";
    public static final String TO_STRING = "toString";
    public static final String SUBTRACT = "subtract";


    @Override
    public Object execute(Object functionObj, Map<String, Object> curMap) {
        Object filter = null;
        boolean toString = false;
        long subtract = 0;
        if (functionObj instanceof Map) {
            filter = ((Map<String, Object>) functionObj).get(FORMAT);
            toString = Boolean.TRUE.equals(((Map<String, Object>) functionObj).get(TO_STRING));
            Object sub = Optional.ofNullable(((Map<String, Object>) functionObj).get(SUBTRACT)).orElse(0);
            if (sub instanceof Number) {
                subtract = ((Number)sub).longValue();
            } else {
                try {
                    subtract = Long.parseLong(String.valueOf(sub));
                } catch (Exception ignore) {
                    throw new IllegalArgumentException("a subtract value is illegal value, \"subtract\" must be a number");
                }
            }
        }
        if (null == filter) {
            throw new IllegalArgumentException("a format value is empty, \"format\" is a required field");
        }


        Long timestamp = null;
        String filterValue = String.valueOf(filter);

        if (filter instanceof Number) {
            timestamp = ((Number) filter).longValue();
        } else if (filterValue.matches("^[0-9]*$")) {
            timestamp = Long.parseLong(filterValue);
        }

        if (null == timestamp) {
            return covertTime(filterValue, subtract, toString);
        }
        return covertTimestamp(timestamp, subtract);
    }

    @Override
    public String getFunctionName() {
        return DYNAMIC_DATE;
    }

    protected Object covertTimestamp(Long dateTime, long subtract) {
        if (null == dateTime) {
            throw new CoreException("Illegal argument in function: {}, wrong value: {}, the correct key value pairs should be as follows: \"\": {an timestamp which more than zero or a data time string}",
                    getFunctionName(), dateTime, getFunctionName());
        }
        return covert(new Date(dateTime), subtract);
    }

    protected Object covertTime(String dateTime, long subtract, boolean toString) {
        dateTime = replaceDate(dateTime);
        String dateFormat = DateUtil.determineDateFormat(dateTime);
        if (null == dateFormat) {
            throw new CoreException("Illegal argument in function: {}, wrong value: {}, the correct key value pairs should be as follows: \"\": {\"yyyy-dd-MM hh:mm:ss[.SSSSSS]\"}",
                    getFunctionName(), dateTime, getFunctionName());
        }
        Object parse = DateUtil.parse(dateTime);
        Date covert = covert(parse, subtract);
        String dateStr = DateUtil.timeStamp2Date(String.valueOf(covert.getTime()), dateFormat);
        if (toString) {
            return dateStr;
        }
        return covert;
    }

    protected String replaceDate(String format) {
        Calendar calendar = Calendar.getInstance();
        format = format(format, "%y", String.valueOf(calendar.get(Calendar.YEAR)));
        format = format(format, "%M", String.valueOf(calendar.get(Calendar.MONTH)+1));
        format = format(format, "%d", String.valueOf(calendar.get(Calendar.DATE)));
        format = format(format, "%h", String.valueOf(calendar.get(Calendar.HOUR)));
        format = format(format, "%m", String.valueOf(calendar.get(Calendar.YEAR)));
        format = format(format, "%s", String.valueOf(calendar.get(Calendar.SECOND)));
        format = format(format, "%S", String.valueOf(calendar.get(Calendar.MILLISECOND)));
        return format;
    }

    protected String format(String str, String format, String value) {
        if (str.contains(format)) {
            str = str.replace(format, value);
        }
        return str;
    }

    protected Date covert(Object parse, long subtract) {
        if (parse instanceof Date) {
            return calculateDate((Date)parse, subtract);
        } else {
            throw new CoreException("UNKnow date time format string: " + parse);
        }
    }

    protected Date calculateDate(Date parse, long subtract) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(parse);
        calendar.add(Calendar.MILLISECOND, -1 * ((Long)subtract).intValue());
        return calendar.getTime();
    }
}