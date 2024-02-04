package io.tapdata.connector.gauss.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

public class TimeUtil {
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static Date parseDate(String dateStr, String format) {
        java.util.Date date = null;
        SimpleDateFormat srtFormat = new SimpleDateFormat(format);
        try {
            date = srtFormat.parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date;
    }
    public static Instant parseDateTime(String dateStr, int faction, boolean withTimeZone) {
        StringBuilder format = new StringBuilder(DATE_TIME_FORMAT);
        int indexOf = dateStr.indexOf(".");
        int timezone = 0;
        if (withTimeZone) {
            int zoneSplit = 0;
            if (dateStr.contains("+")) {
                zoneSplit = dateStr.indexOf("+");
            } else if (dateStr.contains("-")) {
                zoneSplit = dateStr.indexOf("-");
            }

            if (zoneSplit > 0) {
                try {
                    String zoneString = dateStr.substring(zoneSplit);
                    String[] split = zoneString.split(":");
                    if (split.length > 0) {
                        timezone = Integer.parseInt(split[0]);
                    }
                } catch (Exception e) {

                }
                dateStr = dateStr.substring(0, zoneSplit);
            }
        }
        byte type = 0;
        if (faction > 0) {
            int with = 0;
            if (indexOf > 0) {
                int zeroCount = dateStr.length() - indexOf - 1;
                if (zeroCount > faction) {
                    type = 1;
                } else if (zeroCount < faction) {
                    type = -1;
                } else {
                    type = 2;
                }
                with = faction - zeroCount;
            } else {
                with = faction;
            }
            format.append(".");
            for (int index = 0; index < faction; index++) {
                format.append("S");
            }
            if (type == 0) {
                dateStr = dateStr + ".";
            }
            if (type == -1 || type == 0) {
                StringBuilder b = new StringBuilder();
                for (int index = 0; index < with; index++) {
                    b.append("0");
                }
                dateStr += b.toString();
            } else if (type == 1){
                dateStr = dateStr.substring(0, dateStr.length() - with);
            }
        } else {
            if (indexOf > 0) {
                dateStr = dateStr.substring(0, indexOf);
            }
        }
        try {
            LocalDateTime parse = LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern(format.toString()));
            return parse.toInstant(ZoneOffset.ofHours(timezone));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static long parseTimestamp(String dateStr, String format, long defaultTime) {
        Date date = parseDate(dateStr, format);
        return null == date ? defaultTime : date.getTime();
    }

    public static long parseTimestampWitTimeZone(String dateTimeString) {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateTimeString, formatter);
        return zonedDateTime.toInstant().toEpochMilli();
    }

    public static void main(String[] args) {
        long timestamp = TimeUtil.parseTimestamp(
                "2024-01-27 03:02:04",//.130667+08
                "yyyy-MM-dd hh:mm:ss",
                0);
        long t = parseTimestamp("2024-01-27 03:02:04.130667+08", 3);
        System.out.println(t);
        s(t);
        System.out.println(timestamp);
        s(timestamp);
    }

    public static void s(long l) {
        SimpleDateFormat srtFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        //日期转字符串
        Date date = new Date(l);
        String dateStr = srtFormat.format(date);
        System.out.println("日期转字符串：" + dateStr);
    }

    public static long parseTimestamp(String dateStr, int fix) {
        int fixValue = 1;
        for (int i = 0; i < fix; i++) {
            fixValue*=10;
        }
        String[] split = dateStr.split("\\.");
        if (split.length <= 0) split = new String[]{dateStr};
        long timestamp = TimeUtil.parseTimestamp(split[0], "yyyy-MM-dd hh:mm:ss", 0);
        if (split.length <= 1) return timestamp;
        String s1 = split[1];
        if (s1.contains("+")) {
            timestamp = parseTimestamp(s1.split("\\+"), timestamp, fixValue, true);
        } else if (s1.contains("-")) {
            timestamp = parseTimestamp(s1.split("\\-"), timestamp, fixValue, false);
        } else {
            long m = Long.parseLong(s1) / fixValue;
            timestamp+=m;
        }
        return timestamp;
    }

    private static long parseTimestamp(String[] split1, long timestamp, int fixValue, boolean add) {
        if (split1.length <= 0) return timestamp;
        long m = Long.parseLong(split1[0])/fixValue;
        timestamp+=m;
        if (split1.length <= 1) return timestamp;
        int h = Integer.parseInt(split1[1]);
        int value = h * 60 * 60 * 1000;
        return timestamp + ((add ? -1 : 1) * value);
    }
}
