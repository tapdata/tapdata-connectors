package io.tapdata.connector.gauss.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimeUtil {
    public static Date parseDate(String dateStr, String format) {
        java.util.Date date = null;
        SimpleDateFormat srtFormat = new SimpleDateFormat(format);
        try {
            date = srtFormat.parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date;

//        try {
//            java.text.DateFormat df = new java.text.SimpleDateFormat(format);
//            String dt = dateStr.replaceAll("-", "/");
//            if ((!dt.equals(""))) {
//                dt += format.substring(dt.length()).replaceAll("[YyMmDdHhSs]", "0");
//            }
//            date = df.parse(dt);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return date;
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
        System.out.println(
                TimeUtil.parseTimestamp(
                        "2024-01-27 03:02:04.130667+08",
                        "yyyy-MM-dd hh:mm:ss.ssssss",
                        0));

            
    }
}
