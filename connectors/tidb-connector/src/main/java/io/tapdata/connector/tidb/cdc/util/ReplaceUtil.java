package io.tapdata.connector.tidb.cdc.util;

public class ReplaceUtil {
    private ReplaceUtil(){}
    public static String replaceAll(String value, String replace, String to) {
        if (null == value) return null;
        while (value.contains(replace)) {
            value = value.replace(replace, to);
        }
        return value;
    }
}
