package io.tapdata.constant;

import java.util.Collection;
import java.util.Map;

public enum JsonType {
    STRING,
    NUMBER,
    BOOLEAN,
    NULL,
    OBJECT,
    ARRAY,
    TEXT,
    INTEGER,
    ;

    public static JsonType of(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Map) {
            return JsonType.OBJECT;
        }
        if ((obj instanceof Collection) || obj.getClass().isArray()) {
            return JsonType.ARRAY;
        }
        if (obj instanceof Number) {
            if (obj instanceof Integer) {
                return JsonType.INTEGER;
            }
            return JsonType.NUMBER;
        }
        if (obj instanceof Boolean) {
            return JsonType.BOOLEAN;
        }
        if (obj instanceof String) {
            if (String.valueOf(obj).length() > 200) {
                return JsonType.TEXT;
            }
            return JsonType.STRING;
        }
        return null;
    }
    public static JsonType of(String obj) {
        if (obj == null) {
            return null;
        }
        if ("Map".equalsIgnoreCase(obj)) {
            return JsonType.OBJECT;
        }
        if ("Collection".equalsIgnoreCase(obj) || "Array".equalsIgnoreCase(obj)) {
            return JsonType.ARRAY;
        }
        if ("Number".equalsIgnoreCase(obj)) {
            return JsonType.NUMBER;
        }
        if ("Boolean".equalsIgnoreCase(obj)) {
            return JsonType.BOOLEAN;
        }
        if ("String".equalsIgnoreCase(obj)) {
            return JsonType.STRING;
        }
        if ("Integer".equalsIgnoreCase(obj)) {
            return JsonType.INTEGER;
        }
        if ("Test".equalsIgnoreCase(obj)) {
            return JsonType.TEXT;
        }
        return null;
    }

}
