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
    BINARY,
    ;

    public static JsonType of(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Map) {
            return JsonType.OBJECT;
        }
        if ((obj instanceof Collection) || obj.getClass().isArray()) {
            if (obj instanceof byte[]) {
                return BINARY;
            }
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

}
