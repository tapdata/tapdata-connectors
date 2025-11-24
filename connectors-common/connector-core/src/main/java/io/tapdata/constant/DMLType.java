package io.tapdata.constant;



import org.apache.commons.lang3.StringUtils;

public enum DMLType {
    INSERT,UPDATE,DELETE,UN_KNOW;

    public static DMLType parse(String type) {
        if (StringUtils.isBlank(type)) return UN_KNOW;
        type = type.toUpperCase();
        for (DMLType value : values()) {
            if (value.name().equals(type)) return value;
        }
        return UN_KNOW;
    }
}
