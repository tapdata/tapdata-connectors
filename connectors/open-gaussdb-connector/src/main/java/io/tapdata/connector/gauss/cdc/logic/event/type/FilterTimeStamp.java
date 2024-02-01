package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.Set;

public class FilterTimeStamp extends Filter {
    private static FilterTimeStamp instance;
    private FilterTimeStamp() {

    }
    public static FilterTimeStamp instance() {
        if (null == instance) {
            synchronized (FilterTimeStamp.class) {
                if (null == instance) instance = new FilterTimeStamp();
            }
        }
        return instance;
    }

    @Override
    public Object convert() {
        return null;
    }

    /**
     * INTEGER、BIGINT、SMALLINT、TINYINT、
     * SERIAL、SMALLSERIAL、BIGSERIAL、
     * FLOAT、DOUBLE PRECISION、
     *
     *
     * DATE、TIME[WITHOUT TIME ZONE]、
     * TIMESTAMP[WITHOUT TIME ZONE]、
     *
     * CHAR(n)、VARCHAR(n)、TEXT
     * */
    @Override
    public boolean ifSupported(String typeName) {
        String item = typeName.toUpperCase();
        switch (item) {
            case "TIMESTAMP[WITHOUT TIME ZONE]":
                return true;
            default:
                if (item.contains("TIMESTAMP")) {
                    return true;
                }
        }
        return false;
    }

    @Override
    public Set<String> supportedType() {
        return null;
    }
}
