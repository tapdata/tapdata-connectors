package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.Set;

public class FilterTime extends Filter {
    private static FilterTime instance;
    private FilterTime() {

    }
    public static FilterTime instance() {
        if (null == instance) {
            synchronized (FilterTime.class) {
                if (null == instance) instance = new FilterTime();
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
            case "TIME[WITHOUT TIME ZONE]":
                return true;
            default:
                if (item.contains("TIME") && !item.contains("TIMESTAMP")) {
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
