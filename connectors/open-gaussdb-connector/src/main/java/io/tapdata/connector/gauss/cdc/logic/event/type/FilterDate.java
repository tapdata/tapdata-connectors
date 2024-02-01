package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.HashSet;
import java.util.Set;

class FilterDate extends Filter {
    private static FilterDate instance;
    private FilterDate() {

    }
    public static FilterDate instance() {
        if (null == instance) {
            synchronized (FilterDate.class) {
                if (null == instance) instance = new FilterDate();
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
            case "DATE":
                return true;
            default:
                if (item.contains("DATE")) {
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