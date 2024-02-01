package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.Set;

class FilterInteger extends Filter {
    private static FilterInteger instance;
    private FilterInteger() {

    }
    public static FilterInteger instance() {
        if (null == instance) {
            synchronized (FilterInteger.class) {
                if (null == instance) instance = new FilterInteger();
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
     * DATE、TIME[WITHOUT TIME ZONE]、
     * TIMESTAMP[WITHOUT TIME ZONE]、
     *
     * CHAR(n)、VARCHAR(n)、TEXT
     * */
    @Override
    public boolean ifSupported(String typeName) {
        String item = typeName.toUpperCase();
        switch (item) {
            case "INTEGER":
                return true;
            default:
                if (item.contains("INTEGER")) {
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