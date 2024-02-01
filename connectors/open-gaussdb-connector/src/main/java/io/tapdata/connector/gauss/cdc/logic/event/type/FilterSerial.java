package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.Set;

public class FilterSerial extends Filter {
    private static FilterSerial instance;
    private FilterSerial() {

    }
    public static FilterSerial instance() {
        if (null == instance) {
            synchronized (FilterSerial.class) {
                if (null == instance) instance = new FilterSerial();
            }
        }
        return instance;
    }
    @Override
    public Object convert() {
        return null;
    }

    /**
     * INTEGER、
     * BIGINT、
     * SMALLINT、
     * TINYINT、
     *
     * SERIAL、SMALLSERIAL、BIGSERIAL、
     *
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
            case "SERIAL":
                return true;
            default:
                if (item.contains("SERIAL")) {
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
