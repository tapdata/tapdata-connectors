package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.Set;

public class FilterDouble extends Filter {
    private static FilterDouble instance;
    private FilterDouble() {

    }
    public static FilterDouble instance() {
        if (null == instance) {
            synchronized (FilterDouble.class) {
                if (null == instance) instance = new FilterDouble();
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
            case "DOUBLE":
                return true;
            default:
                if (item.contains("DOUBLE")) {
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
