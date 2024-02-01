package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.Set;

public class FilterSmallInt extends Filter {
    private static FilterSmallInt instance;
    private FilterSmallInt() {

    }
    public static FilterSmallInt instance() {
        if (null == instance) {
            synchronized (FilterSmallInt.class) {
                if (null == instance) instance = new FilterSmallInt();
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
            case "SMALLINT":
                return true;
            default:
                if (item.contains("SMALLINT")) {
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