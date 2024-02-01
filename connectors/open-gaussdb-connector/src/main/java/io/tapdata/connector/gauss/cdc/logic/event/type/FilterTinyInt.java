package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.Set;

public class FilterTinyInt extends Filter {
    private static FilterTinyInt instance;
    private FilterTinyInt() {

    }
    public static FilterTinyInt instance() {
        if (null == instance) {
            synchronized (FilterTinyInt.class) {
                if (null == instance) instance = new FilterTinyInt();
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
            case "TINYINT":
                return true;
            default:
                if (item.contains("TINYINT")) {
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
