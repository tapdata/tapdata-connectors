package io.tapdata.connector.gauss.cdc.logic.event.type;

import java.util.Set;

public class FilterLong extends Filter{
    private static FilterLong instance;
    private FilterLong() {

    }
    public static FilterLong instance() {
        if (null == instance) {
            synchronized (FilterLong.class) {
                if (null == instance) instance = new FilterLong();
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
            case "BIGINT":
                return true;
            default:
                if (item.contains("BIGINT")) {
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
