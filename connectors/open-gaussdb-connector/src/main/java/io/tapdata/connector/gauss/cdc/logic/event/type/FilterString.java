package io.tapdata.connector.gauss.cdc.logic.event.type;
import java.util.Set;

class FilterString extends Filter {
    private static FilterString instance;
    private FilterString() {

    }
    public static FilterString instance() {
        if (null == instance) {
            synchronized (FilterString.class) {
                if (null == instance) instance = new FilterString();
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
     * DATE、TIME[WITHOUT TIME ZONE]、
     * TIMESTAMP[WITHOUT TIME ZONE]、
     *
     * CHAR(n)、VARCHAR(n)、TEXT
     * */
    @Override
    public boolean ifSupported(String typeName) {
        String item = typeName.toUpperCase();
        switch (item) {
            case "TEXT":
                return true;
            default:
                if (item.contains("CHAR")) {
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