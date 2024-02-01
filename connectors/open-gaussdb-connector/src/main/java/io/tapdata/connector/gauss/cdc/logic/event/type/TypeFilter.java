package io.tapdata.connector.gauss.cdc.logic.event.type;


import java.util.HashMap;
import java.util.Map;

public class TypeFilter {
    private Map<String, Filter> typeIdMap = new HashMap<>();
    private static TypeFilter filter;
    private TypeFilter() {}
    public static TypeFilter init() {
        if (null == filter) {
            synchronized (TypeFilter.class) {
                if (null == filter) {
                    filter = new TypeFilter();
                }
            }
        }
        return filter;
    }
}

/**
 * INTEGER、BIGINT、SMALLINT、TINYINT、
 * SERIAL、SMALLSERIAL、BIGSERIAL、FLOAT、
 * DOUBLE PRECISION、DATE、TIME[WITHOUT TIME ZONE]、
 * TIMESTAMP[WITHOUT TIME ZONE]、CHAR(n)、VARCHAR(n)、TEXT
 * */