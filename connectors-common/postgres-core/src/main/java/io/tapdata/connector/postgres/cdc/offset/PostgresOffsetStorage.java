package io.tapdata.connector.postgres.cdc.offset;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PostgresOffsetStorage {
    public static final Map<String, PostgresOffset> DBZ_OFFSET = new ConcurrentHashMap<>();
}
