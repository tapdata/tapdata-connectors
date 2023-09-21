package io.tapdata.tdd.tdd.tests.support.connector;

import java.util.UUID;

public class TSDBSupport implements TableNameSupport {
    @Override
    public String tableName() {
        return "AUTO_" + UUID.randomUUID().toString();
    }
}
