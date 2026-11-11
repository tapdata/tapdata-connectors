package io.tapdata.it.ddl;

import io.tapdata.connector.adb.TencentDBPostgresConnector;

public class TencentDbPostgresDdlIT extends PostgresFamilyDdlIT {
    public TencentDbPostgresDdlIT() {
        super(TencentDBPostgresConnector::new, "tencent-db-postgres-spec.json", "tencent-db-postgres-connector");
    }
}
