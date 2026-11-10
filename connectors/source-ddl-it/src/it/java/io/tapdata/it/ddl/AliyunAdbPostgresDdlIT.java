package io.tapdata.it.ddl;

import io.tapdata.connector.adb.AliyunADBPostgresConnector;

public class AliyunAdbPostgresDdlIT extends PostgresFamilyDdlIT {
    public AliyunAdbPostgresDdlIT() {
        super(AliyunADBPostgresConnector::new, "aliyun-adb-postgres-spec.json", "aliyun-adb-postgres-connector");
    }
}
