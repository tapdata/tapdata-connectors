package io.tapdata.it.ddl;

import io.tapdata.connector.rds.AliyunRDSPostgresConnector;

public class AliyunRdsPostgresDdlIT extends PostgresFamilyDdlIT {
    public AliyunRdsPostgresDdlIT() {
        super(AliyunRDSPostgresConnector::new, "aliyun-rds-postgres-spec.json", "aliyun-rds-postgres-connector");
    }
}
