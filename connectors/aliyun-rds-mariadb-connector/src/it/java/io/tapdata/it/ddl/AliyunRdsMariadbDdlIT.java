package io.tapdata.it.ddl;

import io.tapdata.connector.rds.AliyunRDSMariaDBConnector;

public class AliyunRdsMariadbDdlIT extends MysqlFamilyDdlIT {
    public AliyunRdsMariadbDdlIT() {
        super(AliyunRDSMariaDBConnector::new, "aliyun-rds-mariadb-spec.json", "mariadb12", "aliyun-rds-mariadb-connector");
    }
}
