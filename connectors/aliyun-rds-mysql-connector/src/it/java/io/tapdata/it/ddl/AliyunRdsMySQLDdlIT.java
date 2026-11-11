package io.tapdata.it.ddl;

import io.tapdata.connector.rds.AliyunRDSMySQLConnector;

public class AliyunRdsMySQLDdlIT extends MysqlFamilyDdlIT {
    public AliyunRdsMySQLDdlIT() {
        super(AliyunRDSMySQLConnector::new, "aliyun-rds-mysql-spec.json", "mysql", "aliyun-rds-mysql-connector");
    }
}
