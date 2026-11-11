package io.tapdata.it.ddl;

import io.tapdata.connector.rds.AWSRDSMySQLConnector;

public class AwsRdsMySQLDdlIT extends MysqlFamilyDdlIT {
    public AwsRdsMySQLDdlIT() {
        super(AWSRDSMySQLConnector::new, "aws-rds-mysql-spec.json", "mysql", "aws-rds-mysql-connector");
    }
}
