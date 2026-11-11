package io.tapdata.it.ddl;

import io.tapdata.connector.polar.db.mysql.PolarDBMySQLConnector;

public class PolarDbMySQLDdlIT extends MysqlFamilyDdlIT {
    public PolarDbMySQLDdlIT() {
        super(PolarDBMySQLConnector::new, "polar-db-mysql-spec.json", "mysql", "polar-db-mysql-connector");
    }
}
