package io.tapdata.it.ddl;

import io.tapdata.connector.mysql.pxc.MySQLPXCConnector;

public class MySQLPxcDdlIT extends MysqlFamilyDdlIT {
    public MySQLPxcDdlIT() {
        super(MySQLPXCConnector::new, "mysql-pxc-spec.json", "mysql", "mysql-pxc-connector");
    }
}
