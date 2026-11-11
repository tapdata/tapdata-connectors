package io.tapdata.it.ddl;

import io.tapdata.connector.mariadb.MariadbConnector;

public class MariadbDdlIT extends MysqlFamilyDdlIT {
    public MariadbDdlIT() {
        super(MariadbConnector::new, "spec_mariadb.json", "mariadb12", "mariadb-connector");
    }
}
