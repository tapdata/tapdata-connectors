package io.tapdata.it.ddl;

import io.tapdata.connector.tencent.db.mariadb.TencentDBMariaDBConnector;

public class TencentDbMariadbDdlIT extends MysqlFamilyDdlIT {
    public TencentDbMariadbDdlIT() {
        super(TencentDBMariaDBConnector::new, "tencent-db-mariadb-spec.json", "mariadb12", "tencent-db-mariadb-connector");
    }
}
