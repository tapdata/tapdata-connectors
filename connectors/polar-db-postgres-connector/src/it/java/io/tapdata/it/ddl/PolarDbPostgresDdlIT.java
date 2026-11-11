package io.tapdata.it.ddl;

import io.tapdata.connector.adb.PolarDBPostgresConnector;

public class PolarDbPostgresDdlIT extends PostgresFamilyDdlIT {
    public PolarDbPostgresDdlIT() {
        super(PolarDBPostgresConnector::new, "polar-db-postgres-spec.json", "polar-db-postgres-connector");
    }
}
