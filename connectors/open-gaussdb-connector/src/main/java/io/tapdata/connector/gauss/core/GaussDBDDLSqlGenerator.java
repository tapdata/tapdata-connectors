package io.tapdata.connector.gauss.core;

import io.tapdata.connector.postgres.ddl.PostgresDDLSqlGenerator;

public class GaussDBDDLSqlGenerator extends PostgresDDLSqlGenerator {
    public static GaussDBDDLSqlGenerator instance() {
        return new GaussDBDDLSqlGenerator();
    }
}
