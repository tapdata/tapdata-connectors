package io.tapdata.connector.gauss.core;

import io.tapdata.connector.postgres.PostgresSqlMaker;

public class GaussDBSqlMaker extends PostgresSqlMaker {
    public static GaussDBSqlMaker instance() {
        return new GaussDBSqlMaker();
    }
}
