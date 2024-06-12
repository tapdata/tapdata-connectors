package io.tapdata.connector.gauss.core;

import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;

public class GaussDBExceptionCollector extends PostgresExceptionCollector {
    public static GaussDBExceptionCollector instance() {
        return new GaussDBExceptionCollector();
    }
}
