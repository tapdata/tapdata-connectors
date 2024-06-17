package io.tapdata.connector.gauss.entity;

import io.tapdata.connector.gauss.GaussDBTest;

public interface TestAccept {
    void accept(GaussDBTest test) throws Throwable;
}