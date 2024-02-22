package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.entity.logger.Log;

public abstract class EventFactory<T> {
    long DEFAULT_CACHE_TIME = 10 * 60 * 1000;
    long DEFAULT_CACHE_COUNT = 1000;

    public abstract void emit(T logEvent, Log log);

    protected abstract void process();

    protected abstract void accept();
}
