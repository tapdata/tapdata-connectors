package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.entity.logger.Log;

public abstract class EventFactory<T> {
    protected static final long DEFAULT_CACHE_TIME = 10L * 60L * 1000L;
    protected static final long DEFAULT_CACHE_COUNT = 1000L;

    public abstract void emit(T logEvent, Log log);
    public abstract void emit(T logEvent, Log log, String charset);

    protected abstract void process();

    protected abstract void accept();
}
