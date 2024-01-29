package io.tapdata.connector.gauss.cdc.logic.event;

public abstract class EventFactory<T> {
    long DEFAULT_CACHE_TIME = 10 * 60 * 1000;
    long DEFAULT_CACHE_COUNT = 1000;

    public abstract void emit(T logEvent);

    protected abstract void process();
}
