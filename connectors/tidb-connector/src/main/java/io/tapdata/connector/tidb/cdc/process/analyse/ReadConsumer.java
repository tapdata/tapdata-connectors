package io.tapdata.connector.tidb.cdc.process.analyse;

public interface ReadConsumer<T> {
    void accept(T data);
}
