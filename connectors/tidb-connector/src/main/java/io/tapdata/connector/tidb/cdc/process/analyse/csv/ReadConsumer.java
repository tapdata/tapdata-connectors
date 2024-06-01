package io.tapdata.connector.tidb.cdc.process.analyse.csv;

public interface ReadConsumer<T> {
    void accept(T data);
}
