package io.tapdata.connector.tidb.cdc.process.analyse;

public interface AnalyseEventFilter<T> {
    boolean filter(T cdcInfo);
}
