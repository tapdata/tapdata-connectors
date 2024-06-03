package io.tapdata.connector.tidb.cdc.process.analyse;

/**
 * @author GavinXiao
 * @description AnalyseRecord create by Gavin
 * @create 2023/7/14 9:59
 **/
public interface AnalyseRecord<V, T> {
    public T analyse(V record, AnalyseColumnFilter<V> filter);
}
