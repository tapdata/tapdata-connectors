package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.entity.logger.Log;

/**
 * @author GavinXiao
 * @description AnalyseRecord create by Gavin
 * @create 2023/7/14 9:59
 **/
public interface AnalyseRecord<V, T> {
    T analyse(V record, AnalyseColumnFilter<V> filter, Log log);
}
