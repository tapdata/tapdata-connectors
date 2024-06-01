package io.tapdata.connector.tidb.cdc.process.analyse;

import java.util.LinkedHashMap;

/**
 * @author GavinXiao
 * @description AnalyseRecord create by Gavin
 * @create 2023/7/14 9:59
 **/
public interface AnalyseRecord<V, T> {
    public T analyse(V record, LinkedHashMap<String, TableTypeEntity> tapTable, String tableId, CsvAnalyseFilter filter);
}
