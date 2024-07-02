package io.tapdata.connector.tidb.cdc.process.analyse;

import java.util.Map;

/**
 * @author GavinXiao
 * @description CsvAnalyseFilter create by Gavin
 * @create 2023/8/9 15:22
 **/
public interface AnalyseColumnFilter<T> {
    boolean filter(Map<String, Object> before, Map<String, Object> after, T cdcInfo);
}