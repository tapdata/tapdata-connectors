package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.connector.tidb.cdc.process.ddl.convert.Convert;

/**
 * @author GavinXiao
 * @description TiDBDataTypeConvert create by Gavin
 * @create 2024/6/3 13:00
 **/
public interface TiDBDataTypeConvert {
    public Object convert(Object fromValue, Convert convert);
}
