package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.connector.tidb.cdc.process.ddl.convert.Convert;

/**
 * @author GavinXiao
 * @description DefaultConvert create by Gavin
 * @create 2024/6/3 13:00
 **/
public class DefaultConvert implements TiDBDataTypeConvert {

    @Override
    public Object convert(Object fromValue, Convert convert) {
        if (null == fromValue) return null;
        if (null == convert) return fromValue;
        return convert.convert(fromValue);
    }
}
