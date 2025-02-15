package io.tapdata.common;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

/**
 * attributes for common columns
 *
 * @author Jarad
 * @date 2022/4/20
 */
public class CommonColumn {

    protected String columnName;
    protected String dataType;
    protected String nullable;
    protected String remarks;
    protected String columnDefaultValue;
    protected String autoInc;
    protected String seedValue;
    protected String incrementValue;
    protected String autoIncCacheValue;
    protected String pureDataType;
    protected Integer dataLength;
    protected Integer dataPrecision;
    protected Integer dataScale;

    public CommonColumn() {
    }

    public CommonColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("columnName");
        this.dataType = dataMap.getString("dataType");
        this.pureDataType = dataMap.getString("dataType");
        this.dataLength = dataMap.getInteger("dataLength");
        this.dataPrecision = dataMap.getInteger("dataPrecision");
        this.dataScale = dataMap.getInteger("dataScale");
        this.nullable = dataMap.getString("nullable");
        this.remarks = dataMap.getString("columnComment");
        this.columnDefaultValue = getDefaultValue(dataMap.getString("columnDefault"));
    }

    protected Boolean isNullable() {
        return "1".equals(this.nullable);
    }

    protected Boolean isAutoInc() {
        return "1".equals(this.autoInc);
    }

    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }

    protected String getDefaultValue(String defaultValue) {
        return null;
    }
}
