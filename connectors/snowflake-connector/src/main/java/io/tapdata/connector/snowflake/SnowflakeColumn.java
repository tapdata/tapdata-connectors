package io.tapdata.connector.snowflake;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.StringKit;

public class SnowflakeColumn extends CommonColumn {

    public SnowflakeColumn(DataMap dataMap) {
        super(dataMap);
        this.dataScale = this.dataScale == null ? dataMap.getInteger("dataFraction") : this.dataScale;
        this.dataType = getDataType(dataMap); //'dataType' with precision and scale (oracle has no function)
    }

    @Override
    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType)
                .pureDataType(StringKit.removeParentheses(this.pureDataType))
                .length(this.dataLength)
                .precision(this.dataPrecision)
                .scale(this.dataScale)
                .nullable(this.isNullable())
                .defaultValue(columnDefaultValue)
                .comment(this.remarks);
    }

    @Override
    protected Boolean isNullable() {
        return "YES".equals(this.nullable);
    }

    private String getDataType(DataMap dataMap) {
        String dataType = dataMap.getString("dataType");
        String dataLength = dataMap.getString("dataLength");
        String dataPrecision = dataMap.getString("dataPrecision");
        if (dataType.contains("(")) {
            return dataType;
        } else {
            switch (dataType) {
                case "TEXT":
                    if (dataLength == null) {
                        return dataType;
                    }
                    return dataType + "(" + dataLength + ")";
                case "NUMBER":
                    if (dataScale == null) {
                        return "NUMBER";
                    } else {
                        return "NUMBER(" + dataPrecision + "," + dataScale + ")";
                    }
                case "TIME":
                case "TIMESTAMP_NTZ":
                case "TIMESTAMP_TZ":
                case "TIMESTAMP_LTZ":
                    if (dataScale == null) {
                        return dataType;
                    }
                    return dataType + "(" + dataScale + ")";
                default:
                    return dataType;
            }
        }
    }
}
