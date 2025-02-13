package io.tapdata.connector.postgres.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jarad
 * @date 2022/4/20
 */
public class PostgresColumn extends CommonColumn {

    public PostgresColumn() {

    }

    public PostgresColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("columnName");
        this.dataType = dataMap.getString("dataType").replaceAll("\\[]", " array"); //'dataType' with precision and scale (postgres has its function)
//        this.dataType = dataMap.getString("data_type"); //'data_type' without precision or scale
        this.pureDataType = dataMap.getString("pureDataType");
        if ("USER-DEFINED".equals(this.pureDataType)) {
            this.dataType = StringKit.removeParentheses(this.dataType);
            this.pureDataType = this.dataType;
        }
        this.nullable = dataMap.getString("nullable");
        this.autoInc = dataMap.getString("autoInc");
        this.seedValue = dataMap.getString("seedValue");
        this.incrementValue = dataMap.getString("incrementValue");
        this.autoIncCacheValue = dataMap.getString("cacheValue");
        this.remarks = dataMap.getString("columnComment");
        //create table in target has no need to set default value
//        this.columnDefaultValue = null;
        this.columnDefaultValue = getDefaultValue(dataMap.getString("columnDefault"));
    }

    @Override
    public TapField getTapField() {
        TapField field = new TapField(this.columnName, this.dataType).pureDataType(this.pureDataType)
                .nullable(this.isNullable()).autoInc(isAutoInc())
                .defaultValue(columnDefaultValue).comment(this.remarks);
        if (isAutoInc()) {
            if (EmptyKit.isNotNull(seedValue)) {
                field.autoIncStartValue(Long.parseLong(seedValue));
            }
            if (EmptyKit.isNotNull(incrementValue)) {
                field.autoIncrementValue(Long.parseLong(incrementValue));
            }
            if (EmptyKit.isNotNull(autoIncCacheValue)) {
                field.setAutoIncCacheValue(Long.parseLong(autoIncCacheValue));
            }
            Object defaultValueObj = columnDefaultValue;
            String tapDefaultFunction = null;
            if (EmptyKit.isNotNull(columnDefaultValue)) {
                tapDefaultFunction = PostgresDefaultFunction.parseFunction(columnDefaultValue);
                if (columnDefaultValue.matches("-?\\d+(\\.\\d+)?")) {
                    defaultValueObj = new BigDecimal(columnDefaultValue);
                } else {
                    defaultValueObj = columnDefaultValue;
                }
            }
            field.defaultValue(defaultValueObj).defaultFunction(tapDefaultFunction);
        }
        return field;
    }

    @Override
    protected Boolean isNullable() {
        return "YES".equals(this.nullable);
    }

    @Override
    protected Boolean isAutoInc() {
        return "YES".equals(this.autoInc);
    }

    private String getDefaultValue(String defaultValue) {
        if (EmptyKit.isNull(defaultValue) || defaultValue.startsWith("NULL::")) {
            return null;
        } else if (defaultValue.contains("::")) {
            return defaultValue.substring(0, defaultValue.lastIndexOf("::"));
        } else {
            return defaultValue;
        }
    }

    public enum PostgresDefaultFunction {
        _CURRENT_TIMESTAMP("current_timestamp"),
        _CURRENT_USER("current_user");

        private final String function;
        private static final Map<String, String> map = new HashMap<>();

        static {
            for (PostgresDefaultFunction value : PostgresDefaultFunction.values()) {
                map.put(value.function, value.name());
            }
        }

        PostgresDefaultFunction(String function) {
            this.function = function;
        }

        public static String parseFunction(String key) {
            if (map.containsKey(key)) {
                return map.get(key);
            }
            return null;
        }

        public String getFunction() {
            return function;
        }
    }
}
