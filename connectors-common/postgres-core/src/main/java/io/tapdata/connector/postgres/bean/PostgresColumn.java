package io.tapdata.connector.postgres.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

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
        this.sequenceName = dataMap.getString("sequenceName");
        this.remarks = dataMap.getString("columnComment");
        this.columnDefaultValue = getDefaultValue(dataMap.getString("columnDefault"));
        if (EmptyKit.isNotNull(this.sequenceName)) {
            this.autoInc = "YES";
            this.seedValue = dataMap.getString("seedValue2");
            this.incrementValue = dataMap.getString("incrementValue2");
            this.columnDefaultValue = dataMap.getString("columnDefault");
        }
    }

    @Override
    public TapField getTapField() {
        TapField field = new TapField(this.columnName, this.dataType).pureDataType(this.pureDataType)
                .nullable(this.isNullable()).autoInc(isAutoInc())
                .defaultValue(columnDefaultValue).comment(this.remarks).sequenceName(sequenceName);
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
        }
        generateDefaultValue(field);
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

    protected String getDefaultValue(String defaultValue) {
        String res;
        if (EmptyKit.isNull(defaultValue) || defaultValue.startsWith("NULL::")) {
            return null;
        } else if (defaultValue.contains("::")) {
            res = defaultValue.substring(0, defaultValue.lastIndexOf("::"));
        } else {
            res = defaultValue;
        }
        if (res.startsWith("\"")) {
            res = StringKit.removeHeadTail(res, "\"", null);
        } else if (res.startsWith("'")) {
            res = StringKit.removeHeadTail(res, "'", null).replace("''", "'");
        } else {
            isString = false;
        }
        return res;
    }

    protected String parseDefaultFunction(String defaultValue) {
        return PostgresDefaultFunction.parseFunction(columnDefaultValue);
    }

    public enum PostgresDefaultFunction {
        _CURRENT_TIMESTAMP("CURRENT_TIMESTAMP"),
        _CURRENT_USER("CURRENT_USER"),
        _GENERATE_UUID("gen_random_uuid()");

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
