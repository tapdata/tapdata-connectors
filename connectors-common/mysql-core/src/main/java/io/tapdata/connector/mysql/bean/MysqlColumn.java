package io.tapdata.connector.mysql.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;


/**
 * @author jarad
 */
public class MysqlColumn extends CommonColumn {

    private String version;
    private Long seedValue;
    private Long incrementValue;
    private Long autoIncCacheValue;

    public MysqlColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("columnName");
        this.dataType = dataMap.getString("dataType");
        this.nullable = dataMap.getString("nullable");
        this.remarks = dataMap.getString("columnComment");
        this.columnDefaultValue = dataMap.getString("columnDefault");
        this.autoInc = dataMap.getString("autoInc");
    }

    @Override
    public TapField getTapField() {
        TapField field = new TapField(this.columnName, this.dataType)
                .nullable(this.isNullable()).autoInc(isAutoInc())
                .defaultValue(columnDefaultValue).comment(this.remarks);
        if (isAutoInc()) {
            field.autoIncStartValue(seedValue)
                    .autoIncrementValue(incrementValue)
                    .autoIncCacheValue(autoIncCacheValue);
        }
        Object defaultValueObj = columnDefaultValue;
        String tapDefaultFunction = null;
        if (EmptyKit.isNotNull(columnDefaultValue)) {
            tapDefaultFunction = MysqlDefaultFunction.parseFunction(columnDefaultValue);
            if (columnDefaultValue.matches("-?\\d+(\\.\\d+)?")) {
                defaultValueObj = new BigDecimal(columnDefaultValue);
            } else {
                defaultValueObj = columnDefaultValue;
            }
        }
        field.defaultValue(defaultValueObj).defaultFunction(tapDefaultFunction);
        return field;
    }

    public MysqlColumn withVersion(String version) {
        this.version = version;
        return this;
    }

    public MysqlColumn withSeedValue(Long seedValue) {
        this.seedValue = seedValue;
        return this;
    }

    public MysqlColumn withIncrementValue(Long incrementValue) {
        this.incrementValue = incrementValue;
        return this;
    }

    public MysqlColumn withAutoIncCacheValue(Long autoIncCacheValue) {
        this.autoIncCacheValue = autoIncCacheValue;
        return this;
    }

    @Override
    protected Boolean isNullable() {
        if (EmptyKit.isNotNull(version) && "5.6".compareTo(version) > 0) {
            return true;
        }
        return "YES".equals(this.nullable);
    }

    public enum MysqlDefaultFunction {
        _CURRENT_TIMESTAMP("CURRENT_TIMESTAMP"),
        _GENERATE_UUID("uuid()");

        private final String function;
        private static final Map<String, String> map = new HashMap<>();

        static {
            for (MysqlDefaultFunction value : MysqlDefaultFunction.values()) {
                map.put(value.function, value.name());
            }
        }

        MysqlDefaultFunction(String function) {
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
