package io.tapdata.connector.mysql.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;


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
        return new TapField(this.columnName, this.dataType)
                .nullable(this.isNullable()).autoInc(isAutoInc())
                .defaultValue(columnDefaultValue).comment(this.remarks)
                .autoIncStartValue(seedValue)
                .autoIncrementValue(incrementValue)
                .autoIncCacheValue(autoIncCacheValue);
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

}
