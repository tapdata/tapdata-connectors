package io.tapdata.connector.starrocks.ddl;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.ddl.DDLSqlGenerator;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StarrocksDDLSqlGenerator implements DDLSqlGenerator {

    protected final static String ALTER_TABLE_PREFIX = "alter table `%s`.`%s`";

    @Override
    public List<String> addColumn(CommonDbConfig config, TapNewFieldEvent tapNewFieldEvent) {
        if (null == tapNewFieldEvent) {
            return null;
        }
        List<TapField> newFields = tapNewFieldEvent.getNewFields();
        if (null == newFields) {
            return null;
        }
        String tableId = tapNewFieldEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append add column ddl sql failed, table name is blank");
        }
        StringBuilder totalSql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, config.getDatabase(), tableId)).append(" add column (");
        totalSql.append(newFields.stream().map(v -> {
            StringBuilder sql = new StringBuilder();
            String fieldName = v.getName();
            if (StringUtils.isNotBlank(fieldName)) {
                sql.append(" `").append(fieldName).append("`");
            } else {
                throw new RuntimeException("Append add column ddl sql failed, field name is blank");
            }
            String dataType = v.getDataType();
            if (StringUtils.isNotBlank(dataType)) {
                sql.append(" ").append(dataType);
            } else {
                throw new RuntimeException("Append add column ddl sql failed, data type is blank");
            }
            Boolean nullable = v.getNullable();
            if (null != nullable) {
                if (nullable) {
                    sql.append(" null");
                } else {
                    sql.append(" not null");
                }
            }
            Object defaultValue = v.getDefaultValue();
            if (null != defaultValue) {
                sql.append(" default '").append(defaultValue).append("'");
            }
            String comment = v.getComment();
            if (StringUtils.isNotBlank(comment)) {
                sql.append(" comment '").append(comment).append("'");
            }
            Boolean primaryKey = v.getPrimaryKey();
            if (null != primaryKey && primaryKey) {
                sql.append(" key");
            }
            return sql.toString();
        }).collect(Collectors.joining(",")));
        totalSql.append(")");
        return Collections.singletonList(totalSql.toString());
    }

    @Override
    public List<String> alterColumnAttr(CommonDbConfig config, TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
        if (null == tapAlterFieldAttributesEvent) {
            return null;
        }
        String tableId = tapAlterFieldAttributesEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append alter column attr ddl sql failed, table name is blank");
        }
        StringBuilder sql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, config.getDatabase(), tableId)).append(" modify column ");
        String fieldName = tapAlterFieldAttributesEvent.getFieldName();
        if (StringUtils.isNotBlank(fieldName)) {
            sql.append(" `").append(fieldName).append("`");
        } else {
            throw new RuntimeException("Append alter column attr ddl sql failed, field name is blank");
        }
        ValueChange<String> dataTypeChange = tapAlterFieldAttributesEvent.getDataTypeChange();
        if (StringUtils.isNotBlank(dataTypeChange.getAfter())) {
            String dataTypeChangeAfter = dataTypeChange.getAfter();
            sql.append(" ").append(dataTypeChangeAfter);
        } else {
            throw new RuntimeException("Append alter column attr ddl sql failed, data type is blank");
        }
        ValueChange<Boolean> nullableChange = tapAlterFieldAttributesEvent.getNullableChange();
        if (null != nullableChange && null != nullableChange.getAfter()) {
            if (nullableChange.getAfter()) {
                sql.append(" null");
            } else {
                sql.append(" not null");
            }
        }
        ValueChange<String> commentChange = tapAlterFieldAttributesEvent.getCommentChange();
        if (null != commentChange && StringUtils.isNotBlank(commentChange.getAfter())) {
            sql.append(" comment '").append(commentChange.getAfter()).append("'");
        }
        ValueChange<Integer> primaryChange = tapAlterFieldAttributesEvent.getPrimaryChange();
        if (null != primaryChange && null != primaryChange.getAfter() && primaryChange.getAfter() > 0) {
            sql.append(" key");
        }
        return Collections.singletonList(sql.toString());
    }

    @Override
    public List<String> alterColumnName(CommonDbConfig config, TapAlterFieldNameEvent tapAlterFieldNameEvent) {
        if (null == tapAlterFieldNameEvent) {
            return null;
        }
        String tableId = tapAlterFieldNameEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append alter column name ddl sql failed, table name is blank");
        }
        ValueChange<String> nameChange = tapAlterFieldNameEvent.getNameChange();
        if (null == nameChange) {
            throw new RuntimeException("Append alter column name ddl sql failed, change name object is null");
        }
        String before = nameChange.getBefore();
        String after = nameChange.getAfter();
        if (StringUtils.isBlank(before)) {
            throw new RuntimeException("Append alter column name ddl sql failed, old column name is blank");
        }
        if (StringUtils.isBlank(after)) {
            throw new RuntimeException("Append alter column name ddl sql failed, new column name is blank");
        }
        String sql = String.format(ALTER_TABLE_PREFIX, config.getDatabase(), tableId);
        return Collections.singletonList(sql + " rename column `" + before + "` to `" + after + "`");
    }

    @Override
    public List<String> dropColumn(CommonDbConfig config, TapDropFieldEvent tapDropFieldEvent) {
        if (null == tapDropFieldEvent) {
            return null;
        }
        String tableId = tapDropFieldEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append drop column ddl sql failed, table name is blank");
        }
        String fieldName = tapDropFieldEvent.getFieldName();
        if (StringUtils.isBlank(fieldName)) {
            throw new RuntimeException("Append drop column ddl sql failed, field name is blank");
        }
        return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, config.getDatabase(), tableId) + " drop column `" + fieldName + "`");
    }
}
