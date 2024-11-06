package io.tapdata.connector.postgres.partition.wrappper;

import io.tapdata.connector.postgres.partition.TableType;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartitionField;
import io.tapdata.entity.schema.partition.type.TapPartitionStage;
import io.tapdata.entity.schema.partition.type.TapPartitionType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class PGPartitionWrapper {
    public static final String REGEX = "\\(([^)]+)\\)";
    public static final String SPLIT_REGEX = "\\s*,\\s*";
    private static final Pattern VALID_COLUMN_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");


    protected PGPartitionWrapper() {

    }

    public abstract TapPartitionStage type();

    public List<TapPartitionField> partitionFields(TapTable table, String checkOrPartitionRule, Log log) {
        List<TapField> tapFields = matchFieldNames(table, checkOrPartitionRule);
        if (null != checkOrPartitionRule && !"".equals(checkOrPartitionRule.trim()) && tapFields.isEmpty()) {
            throw new CoreException("Can not support invalid partition filed stage: {}, table: {}", checkOrPartitionRule, table.getId());
        }
        List<TapPartitionField> fieldList = new ArrayList<>();
        for (TapField field : tapFields) {
            if (null == field) continue;
            TapPartitionField tapPartitionField = new TapPartitionField();
            tapPartitionField.setName(field.getName());
            tapPartitionField.setDataType(field.getDataType());
            tapPartitionField.setNullable(field.getNullable());
            tapPartitionField.setDefaultValue(field.getDefaultValue());
            tapPartitionField.setComment(field.getComment());
            tapPartitionField.setPos(field.getPos());
            tapPartitionField.setPrimaryKey(field.getPrimaryKey());
            tapPartitionField.setPrimaryKeyPos(field.getPrimaryKeyPos());
            fieldList.add(tapPartitionField);
        }
        return fieldList;
    }

    public abstract List<TapPartitionType> parse(TapTable table, String partitionSQL, String checkOrPartitionRule, Log log);

    public static TapPartitionStage type(String partitionType, String tableName, Log log) {
        PGPartitionWrapper instance = instance(partitionType);
        if (Objects.nonNull(instance)) {
            return instance.type();
        } else {
            log.warn("UnKnow partition type: {}, table name: {}", partitionType, tableName);
            return null;
        }
    }

    public static List<TapPartitionField> partitionFields(TapTable table, String partitionType, String checkOrPartitionRule, String tableName, Log log) {
        PGPartitionWrapper instance = instance(partitionType);
        if (Objects.nonNull(instance)) {
            return instance.partitionFields(table, checkOrPartitionRule, log);
        } else {
            log.warn("UnKnow partition type: {}, table name: {}", partitionType, tableName);
            return null;
        }
    }

    public static List<TapPartitionType> warp(TapTable table, String partitionType, String checkOrPartitionRule, String partitionSQL, Log log) {
        PGPartitionWrapper instance = instance(partitionType);
        if (Objects.nonNull(instance)) {
            return instance.parse(table, partitionSQL, checkOrPartitionRule, log);
        } else {
            log.warn("UnKnow partition type: {}", partitionType);
            return null;
        }
    }

    protected static PGPartitionWrapper instance(String partitionType) {
        switch (partitionType) {
            case TableType.RANGE:
                return new RangeWrapper();
            case TableType.LIST:
                return new ListWrapper();
            case TableType.HASH:
                return new HashWrapper();
            case TableType.INHERIT:
                return new InheritWrapper();
            default:
                return null;
        }
    }

    public String[] matchValues(String check) {
        Pattern pattern = Pattern.compile(REGEX);
        Matcher matcher = pattern.matcher(check);
        if (matcher.find()) {
            String fields = matcher.group(1);
            return fields.split(SPLIT_REGEX);
        }
        return null;
    }

    public String[] matchFieldNames(String check) {
        String[] matchValues = matchValues(check);
        if (null != matchValues) {
            return matchValues;
        } else {
            throw new CoreException("Unable to get partition fields from {} (partition field setting sql is invalid or not be support), only support simple partition stage,such as: (field_name) or  (field1,field2,...)", check);
        }
    }

    public List<TapField> matchFieldNames(TapTable table, String check) {
        String[] fieldArr = matchFieldNames(check);
        List<TapField> fieldList = new ArrayList<>();
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        for (String field : fieldArr) {
            if (!isValidColumnName(field)) {
                throw new CoreException("Unable to get partition fields from {} (partition field setting sql is invalid or not be support), only support simple partition stage,such as: (field_name) or  (field1,field2,...)", check);
            }
            Optional.ofNullable(nameFieldMap.get(field)).ifPresent(fieldList::add);
        }
        return fieldList;
    }

    boolean isValidColumnName(String columnName) {
        if (columnName == null || columnName.length() == 0 || columnName.length() > 63) {
            return false;
        }
        return VALID_COLUMN_NAME_PATTERN.matcher(columnName).matches();
    }
}
