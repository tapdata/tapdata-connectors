package io.tapdata.quickapi.utils;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/5 09:53 Create
 * @description 根据输入的模型结果生成表模型：
 * input:
 * {
 *     [{
 *      "tableName": "",
 *      "fields": {'fieldName': 'my_field', 'fieldType': 'string','primaryKey': true,'comment': 'my comment'}
 *     }]
 * }
 *
 * output: TapTable
 */
public class SchemaParser {
    public static final String TYPE_MATCH = "^string$|^integer$|^long$|^double$|^date$|^object$|^array$|^boolean$|^datetime(\\()?([0-9](?:\\))|(?:\\)))?|^timestamp(\\()?([0-9](?:\\))|(?:\\)))?|^time(\\()?([0-9](?:\\))|(?:\\)))?";
    private SchemaParser() {

    }

    static TapTable parseOnce(Table schema, Log logger) {
        TapTable tapTable = new TapTable(schema.getTableName(), schema.getTableName());
        List<Field> fields = schema.getFields();
        if (null == fields || fields.isEmpty()) {
            logger.warn("Table {} fields is empty", schema.getTableName());
            return tapTable;
        }
        int keyIndex = 0;
        for (Field field : fields) {
            if (null == field || StringUtils.isBlank(field.getFieldName())) {
                logger.warn("Table {} field is null", schema.getTableName());
                continue;
            }
            TapField tapField = new TapField(field.getFieldName().trim(), field.getFieldType());
            String fieldType = checkType(field.getFieldType(), logger);
            if (null == fieldType) {
                logger.warn("Table {}'s field {}'s type is not supported, invalid type: {}", schema.getTableName(), field.getFieldName(), field.getFieldType());
                continue;
            }
            tapField.dataType(fieldType);
            if (null != field.isPrimaryKey()) {
                tapField.setPrimaryKey(field.isPrimaryKey());
                tapField.primaryKeyPos(keyIndex);
                keyIndex++;
            }
            if (null != field.isNullable()) {
                tapField.setNullable(field.isNullable());
            }
            Optional.ofNullable(field.getComment()).ifPresent(tapField::comment);
            tapTable.add(tapField);
        }
        return tapTable;
    }

    static String checkType(String type, Log logger) {
        if (null == type) {
            logger.warn("Field type is null");
            return null;
        }
        String typeStr = type.toLowerCase().trim();
        return typeStr.matches(TYPE_MATCH) ? type : null;
    }

    public static List<TapTable> parse(String schema, Log logger) {
        List<TapTable> tables = new ArrayList<>();
        try {
            List<?> tableJsons = TapSimplify.fromJsonArray(schema);
            for (int index = 0; index < tableJsons.size(); index++) {
                try {
                    String json = TapSimplify.toJson(tableJsons.get(index));
                    Table table = TapSimplify.fromJson(json, Table.class);
                    TapTable tapTable = parseOnce(table, logger);
                    tables.add(tapTable);
                } catch (Exception e) {
                    logger.warn("Parse table failed, The {} table has an exception and cannot be converted, all table info are: {}, msg: {}", (index + 1), schema, e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.warn("Parse table failed, table info: {}, msg: {}", schema, e.getMessage());
        }
        return tables;
    }


    public static class Table {
        private String tableName;
        private List<Field> fields;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public List<Field> getFields() {
            return fields;
        }

        public void setFields(List<Field> fields) {
            this.fields = fields;
        }
    }
    public static class Field {
        private String fieldName;
        private String fieldType;
        private Boolean primaryKey;
        private String comment;
        private Boolean nullable;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldType() {
            return fieldType;
        }

        public void setFieldType(String fieldType) {
            this.fieldType = fieldType;
        }

        public Boolean isPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(Boolean primaryKey) {
            this.primaryKey = primaryKey;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public Boolean isNullable() {
            return nullable;
        }

        public void setNullable(Boolean nullable) {
            this.nullable = nullable;
        }
    }
}
