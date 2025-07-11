package io.tapdata.connector.doris;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.connector.mysql.bean.MysqlColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.util.*;
import java.util.stream.Collectors;

public class DorisSqlMaker extends CommonSqlMaker {

    public DorisSqlMaker() {
        super('`');
    }

    public String buildColumnDefinitionByOrder(TapTable tapTable, Collection<String> keyOrdered, boolean aggregate) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        List<String> keyOrderedFields = new ArrayList<>(keyOrdered);
        keyOrderedFields.addAll(nameFieldMap.entrySet().stream().filter(v -> !keyOrdered.contains(v.getKey())).sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList()));
        return keyOrderedFields.stream().map(v -> { //pos may be null
            StringBuilder builder = new StringBuilder();
            TapField tapField = nameFieldMap.get(v);
            //ignore those which has no dataType
            if (tapField.getDataType() == null) {
                return "";
            }
            builder.append(getEscapeChar()).append(tapField.getName()).append(getEscapeChar()).append(' ').append(tapField.getDataType()).append(' ');
            if (aggregate && Boolean.FALSE.equals(tapField.getPrimaryKey())) {
                builder.append("REPLACE_IF_NOT_NULL ");
            }
            buildNullDefinition(builder, tapField);
            if (Boolean.TRUE.equals(applyDefault)) {
                buildDefaultDefinition(builder, tapField);
            }
            buildCommentDefinition(builder, tapField);
            return builder.toString();
        }).collect(Collectors.joining(", "));
    }

    protected void buildDefaultDefinition(StringBuilder builder, TapField tapField) {
        if (EmptyKit.isNotNull(tapField.getDefaultValue())) {
            builder.append("DEFAULT").append(' ');
            if (EmptyKit.isNotNull(tapField.getDefaultFunction())) {
                String function = MysqlColumn.MysqlDefaultFunction.valueOf(tapField.getDefaultFunction().toString()).getFunction();
                if (function.endsWith("()")) {
                    builder.append("(").append(function).append(") ");
                } else {
                    builder.append(function).append(' ');
                }
            } else if (tapField.getDefaultValue() instanceof Number) {
                builder.append(tapField.getDefaultValue()).append(' ');
            } else {
                if ("json".equals(tapField.getDataType()) || "longtext".equals(tapField.getDataType())) {
                    builder.append("(").append(tapField.getDefaultValue().toString().replace("\\", "")).append(") ");
                } else {
                    builder.append("'").append(StringKit.escape(tapField.getDefaultValue().toString(), "'")).append("' ");
                }
            }
        } else if (Boolean.TRUE.equals(tapField.getNullable()) && "timestamp".equals(tapField.getDataType())) {
            builder.append("DEFAULT NULL ");
        }
    }
}
