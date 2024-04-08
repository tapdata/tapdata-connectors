package io.tapdata.connector.doris;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

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
            buildDefaultDefinition(builder, tapField);
            buildNullDefinition(builder, tapField);
            buildCommentDefinition(builder, tapField);
            return builder.toString();
        }).collect(Collectors.joining(", "));
    }
}
