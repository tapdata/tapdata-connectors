package io.tapdata.connector.hudi.write;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class HuDiSqlMarker extends CommonSqlMaker {
    public HuDiSqlMarker(char escapeChar) {
       super(escapeChar);
    }
    public String buildColumnDefinition(TapTable tapTable, boolean needComment) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        return nameFieldMap.entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(v -> { //pos may be null
            StringBuilder builder = new StringBuilder();
            TapField tapField = v.getValue();
            //ignore those which has no dataType
            if (tapField.getDataType() == null) {
                return "";
            }
            builder.append(getEscapeChar())
                    .append(tapField.getName())
                    .append(getEscapeChar()).append(' ')
                    .append(tapField.getDataType())
                    .append(' ');
            buildDefaultDefinition(builder, tapField);
            buildNullDefinition(builder, tapField);
            if (needComment) {
                buildCommentDefinition(builder, tapField);
            }
            return builder.toString();
        }).collect(Collectors.joining(", "));
    }
}
