package io.tapdata.connector.dws;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.kit.EmptyKit;

public class DwsSqlMaker extends CommonSqlMaker {

    protected void buildNullDefinition(StringBuilder builder, TapField tapField) {
        boolean nullable = !(EmptyKit.isNotNull(tapField.getNullable()) && !tapField.getNullable());
        String dataType = tapField.getDataType().toUpperCase();
        if (closeNotNull && (dataType.contains("CHAR") || dataType.contains("TEXT"))) {
            nullable = true;
        }
        if (!nullable || (null != tapField.getPrimaryKeyPos() && tapField.getPrimaryKeyPos() > 0)) {
            builder.append("NOT NULL").append(' ');
        }
    }
}
