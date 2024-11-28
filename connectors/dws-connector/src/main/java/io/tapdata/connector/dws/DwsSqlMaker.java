package io.tapdata.connector.dws;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.kit.EmptyKit;

public class DwsSqlMaker extends CommonSqlMaker {

    private Boolean closeNotNull;

    public DwsSqlMaker closeNotNull(Boolean closeNotNull) {
        this.closeNotNull = closeNotNull;
        return this;
    }

    protected void buildNullDefinition(StringBuilder builder, TapField tapField) {
        boolean nullable = !(EmptyKit.isNotNull(tapField.getNullable()) && !tapField.getNullable());
        String dataType = tapField.getDataType().toUpperCase();
        if (closeNotNull && (dataType.contains("CHAR") || dataType.contains("TEXT"))) {
            nullable = true;
        }
        if (!nullable || tapField.getPrimaryKey()) {
            builder.append("NOT NULL").append(' ');
        }
    }
}
