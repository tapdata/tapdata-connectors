package io.tapdata.connector.gauss.core;

import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

public class GaussColumn extends PostgresColumn {
    private int columnTypeOid;
    public GaussColumn() {
        super();
    }
    public GaussColumn(DataMap dataMap) {
        super(dataMap);
        this.columnTypeOid = dataMap.getInteger("fieldTypeOid");
    }

    @Override
    public TapField getTapField() {
        String remarksTemp = "Type oid[" + columnTypeOid + "] " + (null == this.remarks ? "" : this.remarks);
        return new TapField(this.columnName.replaceAll("\"",""),
                this.dataType.toUpperCase().replaceAll("\"",""))
                .nullable(this.isNullable())
                .defaultValue(columnDefaultValue)
                .comment(remarksTemp);
    }
}
