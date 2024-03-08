package io.tapdata.connector.gauss.core;

import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

import java.util.Optional;

public class GaussColumn extends PostgresColumn {
    protected int columnTypeOid;

    @Override
    public TapField getTapField() {
        if (null == columnName) return null;
        if (null == dataType) return null;
        String remarksTemp = "Type oid[" + columnTypeOid + "] " + (null == this.remarks ? "" : this.remarks);
        return new TapField(LogicUtil.replaceAll(this.columnName, "\"",""),
                LogicUtil.replaceAll(this.dataType.toUpperCase(),"\"",""))
                .nullable(this.isNullable())
                .defaultValue(columnDefaultValue)
                .comment(remarksTemp);
    }

    public GaussColumn init(DataMap dataMap) {
        this.columnName = dataMap.getString("columnName");
        String dataType = dataMap.getString("dataType");
        if (null != dataType) {
            this.dataType = LogicUtil.replaceAll(dataType,"[]", " array"); //'dataType' with precision and scale (postgres has its function)
        }
        this.nullable = dataMap.getString("nullable");
        this.remarks = dataMap.getString("columnComment");
        //create table in target has no need to set default value
        this.columnDefaultValue = null;
        try {
            this.columnTypeOid = dataMap.getInteger("fieldTypeOid");
        } catch (Exception e) {
            this.columnTypeOid = 0;
        }
        return this;
    }

    public static GaussColumn instance() {
        return new GaussColumn();
    }
}
