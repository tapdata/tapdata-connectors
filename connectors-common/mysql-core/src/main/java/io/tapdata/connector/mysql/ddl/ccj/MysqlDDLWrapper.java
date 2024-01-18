package io.tapdata.connector.mysql.ddl.ccj;

import io.tapdata.common.ddl.alias.DbDataTypeAlias;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;

public abstract class MysqlDDLWrapper extends CCJBaseDDLWrapper {

    @Override
    protected String getDataTypeFromAlias(String alias) {
        return new DbDataTypeAlias(alias).toDataType();
    }

    protected Object getDefaultValueForMysql(String dataType) {
        if (dataType.contains("char") || dataType.contains("text") || dataType.contains("blob")) {
            return "";
        } else if ("json".equals(dataType)) {
            return "null";
        } else if (dataType.contains("binary")) {
            return '\u0000';
        } else if (dataType.contains("bit")) {
            return 0;
        } else if (dataType.contains("int")) {
            return 0;
        } else if (dataType.startsWith("float")) {
            return 0.0;
        } else if (dataType.startsWith("double")) {
            return 0.0;
        } else if (dataType.startsWith("decimal")) {
            return 0.0;
        } else if (dataType.startsWith("date")) {
            return "1970-01-01";
        } else if (dataType.startsWith("time")) {
            return "00:00:00";
        } else if (dataType.startsWith("year")) {
            return 1970;
        } else if (dataType.startsWith("timestamp")) {
            return "1970-01-01 00:00:00";
        } else if (dataType.contains("datetime")) {
            return "1970-01-01 00:00:00";
        } else {
            return null;
        }
    }
}
