package io.tapdata.connector.mysql.dml.sqlmaker;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.kit.EmptyKit;

import java.util.Map;

public class MysqlSqlMaker extends CommonSqlMaker {

    public MysqlSqlMaker(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    public String buildKeyAndValue(Map<String, Object> record, String splitSymbol, String operator) {
        StringBuilder builder = new StringBuilder();
        if (EmptyKit.isNotEmpty(record)) {
            record.forEach((fieldName, value) -> {
                if (value instanceof Double) {
                    builder.append("CAST(").append(fieldName).append(" AS decimal(10,6))").append("<=>");
                    builder.append(value);
                } else {
                    builder.append(escapeChar).append(fieldName).append(escapeChar).append("<=>");
                    builder.append(buildValueString(value));
                }
                builder.append(' ').append(splitSymbol).append(' ');
            });
            builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        }
        return builder.toString();
    }
}
