package io.tapdata.connector.postgres;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.Collate;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.tapdata.pdk.apis.entity.SortOn.ASCENDING;

public class PostgresSqlMaker extends CommonSqlMaker {

    private Boolean closeNotNull;
    private Boolean createAutoInc = false;
    private Long autoIncStartValue = 1000000L;
    private char escapeChar = '"';

    public PostgresSqlMaker closeNotNull(Boolean closeNotNull) {
        this.closeNotNull = closeNotNull;
        return this;
    }

    public PostgresSqlMaker createAutoInc(Boolean createAutoInc) {
        this.createAutoInc = createAutoInc;
        return this;
    }

    public PostgresSqlMaker autoIncStartValue(long autoIncStartValue) {
        this.autoIncStartValue = autoIncStartValue;
        return this;
    }

    protected void buildNullDefinition(StringBuilder builder, TapField tapField) {
        boolean nullable = !(EmptyKit.isNotNull(tapField.getNullable()) && !tapField.getNullable());
        if (closeNotNull) {
            nullable = true;
        }
        if (!nullable || tapField.getPrimaryKey()) {
            builder.append("NOT NULL").append(' ');
        }
    }

    protected void buildAutoIncDefinition(StringBuilder builder, TapField tapField) {
        if (!Boolean.TRUE.equals(createAutoInc)) {
            return;
        }
        long startValue;
        if (EmptyKit.isNotNull(autoIncStartValue)) {
            startValue = autoIncStartValue;
        } else if (EmptyKit.isNotNull(tapField.getAutoIncStartValue())) {
            startValue = tapField.getAutoIncStartValue();
        } else {
            startValue = 1;
        }
        builder.append("GENERATED BY DEFAULT AS IDENTITY (START WITH ")
                .append(startValue)
                .append(" INCREMENT BY 1) ");
    }

    @Override
    public void buildWhereClause(StringBuilder builder, TapAdvanceFilter filter) {
        if (EmptyKit.isNotEmpty(filter.getMatch()) || EmptyKit.isNotEmpty(filter.getOperators())) {
            builder.append(" WHERE ");
            builder.append(buildKeyAndValue(filter.getMatch(), "AND", "=", filter.getCollateList()));
        }
        if (EmptyKit.isNotEmpty(filter.getOperators())) {
            if (EmptyKit.isNotEmpty(filter.getMatch())) {
                builder.append("AND ");
            }
            builder.append(filter.getOperators().stream().map(v -> queryOperatorToString(v, String.valueOf(escapeChar))).collect(Collectors.joining(" AND "))).append(' ');
        }
    }

    public String buildKeyAndValue(Map<String, Object> record, String splitSymbol, String operator, List<Collate> collateList) {
        StringBuilder builder = new StringBuilder();
        if (EmptyKit.isNotEmpty(record)) {
            record.forEach((fieldName, value) -> {
                builder.append(escapeChar).append(fieldName).append(escapeChar).append(operator);
                builder.append(buildValueString(value));
                Collate collate = EmptyKit.isEmpty(collateList) ? null : collateList.stream()
                        .filter(c -> c.getFieldName().equals(fieldName))
                        .findFirst()
                        .orElse(null);
                if (null != collate) {
                    builder.append(' ').append(buildCollate(collate.getCollateName()));
                }
                builder.append(' ').append(splitSymbol).append(' ');
            });
            builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        }
        return builder.toString();
    }

    public void buildOrderClause(StringBuilder builder, TapAdvanceFilter filter) {
        if (EmptyKit.isNotEmpty(filter.getSortOnList())) {
            builder.append("ORDER BY ");
            List<Collate> collateList = filter.getCollateList();
            builder.append(filter.getSortOnList().stream().map(v -> {
                Collate collate = null;
                if (EmptyKit.isNotEmpty(collateList)) {
                    collate = collateList.stream()
                            .filter(c -> c.getFieldName().equals(v.getKey()))
                            .findFirst()
                            .orElse(null);
                }
                if (null != collate) {
                    return getOrderByFieldClauseWithCollate(v, collate);
                } else {
                    return v.toString(String.valueOf(escapeChar));
                }
            }).collect(Collectors.joining(", "))).append(' ');
        }
    }

    protected String getOrderByFieldClauseWithCollate(SortOn sortOn, Collate collate) {
        StringBuilder sb = new StringBuilder();
        sb.append(escapeChar).append(sortOn.getKey()).append(escapeChar);
        sb.append(' ').append(buildCollate(collate.getCollateName())).append(' ');
        sb.append((sortOn.getSort() == ASCENDING ? "ASC" : "DESC"));
        return sb.toString();
    }

    protected String buildCollate(String collateName) {
        return COLLATE + ' ' + escapeChar + collateName + escapeChar;
    }

}
