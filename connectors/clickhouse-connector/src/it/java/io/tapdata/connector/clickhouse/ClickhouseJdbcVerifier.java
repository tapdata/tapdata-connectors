package io.tapdata.connector.clickhouse;

import io.tapdata.it.schema.TestFieldSpec;
import io.tapdata.it.verifier.JdbcVerifier;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

final class ClickhouseJdbcVerifier extends JdbcVerifier {

    ClickhouseJdbcVerifier(Object jdbcContext) {
        super(jdbcContext);
    }

    @Override
    public void createTable(String table, List<TestFieldSpec> fields) throws Exception {
        StringBuilder sql = new StringBuilder("CREATE TABLE ").append(qualifiedTable(table)).append(" (");
        String orderColumn = null;
        for (int i = 0; i < fields.size(); i++) {
            TestFieldSpec field = fields.get(i);
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(qualifiedColumn(field.getName())).append(' ').append(field.getDataType());
            if (orderColumn == null && field.isPrimaryKey()) {
                orderColumn = qualifiedColumn(field.getName());
            }
        }
        sql.append(") ENGINE = MergeTree ORDER BY ")
                .append(orderColumn == null ? "tuple()" : orderColumn);

        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute(sql.toString());
        }
    }
}
