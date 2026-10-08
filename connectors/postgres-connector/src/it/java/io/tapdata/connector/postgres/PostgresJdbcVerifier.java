package io.tapdata.connector.postgres;

import io.tapdata.it.verifier.JdbcVerifier;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

final class PostgresJdbcVerifier extends JdbcVerifier {

    private final String schema;

    PostgresJdbcVerifier(Object jdbcContext, String schema) {
        super(jdbcContext);
        this.schema = schema == null || schema.isEmpty() ? "public" : schema;
    }

    @Override
    public String identifierMode() {
        return "postgres-quoted";
    }

    @Override
    protected String qualifiedTable(String table) {
        return quote(schema) + "." + quote(table);
    }

    @Override
    protected String qualifiedColumn(String column) {
        return quote(column);
    }

    @Override
    protected String schemaPattern() {
        return schema;
    }

    @Override
    public List<String> listIndexes(String table) throws Exception {
        List<String> indexes = new ArrayList<>();
        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT indexname FROM pg_indexes WHERE schemaname = ? AND tablename = ?")) {
            statement.setString(1, schema);
            statement.setString(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    indexes.add(resultSet.getString(1));
                }
            }
        }
        return indexes;
    }

    @Override
    public List<String> listConstraints(String table) throws Exception {
        List<String> constraints = new ArrayList<>();
        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT constraint_name FROM information_schema.table_constraints "
                             + "WHERE table_schema = ? AND table_name = ?")) {
            statement.setString(1, schema);
            statement.setString(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    constraints.add(resultSet.getString(1));
                }
            }
        }
        return constraints;
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
