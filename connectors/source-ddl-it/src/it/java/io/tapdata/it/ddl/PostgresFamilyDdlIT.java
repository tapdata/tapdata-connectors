package io.tapdata.it.ddl;

import io.tapdata.connector.postgres.PostgresConnector;
import io.tapdata.it.verifier.ConnectorVerifier;
import io.tapdata.it.verifier.JdbcVerifier;

import java.util.List;
import java.util.function.Supplier;

public abstract class PostgresFamilyDdlIT extends JdbcFamilyDdlIT {
    protected PostgresFamilyDdlIT(Supplier<? extends PostgresConnector> factory, String specification, String module) {
        super(factory, specification, "postgresql", module);
    }

    @Override
    protected boolean postgresDialect() {
        return true;
    }

    @Override
    protected ConnectorVerifier createVerifier() {
        JdbcVerifier jdbc = (JdbcVerifier) super.createVerifier();
        return new JdbcVerifier(jdbc.jdbcContext()) {
            @Override
            public String identifierMode() { return "postgres-quoted"; }
            @Override
            protected String qualifiedTable(String table) { return qualified(table); }
            @Override
            protected String qualifiedColumn(String column) { return quote(column); }
            @Override
            protected String schemaPattern() { return "public"; }
        };
    }

    @Override
    protected List<String> sourceDdlStatements(String table) {
        return List.of("ALTER TABLE " + qualified(table) + " ADD COLUMN c_ddl_probe VARCHAR(64)",
                "ALTER TABLE " + qualified(table) + " RENAME COLUMN c_ddl_probe TO c_ddl_probe_new",
                "ALTER TABLE " + qualified(table) + " DROP COLUMN c_ddl_probe_new");
    }

    @Override
    protected String sourceCompositeAddColumnsStatement(String table) {
        return "ALTER TABLE " + qualified(table)
                + " ADD COLUMN c_composite_a VARCHAR(64), ADD COLUMN c_composite_b BIGINT";
    }

    @Override
    protected String sourceDdlInterleaveDropFieldStatement(String table) {
        return "ALTER TABLE " + qualified(table) + " DROP COLUMN c_ddl_probe";
    }

    private static String qualified(String table) {
        return "\"public\"." + quote(table);
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
