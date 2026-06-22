package io.tapdata.connector.postgres.ddl.alias;

import io.tapdata.common.ddl.alias.DbDataTypeAlias;

public class PostgresDataTypeAlias extends DbDataTypeAlias {

    public PostgresDataTypeAlias(String alias) {
        super(alias);
    }

    @Override
    protected String toChar() {
        return "character";
    }

    @Override
    protected String toVarchar() {
        return "character varying";
    }

}
