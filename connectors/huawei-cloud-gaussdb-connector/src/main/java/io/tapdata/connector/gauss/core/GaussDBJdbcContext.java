package io.tapdata.connector.gauss.core;

import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.util.List;

public class GaussDBJdbcContext extends PostgresJdbcContext {
    protected final static String GAUSS_ALL_COLUMN =
            "SELECT\n" +
                    "    col.table_name \"tableName\",\n" +
                    "    col.column_name \"columnName\",\n" +
                    "    col.data_type \"pureDataType\",\n" +
                    "    pt.oid as \"fieldTypeOid\",\n" +
                    "    col.column_default \"columnDefault\",\n" +
                    "    col.is_nullable \"nullable\",\n" +
                    "       (SELECT max(d.description)\n" +
                    "        FROM pg_catalog.pg_class c,\n" +
                    "             pg_description d\n" +
                    "        WHERE c.relname = col.table_name\n" +
                    "          AND d.objoid = c.oid\n" +
                    "          AND d.objsubid = col.ordinal_position) AS \"columnComment\",\n" +
                    "       (SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)\n" +
                    "        FROM pg_catalog.pg_attribute a\n" +
                    "        WHERE a.attnum > 0\n" +
                    "          AND a.attname = col.column_name\n" +
                    "          AND NOT a.attisdropped\n" +
                    "          AND a.attrelid =\n" +
                    "              (SELECT max(cl.oid)\n" +
                    "               FROM pg_catalog.pg_class cl\n" +
                    "               WHERE cl.relname = col.table_name and cl.relnamespace=(select oid from pg_namespace where nspname='%s'))) AS \"dataType\"\n" +
                    "FROM information_schema.columns col\n" +
                    "   left join pg_type pt on pt.typname = col.udt_name\n" +
                    "WHERE col.table_catalog = '%s'\n" +
                    "  AND col.table_schema = '%s' %s\n" +
                    "ORDER BY col.table_name, col.ordinal_position";
    public GaussDBJdbcContext(PostgresConfig config) {
        super(config);
    }

    public static GaussDBJdbcContext instance(PostgresConfig config) {
        return new GaussDBJdbcContext(config);
    }

    @Override
    protected String queryAllColumnsSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(GAUSS_ALL_COLUMN, schema, getConfig().getDatabase(), schema, tableSql);
    }
}
