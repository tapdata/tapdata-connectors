package io.tapdata.connector.gauss.core;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import org.junit.jupiter.api.*;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Disabled
public class GaussDBJdbcContextTest {
    GaussDBJdbcContext context;
    PostgresConfig config;
    @BeforeEach
    void init() {
        config = mock(PostgresConfig.class);
        context = mock(GaussDBJdbcContext.class);
    }

    @Test
    void testParams() {
        Assertions.assertEquals("SELECT\n" +
                "    col.table_name \"tableName\",\n" +
                "    col.column_name \"columnName\",\n" +
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
                "ORDER BY col.table_name, col.ordinal_position", GaussDBJdbcContext.GAUSS_ALL_COLUMN);
    }

    @Test
    void testInstance() {
        try (MockedStatic<GaussDBJdbcContext> gr = mockStatic(GaussDBJdbcContext.class);
             MockedConstruction<GaussDBJdbcContext> construction = mockConstruction(GaussDBJdbcContext.class)){
            gr.when(() -> GaussDBJdbcContext.instance(config)).thenCallRealMethod();
            Assertions.assertNotNull(GaussDBJdbcContext.instance(config));
        }
    }


    @Nested
    class QueryAllColumnsSqlTest {
        String schema;
        List<String> tableNames;
        String database;
        CommonDbConfig config;
        @BeforeEach
        void init() {
            database = "database";
            schema = "schema";
            tableNames = new ArrayList<>();
            config = mock(CommonDbConfig.class);

            when(context.getConfig()).thenReturn(config);
            when(config.getDatabase()).thenReturn(database);
            when(context.queryAllColumnsSql(anyString(), anyList())).thenCallRealMethod();
        }

        @Test
        void testEmptyTables() {
            String columnsSql = context.queryAllColumnsSql(schema, tableNames);
            Assertions.assertEquals("SELECT\n" +
                    "    col.table_name \"tableName\",\n" +
                    "    col.column_name \"columnName\",\n" +
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
                    "               WHERE cl.relname = col.table_name and cl.relnamespace=(select oid from pg_namespace where nspname='schema'))) AS \"dataType\"\n" +
                    "FROM information_schema.columns col\n" +
                    "   left join pg_type pt on pt.typname = col.udt_name\n" +
                    "WHERE col.table_catalog = 'database'\n" +
                    "  AND col.table_schema = 'schema' \n" +
                    "ORDER BY col.table_name, col.ordinal_position", columnsSql);
            verify(context, times(1)).getConfig();
            verify(config, times(1)).getDatabase();
        }

        @Test
        void testNotEmptyTables() {
            tableNames.add("table");
            String columnsSql = context.queryAllColumnsSql(schema, tableNames);
            Assertions.assertEquals("SELECT\n" +
                    "    col.table_name \"tableName\",\n" +
                    "    col.column_name \"columnName\",\n" +
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
                    "               WHERE cl.relname = col.table_name and cl.relnamespace=(select oid from pg_namespace where nspname='schema'))) AS \"dataType\"\n" +
                    "FROM information_schema.columns col\n" +
                    "   left join pg_type pt on pt.typname = col.udt_name\n" +
                    "WHERE col.table_catalog = 'database'\n" +
                    "  AND col.table_schema = 'schema' AND table_name IN ('table')\n" +
                    "ORDER BY col.table_name, col.ordinal_position", columnsSql);
            verify(context, times(1)).getConfig();
            verify(config, times(1)).getDatabase();
        }
    }
}
