package io.tapdata.connector.postgres;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PostgresJdbcContext extends JdbcContext {

    private final static String TAG = PostgresJdbcContext.class.getSimpleName();
    String postgresVersion;

    public PostgresJdbcContext(PostgresConfig config) {
        super(config);
        exceptionCollector = new PostgresExceptionCollector();
    }

    public PostgresJdbcContext withPostgresVersion(String postgresVersion) {
        this.postgresVersion = postgresVersion;
        return this;
    }

    /**
     * query version of database
     *
     * @return version description
     */
    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext("SHOW server_version_num", resultSet -> version.set(resultSet.getString(1)));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return version.get();
    }

    public TimeZone queryTimeZone() throws SQLException {
        AtomicReference<Long> timeOffset = new AtomicReference<>();
        queryWithNext(POSTGRES_TIMEZONE, resultSet -> timeOffset.set(resultSet.getLong(1)));
        DecimalFormat decimalFormat = new DecimalFormat("00");
        if (timeOffset.get() >= 0) {
            return TimeZone.getTimeZone(ZoneId.of("+" + decimalFormat.format(timeOffset.get()) + ":00"));
        } else {
            return TimeZone.getTimeZone(ZoneId.of(decimalFormat.format(timeOffset.get()) + ":00"));
        }
    }

    @Override
    public Long queryTimestamp() throws SQLException {
        AtomicReference<Timestamp> currentTime = new AtomicReference<>();
        queryWithNext(POSTGRES_CURRENT_TIME, resultSet -> currentTime.set(resultSet.getTimestamp(1)));
        return currentTime.get().getTime();
    }

    @Override
    protected String queryAllTablesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND t.table_name IN ('" + tableNames.stream().map(v -> StringKit.escape(v, "'")).collect(Collectors.joining("','")) + "')" : "";
        if (Integer.parseInt(postgresVersion) < 100000) {
            return String.format(PG_ALL_TABLE_LOWER_VERSION, StringKit.escape(getConfig().getDatabase(), "'"), StringKit.escape(schema, "'"), tableSql);
        }
        return String.format(PG_ALL_TABLE, StringKit.escape(getConfig().getDatabase(), "'"), StringKit.escape(schema, "'"), tableSql);
    }

    @Override
    protected String queryAllColumnsSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN ('" + tableNames.stream().map(v -> StringKit.escape(v, "'")).collect(Collectors.joining("','")) + "')" : "";
        if (Integer.parseInt(postgresVersion) < 100000) {
            return String.format(PG_ALL_COLUMN_LOWER_VERSION, StringKit.escape(schema, "'"), StringKit.escape(getConfig().getDatabase(), "'"), StringKit.escape(schema, "'"), tableSql);
        }
        return String.format(PG_ALL_COLUMN, StringKit.escape(schema, "'"), StringKit.escape(getConfig().getDatabase(), "'"), StringKit.escape(schema, "'"), tableSql);
    }

    @Override
    protected String queryAllIndexesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN ('" + tableNames.stream().map(v -> StringKit.escape(v, "'")).collect(Collectors.joining("','")) + "')" : "";
        return String.format(PG_ALL_INDEX, StringKit.escape(getConfig().getDatabase(), "'"), StringKit.escape(schema, "'"), tableSql);
    }

    @Override
    protected String queryAllForeignKeysSql(String schema, List<String> tableNames) {
        return String.format(PG_ALL_FOREIGN_KEY, StringKit.escape(getConfig().getSchema(), "'"), EmptyKit.isEmpty(tableNames) ? "" : " and pc.relname in ('" + tableNames.stream().map(v -> StringKit.escape(v, "'")).collect(Collectors.joining("','")) + "')");
    }

    public DataMap getTableInfo(String tableName) {
        DataMap dataMap = DataMap.create();
        List<String> list = new ArrayList<>();
        list.add("size");
        list.add("rowcount");
        try {
            query(String.format(TABLE_INFO_SQL, StringKit.escape(getConfig().getSchema(), "'"), StringKit.escape(tableName, "'"), StringKit.escape(getConfig().getDatabase(), "'"), StringKit.escape(getConfig().getSchema(), "'")), resultSet -> {
                while (resultSet.next()) {
                    dataMap.putAll(DbKit.getRowFromResultSet(resultSet, list));
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute getTableInfo failed, error: " + e.getMessage(), e);
        }
        return dataMap;
    }

    protected final static String PG_ALL_TABLE_LOWER_VERSION = "SELECT t.table_name \"tableName\",\n" +
            "       (select max(cast(obj_description(relfilenode, 'pg_class') as varchar)) as \"tableComment\"\n" +
            "        from pg_class c\n" +
            "        where relname = t.table_name)\n" +
            "FROM information_schema.tables t WHERE t.table_type='BASE TABLE' and t.table_catalog='%s' AND t.table_schema='%s' %s ORDER BY t.table_name";

    protected final static String PG_ALL_TABLE =
            "SELECT DISTINCT ON (t.table_name)\n" +
                    "    t.table_name AS \"tableName\",\n" +
                    "    (SELECT\n" +
                    "         COALESCE(\n" +
                    "                 CAST(obj_description(c.oid, 'pg_class') AS varchar),\n" +
                    "                 ''\n" +
                    "             )\n" +
                    "     FROM pg_class c\n" +
                    "     WHERE c.relname = t.table_name\n" +
                    "       AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = t.table_schema)\n" +
                    "    ) AS \"tableComment\",\n" +
                    "    CASE\n" +
                    "        WHEN EXISTS (SELECT 1 FROM pg_partitioned_table pt WHERE pt.partrelid = c.oid) THEN 'Parent Table'\n" +
                    "        WHEN EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid) THEN 'Child Table'\n" +
                    "        ELSE 'Regular Table'\n" +
                    "        END AS \"tableType\"\n" +
                    "FROM\n" +
                    "    information_schema.tables t\n" +
                    "        JOIN\n" +
                    "    pg_class c ON c.relname = t.table_name AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = t.table_schema)\n" +
                    "        LEFT JOIN\n" +
                    "    pg_inherits i ON i.inhrelid = c.oid\n" +
                    "WHERE\n" +
                    "        t.table_type = 'BASE TABLE'\n" +
                    "  AND t.table_catalog = '%s'\n" +
                    "  AND t.table_schema = '%s'\n %s " +
                    "ORDER BY\n" +
                    "    t.table_name,\n" +
                    "    CASE\n" +
                    "        WHEN EXISTS (SELECT 1 FROM pg_partitioned_table pt WHERE pt.partrelid = c.oid) \n" +
                    "            THEN c.oid\n" +
                    "            ELSE i.inhparent\n" +
                    "        END";

    protected final static String PG_ALL_COLUMN =
            "SELECT\n" +
                    "    col.table_name \"tableName\",\n" +
                    "    col.column_name \"columnName\",\n" +
                    "    col.data_type \"pureDataType\",\n" +
                    "    col.column_default \"columnDefault\",\n" +
                    "    col.is_nullable \"nullable\",\n" +
                    "    col.is_identity \"autoInc\",\n" +
                    "    col.identity_start AS \"seedValue\",\n" +
                    "    col.identity_increment AS \"incrementValue\",\n" +
                    "    seq.sequencename AS \"sequenceName\",\n" +
                    "    seq.start_value AS \"seedValue2\",\n" +
                    "    seq.increment_by AS \"incrementValue2\",\n" +
                    "       (SELECT seqcache\n" +
                    "        FROM pg_sequence\n" +
                    "        WHERE seqrelid = pg_get_serial_sequence('\"'||replace(col.table_schema, '\"', '\"\"')||'\".\"'||replace(col.table_name,'\"','\"\"')||'\"', col.column_name)::regclass) \"cacheValue\"," +
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
                    "LEFT JOIN pg_sequences seq\n" +
                    "    ON (seq.schemaname=col.table_schema and col.column_default=\n" +
                    "    'nextval('''||(case when schemaname='public' then '' else schemaname||'.' end)||sequencename||'''::regclass)')\n" +
                    "WHERE col.table_catalog = '%s'\n" +
                    "  AND col.table_schema = '%s' %s\n" +
                    "ORDER BY col.table_name, col.ordinal_position";

    protected final static String PG_ALL_COLUMN_LOWER_VERSION =
            "SELECT\n" +
                    "    col.table_name \"tableName\",\n" +
                    "    col.column_name \"columnName\",\n" +
                    "    col.data_type \"pureDataType\",\n" +
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
                    "WHERE col.table_catalog = '%s'\n" +
                    "  AND col.table_schema = '%s' %s\n" +
                    "ORDER BY col.table_name, col.ordinal_position";

    protected final static String PG_ALL_INDEX =
            "SELECT\n" +
                    "    t.relname AS \"tableName\",\n" +
                    "    i.relname AS \"indexName\",\n" +
                    "    a.attname AS \"columnName\",\n" +
                    "    (CASE WHEN ix.indisunique THEN '1' ELSE '0' END) AS \"isUnique\",\n" +
                    "    (CASE WHEN ix.indisreplident THEN '1' ELSE '0' END) AS \"isCoreUnique\",\n" +
                    "    (CASE WHEN ix.indisprimary THEN '1' ELSE '0' END) AS \"isPk\",\n" +
                    "    (CASE WHEN ix.indoption[row_number() over (partition by t.relname,i.relname order by a.attnum) - 1] & 1 = 0 THEN '1' ELSE '0' END) AS \"isAsc\"\n" +
                    "FROM\n" +
                    "    pg_class t,\n" +
                    "    pg_class i,\n" +
                    "    pg_index ix,\n" +
                    "    pg_attribute a,\n" +
                    "    information_schema.tables tt\n" +
                    "WHERE\n" +
                    "        t.oid = ix.indrelid\n" +
                    "  AND i.oid = ix.indexrelid\n" +
                    "  AND a.attrelid = t.oid\n" +
                    "  AND a.attnum = ANY(ix.indkey)\n" +
                    "  AND t.relkind IN ('r', 'p')\n" +
                    "  AND tt.table_name=t.relname\n" +
                    "  AND tt.table_schema=(select nspname from pg_namespace where oid=t.relnamespace)\n" +
                    "  AND tt.table_catalog='%s'\n" +
                    "  AND tt.table_schema='%s' %s\n" +
                    "ORDER BY t.relname, i.relname, a.attnum";

    protected final static String PG_ALL_FOREIGN_KEY = "select con.\"constraintName\",con.\"tableName\",con.\"referencesTableName\",con.\"onUpdate\",con.\"onDelete\",\n" +
            "(select column_name from information_schema.columns where table_schema=con.nspname and table_name=con.\"tableName\" and ordinal_position=con.fk) fk,\n" +
            "(select column_name from information_schema.columns where table_schema=con.nspname and table_name=con.\"referencesTableName\" and ordinal_position=con.rfk) rfk\n" +
            "from\n" +
            "(select c.conname \"constraintName\",pn.nspname,\n" +
            "pc.relname \"tableName\",\n" +
            "(select relname from pg_class where oid=c.confrelid) \"referencesTableName\",\n" +
            "unnest(conkey) fk, unnest(confkey) rfk,\n" +
            "(case c.confupdtype\n" +
            "    when 'a' then 'NO_ACTION'\n" +
            "    when 'n' then 'SET_NULL'\n" +
            "    when 'r' then 'RESTRICT'\n" +
            "    when 'c' then 'CASCADE' else 'SET_DEFAULT' end) \"onUpdate\",\n" +
            "(case c.confdeltype\n" +
            "    when 'a' then 'NO_ACTION'\n" +
            "    when 'n' then 'SET_NULL'\n" +
            "    when 'r' then 'RESTRICT'\n" +
            "    when 'c' then 'CASCADE' else 'SET_DEFAULT' end) \"onDelete\"\n" +
            "from pg_constraint c\n" +
            "join pg_class pc on c.conrelid = pc.oid\n" +
            "join pg_namespace pn on c.connamespace = pn.oid\n" +
            "where c.contype='f' and pn.nspname='%s' %s) con";

    protected final static String TABLE_INFO_SQL = "SELECT\n" +
            " pg_total_relation_size('\"' || table_schema || '\".\"' || table_name || '\"') AS size,\n" +
            " (select reltuples from pg_class pc, pg_namespace pn where pc.relname = t1.table_name and pc.relnamespace=pn.oid and pn.nspname='%s') as rowcount \n" +
            " FROM information_schema.tables t1 where t1.table_name ='%s' and t1.table_catalog='%s' and t1.table_schema='%s' ";

    private final static String POSTGRES_TIMEZONE = "select date_part('hour', now() - now() at time zone 'UTC')";

    private final static String POSTGRES_CURRENT_TIME = "SELECT NOW()";

    // ── DDL Trigger (Attunity-style event trigger) ──────────────────────

    /** DDL audit table. Stores intercepted DDL statements temporarily before Debezium captures them from WAL. */
    public static final String DDL_AUDIT_TABLE = "_tapdata_ddl_audit";

    /**
     * Quote a PostgreSQL identifier by wrapping it in double quotes and escaping
     * any embedded double quotes per SQL standard.
     */
    private static String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return "\"\"";
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /** Event trigger function name. */
    public static final String DDL_TRIGGER_FUNCTION = "_tapdata_intercept_ddl";

    /** Event trigger name. */
    public static final String DDL_EVENT_TRIGGER = "_tapdata_intercept_ddl";

    /**
     * Create the DDL audit table in the given schema.
     * <p>
     * Schema: c_key (bigserial PK), c_time (timestamp), c_user (varchar),
     * c_txn (varchar), c_tag (varchar), c_schema (varchar), c_ddlqry (text).
     */
    public static String buildCreateDdlAuditTableSql(String schema) {
        String s = quoteIdentifier(schema);
        return "CREATE TABLE IF NOT EXISTS " + s + "." + quoteIdentifier(DDL_AUDIT_TABLE) + " (\n" +
                "    c_key    BIGSERIAL PRIMARY KEY,\n" +
                "    c_time   TIMESTAMP DEFAULT now(),\n" +
                "    c_user   VARCHAR(64),\n" +
                "    c_txn    VARCHAR(16),\n" +
                "    c_tag    VARCHAR(24),\n" +
                "    c_schema VARCHAR(64),\n" +
                "    c_ddlqry TEXT\n" +
                ")";
    }

    /**
     * Create the event trigger function in the given schema.
     * <p>
     * Captures CREATE/ALTER/DROP TABLE (and other DDL) via {@code ddl_command_end},
     * writes the DDL statement to the audit table (which produces WAL for Debezium),
     * then immediately deletes it to keep the table clean.
     */
    public static String buildCreateDdlTriggerFunctionSql(String schema) {
        String s = quoteIdentifier(schema);
//        String tbl = quoteIdentifier(DDL_AUDIT_TABLE);
        return "CREATE OR REPLACE FUNCTION " + s + "." + quoteIdentifier(DDL_TRIGGER_FUNCTION) + "()\n" +
                "RETURNS event_trigger\n" +
                "    LANGUAGE plpgsql\n" +
                "AS $$\n" +
                "DECLARE\n" +
                "    _r   RECORD;\n" +
                "    _qry TEXT;\n" +
                "BEGIN\n" +
                "    BEGIN\n" +
                "        IF (tg_tag IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE',\n" +
                "                       'CREATE INDEX', 'DROP INDEX', 'ALTER INDEX',\n" +
                "                       'CREATE VIEW', 'DROP VIEW', 'ALTER VIEW',\n" +
                "                       'CREATE FUNCTION', 'DROP FUNCTION', 'ALTER FUNCTION',\n" +
                "                       'CREATE SCHEMA', 'DROP SCHEMA', 'ALTER SCHEMA',\n" +
                "                       'CREATE SEQUENCE', 'DROP SEQUENCE', 'ALTER SEQUENCE',\n" +
                "                       'CREATE TYPE', 'DROP TYPE', 'ALTER TYPE')) THEN\n" +
                "            _qry := current_query();\n" +
                "            FOR _r IN SELECT * FROM pg_event_trigger_ddl_commands()\n" +
                "            LOOP\n" +
                "                INSERT INTO \"public\".\"_tapdata_ddl_audit\"\n" +
                "                VALUES (DEFAULT,\n" +
                "                        current_timestamp,\n" +
                "                        current_user,\n" +
                "                        CAST(TXID_CURRENT() AS VARCHAR(16)),\n" +
                "                        _r.command_tag,\n" +
                "                        COALESCE(_r.schema_name, current_schema),\n" +
                "                        _qry);\n" +
                "            END LOOP;\n" +
                "        END IF;\n" +
                "    EXCEPTION WHEN OTHERS THEN\n" +
                "        RAISE WARNING 'DDL audit trigger failed for [%]: % (SQLSTATE: %)', tg_tag, SQLERRM, SQLSTATE;\n" +
                "    END;\n" +
                "END;\n" +
                "$$";
    }

    /**
     * Create the event trigger on {@code ddl_command_end}.
     * <p>
     * <b>Requires superuser privileges.</b>
     */
    public static String buildCreateDdlEventTriggerSql(String schema) {
        return "CREATE EVENT TRIGGER " + quoteIdentifier(DDL_EVENT_TRIGGER) + "\n" +
                "ON ddl_command_end\n" +
                "EXECUTE PROCEDURE " + quoteIdentifier(schema) + "." + quoteIdentifier(DDL_TRIGGER_FUNCTION) + "()";
    }

    /** Drop the event trigger. */
    public static String buildDropDdlEventTriggerSql() {
        return "DROP EVENT TRIGGER IF EXISTS " + quoteIdentifier(DDL_EVENT_TRIGGER);
    }

    /** Drop the event trigger function. */
    public static String buildDropDdlTriggerFunctionSql(String schema) {
        return "DROP FUNCTION IF EXISTS " + quoteIdentifier(schema) + "." + quoteIdentifier(DDL_TRIGGER_FUNCTION) + "()";
    }

    /** Drop the DDL audit table. */
    public static String buildDropDdlAuditTableSql(String schema) {
        return "DROP TABLE IF EXISTS " + quoteIdentifier(schema) + "." + quoteIdentifier(DDL_AUDIT_TABLE);
    }

    /** Check if the event trigger exists. */
    public static String buildCheckDdlEventTriggerSql() {
        return "SELECT COUNT(*) FROM pg_event_trigger WHERE evtname = " + quoteLiteral(DDL_EVENT_TRIGGER);
    }

    /**
     * Quote a string literal for safe use in a SQL statement.
     * Doubles embedded single quotes per SQL standard.
     */
    private static String quoteLiteral(String literal) {
        if (literal == null) return "''";
        return "'" + literal.replace("'", "''") + "'";
    }

}
