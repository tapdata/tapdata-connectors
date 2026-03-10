package io.tapdata.connector.klustron.context;

import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/4 14:31 Create
 * @description
 */
public class KunLunPgContext extends PostgresJdbcContext {

    private final static String TAG = PostgresJdbcContext.class.getSimpleName();
    String postgresVersion;

    public KunLunPgContext(PostgresConfig config) {
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


    protected String mateSql(String schema, String mateView) {
        return String.format(StringUtils.isBlank(mateView) ? PG_MAT_VIEW_QUERY : (PG_MAT_VIEW_QUERY + " and t2.relname=" + StringKit.escape(mateView, "'")), schema);
    }

    public Map<String, Map<String, List<String>>> mateViewIndex(String schema, String mateView) throws SQLException {
        Map<String, Map<String, List<String>>> indexMap = new HashMap<>();
        query(mateSql(schema, mateView), resultSet -> {
            while (resultSet.next()) {
                indexMap.put(resultSet.getString("mv_name"), indexOfView(mateView, resultSet.getString("idxes"), resultSet.getString("rels")));
            }
        });
        return indexMap;
    }

    private Map<String, List<String>> indexOfView(String mateView, String indexOId, String relOId) throws SQLException {
        if (null == indexOId || null == relOId) {
            throw new SQLException("Failed to get index oid and rel oid of materialized view " + mateView);
        }
        final String[] indexOIds = indexOId.split(" ");
        final String[] relOIds = relOId.split(" ");
        if (indexOIds.length != relOIds.length) {
            throw new SQLException("Index oid and index rel oid mismatch, indexOIds: " + indexOId + ", relOIds: " + relOId);
        }
        StringJoiner inSql = new StringJoiner(", ");
        StringJoiner joiner = new StringJoiner(" OR ");
        for (int i = 0; i < indexOIds.length; i++) {
            inSql.add(indexOIds[i]);
            inSql.add(relOIds[i]);
            joiner.add("(indexrelid=" + indexOIds[i] + " and indrelid=" + relOIds[i] + ")");
        }
        Map<Integer, String> indexOidRelOidMap = new HashMap<>();
        query(String.format(PG_MAT_VIEW_INDEX_QUERY, inSql), resultSet -> {
            while (resultSet.next()) {
                indexOidRelOidMap.put(resultSet.getInt("oid"), resultSet.getString("relname"));
            }
        });

        //@todo 分区表
        //  对于上面的查询返回的relkind='p'的行，是一个分区表，其relname是这个分区表根表的名称，记为root_relname。
        //  需要进一步使用这个分区表的ID，记为mv_queried_relid，找到这个分区表的所有叶子节点，也就是存储节点中的表分区的名字，
        //  从而建立分区表名到根表名的映射part_name_map。
        //  方法是使用下列查询语句多次查询pg_inherits （因为支持多级分区）遍历其分区树：
        //  select t2.relname as leaf_relname, t2.relkind, t2.oid as nextrelid from pg_inherits t1 join pg_class t2 on t1.inhrelid=t2.oid where inhparent= currelid

        StringJoiner outSql = new StringJoiner(" OR ");
        query(String.format(PG_MAT_VIEW_IND_KEY_QUERY, joiner), resultSet -> {
            while (resultSet.next()) {
                outSql.add(String.format("(attrelid=%d and attnum=%d)", resultSet.getInt("indrelid"), resultSet.getInt("indkey")));
            }
        });

        Map<String, List<String>> indexNameMap = new HashMap<>();
        query(String.format(PG_MAT_VIEW_INDEX_NAME_QUERY, outSql), resultSet -> {
            while (resultSet.next()) {
                String indexName = resultSet.getString("attname");
                String tableName = indexOidRelOidMap.get(resultSet.getInt("attrelid"));
                List<String> indexes = indexNameMap.computeIfAbsent(tableName, k -> new ArrayList<>());
                if (EmptyKit.isEmpty(indexName)) {
                    throw new SQLException("Failed to get index name of materialized view " + mateView);
                }
                indexes.add(indexName);
            }
        });
        return indexNameMap;
    }

    public static final String PG_MAT_VIEW_QUERY = "select " +
            "    rels, idxes, t2.relname as mv_name " +
            "from pg_matview t1 " +
            "    JOIN pg_class t2 " +
            "    JOIN pg_namespace  t3 ON t1.mv_relid=t2.oid  AND t2.relnamespace=t3.oid " +
            "where t1.varying=true and t2.relkind = 'm' and t3.nspname='%s'";
    protected static final String PG_MAT_VIEW_INDEX_QUERY = "select relname, t1.oid oid from " +
            "    pg_class t1 JOIN pg_namespace t2 ON t1.relnamespace=t2.oid " +
            "WHERE t1.oid in (%s)";
    protected static final String PG_MAT_VIEW_IND_KEY_QUERY = "select " +
            "    indrelid, indkey " +
            "from pg_index " +
            "where %s";
    protected static final String PG_MAT_VIEW_INDEX_NAME_QUERY = "select attname, attrelid " +
            "from pg_attribute where %s";

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
                    "    t.table_name AS \"tableName\", t2.relname as mvName, c.oid as \"tableId\"\n" +
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
                    "    left join pg_matview t1 on t1.storage_relid = c.oid " +
                    "    left join pg_class t2 on t1.mv_relid=t2.oid " +
                    "    left join pg_namespace t3 on t2.relnamespace=t3.oid " +
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

}
