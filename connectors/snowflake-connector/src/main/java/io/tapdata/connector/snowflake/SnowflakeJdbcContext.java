package io.tapdata.connector.snowflake;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.snowflake.config.SnowflakeConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * Snowflake JDBC Context
 * <p>
 * Snowflake uses INFORMATION_SCHEMA views which are different from PostgreSQL's system catalogs
 *
 * @author Jarad
 * @date 2026/03/24
 */
public class SnowflakeJdbcContext extends JdbcContext {

    private final static String TAG = SnowflakeJdbcContext.class.getSimpleName();

    public SnowflakeJdbcContext(SnowflakeConfig config) {
        super(config);
    }

    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext("SELECT CURRENT_VERSION()", resultSet -> version.set(resultSet.getString(1)));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return version.get();
    }

    @Override
    public Long queryTimestamp() throws SQLException {
        AtomicReference<Timestamp> currentTime = new AtomicReference<>();
        queryWithNext("SELECT CURRENT_TIMESTAMP()", resultSet -> currentTime.set(resultSet.getTimestamp(1)));
        return currentTime.get().getTime();
    }

    @Override
    protected String queryAllTablesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ?
                "AND TABLE_NAME IN ('" + tableNames.stream().map(v -> StringKit.escape(v, "'")).collect(Collectors.joining("','")) + "')" : "";

        return String.format(SNOWFLAKE_ALL_TABLES,
                StringKit.escape(getConfig().getDatabase(), "'"),
                StringKit.escape(schema, "'"),
                tableSql);
    }

    @Override
    protected String queryAllColumnsSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ?
                "AND TABLE_NAME IN ('" + tableNames.stream().map(v -> StringKit.escape(v, "'")).collect(Collectors.joining("','")) + "')" : "";

        return String.format(SNOWFLAKE_ALL_COLUMNS,
                StringKit.escape(getConfig().getDatabase(), "'"),
                StringKit.escape(schema, "'"),
                tableSql);
    }

    public synchronized List<DataMap> queryAllPks(List<String> tableNames) {
        List<DataMap> pkList = list();
        if (EmptyKit.isEmpty(tableNames)) {
            return pkList;
        }
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute(SNOWFLAKE_ALL_PK1);
            try (
                    ResultSet resultSet = statement.executeQuery(String.format(SNOWFLAKE_ALL_PK2, StringKit.escape(getConfig().getSchema(), "'")))
            ) {
                pkList.addAll(DbKit.getDataFromResultSet(resultSet));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return pkList;
    }

    /**
     * Query all indexes for hybrid tables in batch via INFORMATION_SCHEMA.
     * <p>
     * INFORMATION_SCHEMA.TABLE_CONSTRAINTS only exposes PRIMARY KEY / UNIQUE constraints,
     * so hybrid table indexes (including the PRIMARY KEY backed index and indexes created via
     * CREATE INDEX) are read by joining INFORMATION_SCHEMA.INDEXES and INDEX_COLUMNS, with a
     * LEFT JOIN to TABLE_CONSTRAINTS to flag the PRIMARY KEY backed index via isPk. The output
     * is already in the format expected by {@code makeTapIndex}: one DataMap per
     * (indexName, columnName) with isPk/isUnique/isAsc flags.
     */
    @Override
    public synchronized List<DataMap> queryAllIndexes(List<String> tableNames) {
        List<DataMap> result = list();
        if (EmptyKit.isEmpty(tableNames)) {
            return result;
        }
        String tableSql = "AND ic.TABLE_NAME IN ('" + tableNames.stream().map(v -> StringKit.escape(v, "'")).collect(Collectors.joining("','")) + "')";
        String sql = String.format(SNOWFLAKE_ALL_INDEXES,
                StringKit.escape(getConfig().getDatabase(), "'"),
                StringKit.escape(getConfig().getSchema(), "'"),
                tableSql);
        try {
            query(sql, resultSet -> result.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            // INFORMATION_SCHEMA.INDEXES / INDEX_COLUMNS are only available on AWS/Azure accounts with hybrid tables; ignore otherwise
            TapLogger.warn(TAG, "Query indexes failed, ignore: " + e.getMessage());
        }
        return result;
    }


    @Override
    protected String queryAllForeignKeysSql(String schema, List<String> tableNames) {
        // Snowflake doesn't enforce foreign keys, but they can be defined for metadata
        String tableSql = EmptyKit.isNotEmpty(tableNames) ?
                "AND rc.TABLE_NAME IN ('" + tableNames.stream().map(v -> StringKit.escape(v, "'")).collect(Collectors.joining("','")) + "')" : "";

        return String.format(SNOWFLAKE_ALL_FOREIGN_KEYS,
                StringKit.escape(getConfig().getDatabase(), "'"),
                StringKit.escape(schema, "'"),
                tableSql);
    }

    public DataMap getTableInfo(String tableName) {
        DataMap dataMap = DataMap.create();
        List<String> list = new ArrayList<>();
        list.add("row_count");
        list.add("bytes");
        try {
            query(String.format(SNOWFLAKE_TABLE_INFO,
                    StringKit.escape(getConfig().getDatabase(), "'"),
                    StringKit.escape(getConfig().getSchema(), "'"),
                    StringKit.escape(tableName, "'")), resultSet -> {
                while (resultSet.next()) {
                    dataMap.putAll(DbKit.getRowFromResultSet(resultSet, list));
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute getTableInfo failed, error: " + e.getMessage(), e);
        }
        return dataMap;
    }

    // Snowflake SQL queries using INFORMATION_SCHEMA

    protected final static String SNOWFLAKE_ALL_TABLES =
            "SELECT " +
                    "  TABLE_NAME AS \"tableName\", " +
                    "  COMMENT AS \"tableComment\" " +
                    "FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_CATALOG = '%s' " +
                    "  AND TABLE_SCHEMA = '%s' " +
                    "  AND TABLE_TYPE = 'BASE TABLE' " +
                    "  %s " +
                    "ORDER BY TABLE_NAME";

    protected final static String SNOWFLAKE_ALL_COLUMNS =
            "SELECT " +
                    "  TABLE_NAME AS \"tableName\", " +
                    "  COLUMN_NAME AS \"columnName\", " +
                    "  DATA_TYPE AS \"dataType\", " +
                    "  COLUMN_DEFAULT AS \"columnDefault\", " +
                    "  IS_NULLABLE AS \"nullable\", " +
                    "  IS_IDENTITY AS \"autoInc\", " +
                    "  IDENTITY_START AS \"seedValue\", " +
                    "  IDENTITY_INCREMENT AS \"incrementValue\", " +
                    "  CHARACTER_MAXIMUM_LENGTH AS \"dataLength\", " +
                    "  NUMERIC_PRECISION AS \"dataPrecision\", " +
                    "  NUMERIC_SCALE AS \"dataScale\", " +
                    "  DATETIME_PRECISION AS \"dataFraction\", " +
                    "  COMMENT AS \"columnComment\", " +
                    "  ORDINAL_POSITION AS \"ordinalPosition\" " +
                    "FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_CATALOG = '%s' " +
                    "  AND TABLE_SCHEMA = '%s' " +
                    "  %s " +
                    "ORDER BY TABLE_NAME, ORDINAL_POSITION";

    protected final static String SNOWFLAKE_ALL_PK1 = "SHOW PRIMARY KEYS IN DATABASE";

    protected final static String SNOWFLAKE_ALL_PK2 = "SELECT\n" +
            "    \"table_name\" as \"tableName\",\n" +
            "    \"constraint_name\" as \"constraintName\",\n" +
            "    \"column_name\" as \"columnName\",\n" +
            "    \"key_sequence\" as \"KEYSEQ\"\n" +
            "FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE \"schema_name\" = '%s' ORDER BY \"table_name\", \"key_sequence\"";

    protected final static String SNOWFLAKE_ALL_INDEXES =
            "SELECT " +
                    "  ic.TABLE_NAME AS \"tableName\", " +
                    "  ic.INDEX_NAME AS \"indexName\", " +
                    "  ic.\"NAME\" AS \"columnName\", " +
                    "  CASE WHEN c.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN '1' ELSE '0' END AS \"isPk\", " +
                    "  CASE WHEN i.IS_UNIQUE = 'YES' THEN '1' ELSE '0' END AS \"isUnique\", " +
                    "  '1' AS \"isAsc\" " +
                    "FROM INFORMATION_SCHEMA.INDEX_COLUMNS ic " +
                    "JOIN INFORMATION_SCHEMA.INDEXES i " +
                    "  ON i.TABLE_SCHEMA = ic.TABLE_SCHEMA " +
                    "  AND i.TABLE_NAME = ic.TABLE_NAME " +
                    "  AND i.\"NAME\" = ic.INDEX_NAME " +
                    "LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c " +
                    "  ON i.TABLE_SCHEMA = c.CONSTRAINT_SCHEMA " +
                    "  AND i.CONSTRAINT_NAME = c.CONSTRAINT_NAME " +
                    "WHERE ic.TABLE_CATALOG = '%s' " +
                    "  AND ic.TABLE_SCHEMA = '%s' " +
                    "  %s " +
                    "ORDER BY ic.TABLE_NAME, ic.INDEX_NAME, ic.KEY_SEQUENCE";


    protected final static String SNOWFLAKE_ALL_FOREIGN_KEYS =
            "SELECT " +
                    "  rc.CONSTRAINT_NAME AS \"constraintName\", " +
                    "  rc.TABLE_NAME AS \"tableName\", " +
                    "  rc.REFERENCED_TABLE_NAME AS \"referencesTableName\", " +
                    "  kcu.COLUMN_NAME AS \"fk\", " +
                    "  kcu.REFERENCED_COLUMN_NAME AS \"rfk\", " +
                    "  rc.UPDATE_RULE AS \"onUpdate\", " +
                    "  rc.DELETE_RULE AS \"onDelete\" " +
                    "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc " +
                    "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu " +
                    "  ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME " +
                    "  AND rc.TABLE_SCHEMA = kcu.TABLE_SCHEMA " +
                    "WHERE rc.CONSTRAINT_CATALOG = '%s' " +
                    "  AND rc.CONSTRAINT_SCHEMA = '%s' " +
                    "  %s " +
                    "ORDER BY rc.CONSTRAINT_NAME";

    protected final static String SNOWFLAKE_TABLE_INFO =
            "SELECT " +
                    "  ROW_COUNT AS \"row_count\", " +
                    "  BYTES AS \"bytes\" " +
                    "FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_CATALOG = '%s' " +
                    "  AND TABLE_SCHEMA = '%s' " +
                    "  AND TABLE_NAME = '%s'";
}

