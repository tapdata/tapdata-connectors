package io.tapdata.connector.starrocks;

import io.tapdata.connector.starrocks.bean.StarrocksConfig;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author jarad
 * @date 7/14/22
 */
public class StarrocksJdbcContext extends MysqlJdbcContextV2 {
    public static final String TABLE = "table";
    public static final String VIEW = "view";

    public StarrocksJdbcContext(StarrocksConfig starrocksConfig) {
        super(starrocksConfig);
        exceptionCollector = new StarrocksExceptionCollector();
    }

    public String queryVersion() throws SQLException {
        AtomicReference<String> version = new AtomicReference<>();
        queryWithNext(STARROCKS_VERSION, resultSet -> version.set(resultSet.getString("Value")));
        return version.get();
    }

    public List<TapTable> queryTablesDesc(List<String> tableNames) throws SQLException {
        return queryTablesDesc(tableNames, TABLE);
    }

    public List<TapTable> queryTablesDesc(List<String> tableNames, String tableType) throws SQLException {
        List<TapTable> tableList = new ArrayList<>();
        boolean isView = VIEW.equals(tableType);
        for (String table : tableNames) {
            TapTable tapTable = new TapTable(table);
            tapTable.setType(tableType);
            AtomicInteger fieldPos = new AtomicInteger(0);
            AtomicInteger keyPos = new AtomicInteger(0);
            String columnSql = isView
                    ? String.format(STARROCKS_VIEW_COLUMNS,
                            StringKit.escape(getConfig().getSchema(), "'\\"),
                            StringKit.escape(table, "'\\"))
                    : String.format(STARROCKS_SHOW_COLUMNS, table);
            query(columnSql, resultSet -> {
                while (resultSet.next()) {
                    TapField tapField = new TapField(resultSet.getString("Field"), resultSet.getString("Type").toLowerCase());
                    tapField.setPos(fieldPos.incrementAndGet());
                    tapField.setComment(resultSet.getString("Comment"));
                    tapField.setNullable("YES".equals(resultSet.getString("Null")));
                    if ("YES".equals(resultSet.getString("Key"))) {
                        tapField.setPrimaryKey(true);
                        tapField.setPrimaryKeyPos(keyPos.incrementAndGet());
                    }
                    tapTable.add(tapField);
                }
            });
            if (!isView) {
                query(String.format(STARROCKS_SHOW_INDEX, table), resultSet -> {
                    while (resultSet.next()) {
                        TapIndex tapIndex = new TapIndex();
                        tapIndex.name(resultSet.getString("Key_name"));
                        tapIndex.setUnique(false);
                        tapIndex.setPrimary(false);
                        tapIndex.indexField(new TapIndexField().name(resultSet.getString("Column_name")).fieldAsc(true));
                        tapTable.add(tapIndex);
                    }
                });
            }
            tableList.add(tapTable);
        }
        return tableList;
    }

    @Override
    protected String queryAllTablesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames)
                ? "AND TABLE_NAME IN ('" + tableNames.stream()
                    .map(v -> StringKit.escape(v, "'\\"))
                    .collect(Collectors.joining("','")) + "')"
                : "";
        return String.format(STARROCKS_ALL_TABLE_AND_VIEWS, StringKit.escape(schema, "'\\"), tableSql);
    }

    private static final String STARROCKS_VERSION = "show variables like '%version_comment%'";
    private static final String STARROCKS_SHOW_COLUMNS = "show full columns from `%s`";
    private static final String STARROCKS_SHOW_INDEX = "show index from `%s`";
    private static final String STARROCKS_VIEW_COLUMNS =
            "SELECT\n" +
                    "\tCOLUMN_NAME AS `Field`,\n" +
                    "\tCOLUMN_TYPE AS `Type`,\n" +
                    "\tIS_NULLABLE AS `Null`,\n" +
                    "\tCOLUMN_KEY AS `Key`,\n" +
                    "\tCOLUMN_COMMENT AS `Comment`\n" +
                    "FROM INFORMATION_SCHEMA.COLUMNS\n" +
                    "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'\n" +
                    "ORDER BY ORDINAL_POSITION";

    private static final String STARROCKS_ALL_TABLE_AND_VIEWS =
            "SELECT\n" +
                    "\tTABLE_NAME `tableName`,\n" +
                    "\tTABLE_COMMENT `tableComment`,\n" +
                    "\tTABLE_COLLATION `tableCollation`,\n" +
                    "\tTABLE_TYPE `tableType`\n" +
                    "FROM\n" +
                    "\tINFORMATION_SCHEMA.TABLES\n" +
                    "WHERE\n" +
                    "\tTABLE_SCHEMA = '%s' %s\n" +
                    "\tAND TABLE_TYPE IN ('BASE TABLE', 'VIEW')";

}
