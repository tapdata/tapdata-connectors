package io.tapdata.connector.tdengine;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.connector.tdengine.kit.TDengineDbKit;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author IssaacWang
 * @create 2022-10-10
 **/
public class TDengineJdbcContext extends JdbcContext {

    private static final String TAG = TDengineJdbcContext.class.getSimpleName();

    public static final String DATABASE_TIMEZON_SQL = "select TIMEZONE()";
    public static final String DATABASE_VERSION_SQL = "select server_version()";

    public TDengineJdbcContext(TDengineConfig config) {
        super(config);
    }

    @Override
    public String queryVersion() throws SQLException {
        String version = null;
        TapLogger.debug(TAG, "Get version sql: " + DATABASE_VERSION_SQL);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(DATABASE_VERSION_SQL)
        ) {
            while (resultSet.next()) {
                version = resultSet.getString(1);
            }
        } catch (Exception e) {
            return super.queryVersion();
        }
        return version;
    }

    public String timezone() throws Exception {

        String timezone = null;
        TapLogger.debug(TAG, "Get timezone sql: " + DATABASE_TIMEZON_SQL);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(DATABASE_TIMEZON_SQL)
        ) {
            while (resultSet.next()) {
                timezone = resultSet.getString(1);
            }
        }
        return timezone;
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getDatabase());
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        String stableSql = EmptyKit.isNotEmpty(tableNames) ? "AND stable_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        String typeSql = EmptyKit.isNotEmpty(((TDengineConfig) getConfig()).getLoadTableOptions()) ? "AND type IN (" + StringKit.joinString(((TDengineConfig) getConfig()).getLoadTableOptions(), "'", ",") + ")" : "";
        try {
            query(String.format("select db_name, table_name, table_comment, type from information_schema.ins_tables " +
                            "where db_name = '%s' %s %s" +
                            "union select db_name, stable_name, table_comment, \"SUPER_TABLE\" from information_schema.ins_stables where db_name = '%s' %s;", getConfig().getDatabase(), tableSql, typeSql, getConfig().getDatabase(), stableSql),
                    resultSet -> tableList.addAll(TDengineDbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getDatabase());
        List<String> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        String typeSql = EmptyKit.isNotEmpty(((TDengineConfig) getConfig()).getLoadTableOptions()) ? "AND type IN (" + StringKit.joinString(((TDengineConfig) getConfig()).getLoadTableOptions(), "'", ",") + ")" : "";
        try {
            query(String.format("select db_name, table_name, table_comment, type from information_schema.ins_tables where db_name='%s' %s %s union select db_name, stable_name, table_comment, \"SUPER_TABLE\" from information_schema.ins_stables where db_name = '%s' %s;", getConfig().getDatabase(), tableSql, typeSql, getConfig().getDatabase(), tableSql),
                    resultSet -> {
                        while (resultSet.next()) {
                            String tableName = resultSet.getString("table_name");
                            if (StringUtils.isNotBlank(tableName)) {
                                tableList.add(tableName);
                            }
                            if (tableList.size() >= batchSize) {
                                consumer.accept(tableList);
                                tableList.clear();
                            }
                        }
                    });
            if (!tableList.isEmpty()) {
                consumer.accept(tableList);
                tableList.clear();
            }
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + getConfig().getDatabase());
        List<DataMap> columnList = TapSimplify.list();
        try {
            for (String tableName : tableNames) {
                String sql = String.format("DESCRIBE `%s`.`%s`", getConfig().getDatabase(), tableName);
                query(sql, resultSet -> {
                    List<DataMap> list = TDengineDbKit.getDataFromResultSet(resultSet);
                    list.forEach(dataMap -> dataMap.put("table_name", tableName));
                    String type = list.get(0).getString("type");
                    if (StringUtils.equalsIgnoreCase("TIMESTAMP", type)) {
                        list.get(0).put("is_primary", Boolean.TRUE);
                    }
                    columnList.addAll(list);
                });
            }
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    public Map<String, List<String>> queryAllTags(List<String> tableNames) {
        Map<String, List<String>> tagsMap = new HashMap<>();
        try {
            String sql = String.format("select distinct stable_name, tag_name from `information_schema`.`ins_tags` where db_name='%s' and stable_name in ('%s')", getConfig().getDatabase(), String.join("','", tableNames));
            query(sql, resultSet -> TDengineDbKit.getDataFromResultSet(resultSet).forEach(dataMap -> {
                String stableName = dataMap.getString("stable_name");
                String tag = dataMap.getString("tag_name");
                if (tagsMap.containsKey(stableName)) {
                    tagsMap.get(stableName).add(tag);
                } else {
                    List<String> tags = new ArrayList<>();
                    tags.add(tag);
                    tagsMap.put(stableName, tags);
                }
            }));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTags failed, error: " + e.getMessage(), e);
        }
        return tagsMap;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        return Collections.emptyList();
    }

    public void queryWithStream(String sql, ResultSetConsumer resultSetConsumer) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            if (EmptyKit.isNotNull(resultSet)) {
                resultSetConsumer.accept(resultSet);
            }
        }
    }

}
