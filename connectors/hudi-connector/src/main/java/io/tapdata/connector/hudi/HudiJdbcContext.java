package io.tapdata.connector.hudi;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.list;

public class HudiJdbcContext extends HiveJdbcContext {
    private final static String HUDI_ALL_TABLE = "show tables";
    private final static String TABLE_RECORD_COUNT = "select count(*) from ";
    public HudiJdbcContext(HiveConfig config) {
        super(config);
    }
    public Connection getConnection() throws SQLException {
        Connection connection = super.getConnection();
        connection.setAutoCommit(true);
        return connection;
    }

    @Override
    public void batchExecute(List<String> sqlList) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            for (String sql : sqlList) {
                statement.execute(sql);
            }
            connection.commit();
        }
    }
    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) throws SQLException {
        List<DataMap> tableList = list();
        query(HUDI_ALL_TABLE, resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        if (EmptyKit.isNotEmpty(tableNames)) {
            return tableList.stream().filter(t -> tableNames.contains(t.getString("tableName"))).collect(Collectors.toList());
        }
        return tableList;
    }

    /*private boolean queryRecordById(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws SQLException{
        List<DataMap> tableList = list();
        StringBuilder sql = new StringBuilder();
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        sql.append(TABLE_RECORD_COUNT).append(database).append(".").append(tapTable.getId()).append(" where ");
        Collection<String> primaryKeys = tapTable.primaryKeys();
        Map<String, Object> record = new HashMap<>();
        if (tapRecordEvent instanceof TapInsertRecordEvent){
            Map<String, Object> after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
            record = after;
        }else if (tapRecordEvent instanceof TapUpdateRecordEvent){
            Map<String, Object> before = ((TapUpdateRecordEvent) tapRecordEvent).getBefore();
            record = before;
        }else {
            return false;
        }
        Iterator<String> iterator = primaryKeys.iterator();
        while (iterator.hasNext()){
            sql.append(iterator.next());
            sql.append(" = ");
            if (record.containsKey(iterator.next())){
                sql.append(record.get(iterator.next()));
            }
            iterator.next();
        }

        for (String primaryKey : primaryKeys) {
            sql.append(primaryKey);
            sql.append(" = ");
            if (record.containsKey(primaryKey)){
                sql.append(record.get(primaryKey));
            }
        }
        query(sql.toString(), resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));

    }*/
}
