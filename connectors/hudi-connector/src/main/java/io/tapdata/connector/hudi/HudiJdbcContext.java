package io.tapdata.connector.hudi;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.field;
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

    private Map<String, String> getTableModule() {
        Map<String, String> map = new HashMap<>();
        map.put("Detailed Table Information", "table_info");
        map.put("# Partition Information", "partition_info");
        map.put("# col_name", "partition_info");
        return map;
    }

//    public TapTable getTableInfo(ResultSet resultSet, String table) throws SQLException {
//        Map<String, String> tableModuleMap = getTableModule();
//        TapTable tapTable = TapSimplify.table(table);
//        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
//        String moduleName = "column_info";
//        AtomicInteger partitionKeyPos = new AtomicInteger(0);
//        while (resultSet.next()) {
//            String title = resultSet.getString("col_name").trim();
//            if (("".equals(title) && resultSet.getString("data_type") == null)) {
//                continue;
//            }
//            if (tableModuleMap.containsKey(title)) {
//                moduleName = tableModuleMap.get(title);
//                continue;
//            }
//            switch (moduleName) {
//                case "column_info":
//                    TapField tapField = field(title, resultSet.getString("data_type"));
//                    tapField.setComment(resultSet.getString("comment"));
//                    nameFieldMap.put(title, tapField);
//                    break;
//                case "partition_info":
//                    TapField field = nameFieldMap.get(title);
//                    field.setPartitionKeyPos(partitionKeyPos.incrementAndGet());
//                    field.setPartitionKey(true);
//                    break;
//            }
//            if ("Constraints".equals(title)) {
//                Pattern pattern = Pattern.compile("Primary Key for .*:\\[(.*)].*");
//                Matcher matcher = pattern.matcher(resultSet.getString("data_type"));
//                if (matcher.find()) {
//                    AtomicInteger primaryKeyPos = new AtomicInteger(0);
//                    Arrays.stream(matcher.group(1).split(",")).forEach(v -> {
//                        TapField field = nameFieldMap.get(v);
//                        field.setPrimaryKey(true);
//                        field.setPrimaryKeyPos(primaryKeyPos.incrementAndGet());
//                    });
//                }
//            } else if (title.startsWith("Not Null Constraints for ")) {
//                Pattern pattern = Pattern.compile("\\{Constraint Name: [\\w\\s]+, Column Name: ([\\w\\s]+)}");
//                Matcher matcher = pattern.matcher(title);
//                while (matcher.find()) {
//                    TapField field = nameFieldMap.get(matcher.group(1));
//                    field.setNullable(false);
//                }
//            } else if (title.startsWith("Default Constraints for ")) {
//                Pattern pattern = Pattern.compile("\\{Constraint Name: [\\w\\s]+, \\(Column Name: ([\\w\\s]+), Default Value: ([\\w\\s()]+)\\)}");
//                Matcher matcher = pattern.matcher(title);
//                while (matcher.find()) {
//                    TapField field = nameFieldMap.get(matcher.group(1));
//                    field.setDefaultValue(matcher.group(2));
//                }
//            }
//        }
//        tapTable.setNameFieldMap(nameFieldMap);
//        return tapTable;
//    }

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
