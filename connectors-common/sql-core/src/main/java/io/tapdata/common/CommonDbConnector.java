package io.tapdata.common;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.ddl.DDLSqlGenerator;
import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.entity.TapConstraintException;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.constraint.TapCreateConstraintEvent;
import io.tapdata.entity.event.ddl.constraint.TapDropConstraintEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.index.TapDeleteIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class CommonDbConnector extends ConnectorBase {

    //SQL for Primary key sorting area reading
    private final static String FIND_KEY_FROM_OFFSET = "select * from (select %s, row_number() over (order by %s) as tap__rowno from %s ) a where tap__rowno=%s";
    private final static String wherePattern = "where %s ";
    //offset for Primary key sorting area reading
    private final static Long offsetSize = 1000000L;
    protected static final int BATCH_ADVANCE_READ_LIMIT = 1000;
    protected Map<String, DataMap> writtenTableMap = new ConcurrentHashMap<>();
    protected static final String HAS_UNIQUE_INDEX = "HAS_UNIQUE_INDEX";
    protected static final String HAS_MULTI_UNIQUE_INDEX = "HAS_MULTI_UNIQUE_INDEX";
    protected static final String HAS_AUTO_INCR = "HAS_AUTO_INCR";
    protected static final String HAS_REMOVED_COLUMN = "HAS_REMOVED_COLUMN";
    protected static final String CANNOT_CLOSE_CONSTRAINT = "CANNOT_CLOSE_CONSTRAINT";
    private static final String PARAMS_NAME = "name";
    private static final String PARAMS_TYPE = "type";
    private static final String PARAMS_MODE = "mode";
    private static final String PARAMS_VALUE = "value";
    private static final String MODE_IN = "in";
    private static final String MODE_OUT = "out";
    private static final String MODE_IN_OUT = "in/out";
    private static final String MODE_RETURN = "return";
    //ddlHandlers which for ddl collection
    protected BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    //ddlSqlMaker which for ddl execution
    protected DDLSqlGenerator ddlSqlGenerator;
    //Once the task is started, this ID is a unique identifier and stored in the stateMap
    protected String firstConnectorId;
    //jdbc context for each relation datasource
    protected JdbcContext jdbcContext;
    //db config for each relation datasource (load properties from TapConnectionContext)
    protected CommonDbConfig commonDbConfig;
    protected CommonSqlMaker commonSqlMaker;
    protected Log tapLogger;
    protected ExceptionCollector exceptionCollector = new AbstractExceptionCollector() {
    };
    protected Map<String, Connection> transactionConnectionMap = new ConcurrentHashMap<>();
    protected boolean isTransaction = false;

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws SQLException {
        return jdbcContext.queryAllTables(null).size();
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws SQLException {
        List<DataMap> tableList = jdbcContext.queryAllTables(tables);
        multiThreadDiscoverSchema(tableList, tableSize, consumer);
    }

    /**
     * when your connector need to support partition table and main-curl table
     * you should impl this function to discover those tablies relations
     */
    protected List<TapTable> discoverPartitionInfo(List<TapTable> tapTableList) {
        return tapTableList;
    }

    @Override
    protected void singleThreadDiscoverSchema(List<DataMap> subList, Consumer<List<TapTable>> consumer) throws SQLException {
        List<TapTable> tapTableList = TapSimplify.list();
        List<String> subTableNames = subList.stream().map(v -> v.getString("tableName")).collect(Collectors.toList());
        List<DataMap> columnList = jdbcContext.queryAllColumns(subTableNames);
        List<DataMap> indexList = jdbcContext.queryAllIndexes(subTableNames);
        List<DataMap> fkList = jdbcContext.queryAllForeignKeys(subTableNames);
        subList.forEach(subTable -> {
            //2、table name/comment
            String table = subTable.getString("tableName");
            TapTable tapTable = table(table);
            tapTable.setTableAttr(getSpecificAttr(subTable));
            tapTable.setComment(subTable.getString("tableComment"));
            String tableCollation = subTable.getString("tableCollation");
            if (StringUtils.isNotBlank(tableCollation)) {
                String charset = tableCollation.split("_")[0];
                tapTable.setCharset(charset);
            }
            //3、primary key and table index
            List<String> primaryKey = TapSimplify.list();
            List<TapIndex> tapIndexList = TapSimplify.list();
            makePrimaryKeyAndIndex(indexList, table, primaryKey, tapIndexList);
            //4、table columns info
            AtomicInteger keyPos = new AtomicInteger(0);
            columnList.stream().filter(col -> table.equals(col.getString("tableName")))
                    .forEach(col -> {
                        try {
                            TapField tapField = makeTapField(col);
                            if (null == tapField) return;
                            tapField.setPos(keyPos.incrementAndGet());
                            tapField.setPrimaryKey(primaryKey.contains(tapField.getName()));
                            tapField.setPrimaryKeyPos(primaryKey.indexOf(tapField.getName()) + 1);
                            if (tapField.getPrimaryKey()) {
                                tapField.setNullable(false);
                            }
                            tapTable.add(tapField);
                        } catch (Exception e) {
                            throw new CoreException("Construct field failed, table: " + table + ", column: " + col + ", error: " + e.getMessage());
                        }
                    });
            tapTable.setIndexList(tapIndexList);
            tapTable.setConstraintList(makeForeignKey(fkList, table));
            tapTableList.add(tapTable);
        });
        syncSchemaSubmit(discoverPartitionInfo(tapTableList), consumer);
    }

    //some datasource makePrimaryKeyAndIndex in not the same way, such as db2
    protected void makePrimaryKeyAndIndex(List<DataMap> indexList, String table, List<String> primaryKey, List<TapIndex> tapIndexList) {
        Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("tableName")) && EmptyKit.isNotBlank(idx.getString("indexName")))
                .collect(Collectors.groupingBy(idx -> idx.getString("indexName"), LinkedHashMap::new, Collectors.toList()));
        indexMap.forEach((key, value) -> {
            if (value.stream().anyMatch(v -> ("1".equals(v.getString("isPk"))))) {
                primaryKey.addAll(value.stream().map(v -> v.getString("columnName")).collect(Collectors.toList()));
            }
            tapIndexList.add(makeTapIndex(key, value));
        });
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new CommonColumn(dataMap).getTapField();
    }

    protected List<TapConstraint> makeForeignKey(List<DataMap> fkList, String table) {
        List<TapConstraint> tapConstraints = new ArrayList<>();
        fkList.stream().filter(v -> Objects.nonNull(v) && table.equals(v.getString("tableName"))).collect(Collectors.groupingBy(map -> map.getString("constraintName"))).forEach((constraintName, fk) -> tapConstraints.add(makeTapConstraint(constraintName, fk)));
        return tapConstraints;
    }

    protected Map<String, Object> getSpecificAttr(DataMap dataMap) {
        return null;
    }

    protected void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) throws SQLException {
        jdbcContext.queryAllTables(list(), batchSize, listConsumer);
    }

    protected CreateTableOptions createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent, Boolean commentInField, String append) throws SQLException {
        if (Boolean.TRUE.equals(commonDbConfig.getDoubleActive())) {
            createDoubleActiveTempTable();
        }
        TapTable tapTable = createTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (jdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }

        Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
        for (String field : fieldMap.keySet()) {
            Object defaultValue = fieldMap.get(field).getDefaultValue();
            if (defaultValue instanceof String) {
                String fieldDefault = (String) fieldMap.get(field).getDefaultValue();
                if (EmptyKit.isNotEmpty(fieldDefault) && !Boolean.TRUE.equals(fieldMap.get(field).getAutoInc()) && EmptyKit.isNull(fieldMap.get(field).getDefaultFunction())) {
                    if (fieldDefault.contains("'")) {
                        fieldDefault = fieldDefault.replaceAll("'", "''");
                        fieldMap.get(field).setDefaultValue(fieldDefault);
                    }
                }
            }
        }
        List<String> sqlList = TapSimplify.list();
        sqlList.add(getCreateTableSql(tapTable, commentInField) + " " + append);
        if (!commentInField) {
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqlList.add(getTableCommentSql(tapTable));
            }
            for (String fieldName : fieldMap.keySet()) {
                TapField field = fieldMap.get(fieldName);
                String fieldComment = field.getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqlList.add(getColumnCommentSql(tapTable, field));
                }
            }
        }
        try {
            tapLogger.info("Create table sqls: {}", sqlList);
            jdbcContext.batchExecute(sqlList);
        } catch (SQLException e) {
            exceptionCollector.collectWritePrivileges("createTable", Collections.emptyList(), e);
            throw e;
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    //for pg,oracle type
    protected CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        return createTable(connectorContext, createTableEvent, false, "");
    }

    //for mysql type
    protected CreateTableOptions createTableV3(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        return createTable(connectorContext, createTableEvent, true, "");
    }

    protected void createDoubleActiveTempTable() throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList("_tap_double_active")).size() < 1) {
            String create = String.format("create table %s (%s int primary key, %s varchar(50))", getSchemaAndTable("_tap_double_active"),
                    commonDbConfig.getEscapeChar() + "c1" + commonDbConfig.getEscapeChar(), commonDbConfig.getEscapeChar() + "c2" + commonDbConfig.getEscapeChar());
            tapLogger.info("Create double active table sql: {}", create);
            jdbcContext.execute(create);
            String insert = String.format("insert into %s values (1, null)", getSchemaAndTable("_tap_double_active"));
            tapLogger.info("Insert one initial record into double active table: {}", insert);
            jdbcContext.execute(insert);
        }
    }

    //Primary key sorting area reading
    protected void batchReadV3(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        List<String> primaryKeys = new ArrayList<>(tapTable.primaryKeys());
        char escapeChar = commonDbConfig.getEscapeChar();
        String selectClause = getSelectSql(tapTable);
        CommonDbOffset offset = (CommonDbOffset) offsetState;
        if (EmptyKit.isNull(offset)) {
            offset = new CommonDbOffset(new DataMap(), 0L);
        }
        if (EmptyKit.isEmpty(primaryKeys)) {
            submitInitialReadEvents(selectClause, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
        } else {
            while (isAlive()) {
                DataMap from = offset.getColumnValue();
                DataMap to = findPrimaryKeyValue(tapTable, offset.getOffsetSize() + offsetSize);
                if (EmptyKit.isEmpty(from) && EmptyKit.isEmpty(to)) {
                    submitInitialReadEvents(selectClause, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    break;
                } else if (EmptyKit.isEmpty(from) && EmptyKit.isNotEmpty(to)) {
                    for (int i = 0; i < primaryKeys.size(); i++) {
                        StringBuilder whereAppender = new StringBuilder();
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                        if (i > 0) {
                            whereAppender.append(" and ");
                        }
                        if (i == primaryKeys.size() - 1) {
                            whereAppender.append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append("<=?");
                        } else {
                            whereAppender.append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append("<?");
                        }
                        List<Object> params = primaryKeys.stream().limit(i + 1).map(to::get).collect(Collectors.toList());
                        submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppender), params, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    }
                } else if (EmptyKit.isNotEmpty(from) && EmptyKit.isNotEmpty(to)) {
                    int sameKeySize = 0;
                    for (String key : primaryKeys) {
                        if (Objects.equals(from.get(key), to.get(key))) {
                            sameKeySize++;
                        } else {
                            break;
                        }
                    }
                    for (int i = primaryKeys.size() - 1; i > sameKeySize; i--) {
                        StringBuilder whereAppender = new StringBuilder();
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                        if (i > 0) {
                            whereAppender.append(" and ");
                        }
                        whereAppender.append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append(">?");
                        List<Object> params = primaryKeys.stream().limit(i + 1).map(from::get).collect(Collectors.toList());
                        submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppender), params, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    }
                    StringBuilder whereAppenderMajor = new StringBuilder();
                    whereAppenderMajor.append(primaryKeys.stream().limit(sameKeySize).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                    if (sameKeySize > 0) {
                        whereAppenderMajor.append(" and ");
                    }
                    whereAppenderMajor.append(escapeChar).append(primaryKeys.get(sameKeySize)).append(escapeChar).append(">? and ").append(escapeChar).append(primaryKeys.get(sameKeySize)).append(escapeChar).append("<?");
                    List<Object> paramsMajor = primaryKeys.stream().limit(sameKeySize + 1).map(from::get).collect(Collectors.toList());
                    paramsMajor.add(to.get(primaryKeys.get(sameKeySize)));
                    submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppenderMajor), paramsMajor, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    for (int i = sameKeySize + 1; i <= primaryKeys.size(); i++) {
                        StringBuilder whereAppender = new StringBuilder();
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                        if (i < primaryKeys.size()) {
                            whereAppender.append(" and ").append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append("<?");
                        }
                        List<Object> params = primaryKeys.stream().limit(i).map(to::get).collect(Collectors.toList());
                        if (i < primaryKeys.size()) {
                            params.add(to.get(primaryKeys.get(i)));
                        }
                        submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppender), params, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    }
                } else {
                    for (int i = primaryKeys.size() - 1; i >= 0; i--) {
                        StringBuilder whereAppender = new StringBuilder();
                        whereAppender.append(primaryKeys.stream().limit(i).map(col -> escapeChar + col + escapeChar + "=?").collect(Collectors.joining(" and ")));
                        if (i > 0) {
                            whereAppender.append(" and ");
                        }
                        whereAppender.append(escapeChar).append(primaryKeys.get(i)).append(escapeChar).append(">?");
                        List<Object> params = primaryKeys.stream().limit(i + 1).map(from::get).collect(Collectors.toList());
                        submitOffsetReadEvents(selectClause + String.format(wherePattern, whereAppender), params, tapTable, eventBatchSize, eventsOffsetConsumer, offset);
                    }
                    break;
                }
                offset = new CommonDbOffset(to, offset.getOffsetSize() + offsetSize);
            }
        }
    }

    private void submitInitialReadEvents(String sql, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, Object offset) throws Throwable {
        jdbcContext.query(sql, resultSet -> allOverResultSet(resultSet, tapTable, eventBatchSize, eventsOffsetConsumer, offset));
    }

    private void submitOffsetReadEvents(String prepareSql, List<Object> params, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, Object offset) throws Throwable {
        jdbcContext.prepareQuery(prepareSql, params, resultSet -> allOverResultSet(resultSet, tapTable, eventBatchSize, eventsOffsetConsumer, offset));
    }

    private void allOverResultSet(ResultSet resultSet, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, Object offset) throws SQLException {
        List<TapEvent> tapEvents = list();
        //get all column names
        List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
        try {
            while (isAlive() && resultSet.next()) {
                DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                processDataMap(dataMap, tapTable);
                tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents, offset);
                    tapEvents = list();
                }
            }
        } catch (SQLException e) {
            exceptionCollector.collectTerminateByServer(e);
            exceptionCollector.collectReadPrivileges("batchReadV3", Collections.emptyList(), e);
            throw e;
        }
        //last events those less than eventBatchSize
        if (EmptyKit.isNotEmpty(tapEvents)) {
            eventsOffsetConsumer.accept(tapEvents, offset);
        }
    }

    protected void processDataMap(DataMap dataMap, TapTable tapTable) throws RuntimeException {

    }

    private DataMap findPrimaryKeyValue(TapTable tapTable, Long offsetSize) throws Throwable {
        char escapeChar = commonDbConfig.getEscapeChar();
        String primaryKeyString = escapeChar + String.join(escapeChar + "," + escapeChar, tapTable.primaryKeys()) + escapeChar;
        DataMap dataMap = new DataMap();
        jdbcContext.query(String.format(FIND_KEY_FROM_OFFSET, primaryKeyString, primaryKeyString, getSchemaAndTable(tapTable.getId()), offsetSize), resultSet -> {
            if (resultSet.next()) {
                dataMap.putAll(DataMap.create(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet))));
            }
        });
        return dataMap;
    }

    protected void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() >= 1) {
            String clear = "truncate table " + getSchemaAndTable(tapClearTableEvent.getTableId());
            tapLogger.info("Truncate table sql: {}", clear);
            jdbcContext.execute(clear);
        } else {
            tapLogger.warn("Table {} not exists, skip truncate", tapClearTableEvent.getTableId());
        }
    }

    protected void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() >= 1) {
            String drop = "drop table " + getSchemaAndTable(tapDropTableEvent.getTableId());
            tapLogger.info("Drop table sql: {}", drop);
            jdbcContext.execute(drop);
        } else {
            tapLogger.warn("Table {} not exists, skip drop", tapDropTableEvent.getTableId());
        }
    }

    protected long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        try {
            AtomicLong count = new AtomicLong(0);
            String sql = "select count(1) from " + getSchemaAndTable(tapTable.getId());
            jdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
            return count.get();
        } catch (SQLException e) {
            exceptionCollector.collectReadPrivileges("batchCount", Collections.emptyList(), e);
            throw e;
        }
    }

    //one filter can only match one record
    protected void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "select * from " + getSchemaAndTable(tapTable.getId()) + " where " + commonSqlMaker.buildKeyAndValue(filter.getMatch(), "and", "=");
            FilterResult filterResult = new FilterResult();
            try {
                jdbcContext.query(sql, resultSet -> {
                    if (resultSet.next()) {
                        DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                        processDataMap(dataMap, tapTable);
                        filterResult.setResult(dataMap);
                    }
                });
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    protected void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) throws SQLException {
        List<TapIndex> indexList = createIndexEvent.getIndexList()
                .stream()
                .filter(v -> discoverIndex(tapTable.getId())
                        .stream()
                        .noneMatch(i -> DbKit.ignoreCreateIndex(i, v)))
                .collect(Collectors.toList());
        if (EmptyKit.isNotEmpty(indexList)) {
            indexList.stream().filter(i -> !i.isPrimary()).forEach(i -> {
                String sql = getCreateIndexSql(tapTable, i);
                try {
                    tapLogger.info("Create index sql: {}", sql);
                    jdbcContext.execute(sql);
                } catch (SQLException e) {
                    if (!exceptionCollector.violateIndexName(e)) {
                        tapLogger.warn("Create index failed {}, please execute it manually [{}]", e.getMessage(), sql);
                    } else {
                        String rename = i.getName().substring(0, i.getName().length() - 5) + "_" + UUID.randomUUID().toString().replaceAll("-", "").substring(28);
                        tapLogger.warn("Create index failed {}, rename {} to {} and retry ...", e.getMessage(), i.getName(), rename);
                        i.setName(rename);
                        sql = getCreateIndexSql(tapTable, i);
                        try {
                            tapLogger.info("Recreate index sql: {}", sql);
                            jdbcContext.execute(sql);
                        } catch (SQLException e1) {
                            tapLogger.warn("Create index failed again {}, please execute it manually [{}]", e1.getMessage(), sql);
                        }
                    }
                }
            });
            List<String> afterUniqueAutoIncrementSql = getAfterUniqueAutoIncrementFields(tapTable, indexList);
            if (EmptyKit.isNotEmpty(afterUniqueAutoIncrementSql)) {
                afterUniqueAutoIncrementSql.forEach(sql -> {
                    try {
                        tapLogger.info("Update auto-increment column sql: {}", sql);
                        jdbcContext.execute(sql);
                    } catch (SQLException e) {
                        tapLogger.warn("Failed to update auto-increment column {}, please execute it manually [{}]", e.getMessage(), sql);
                    }
                });
            }
        }

    }

    protected void createConstraint(TapConnectorContext connectorContext, TapTable tapTable, TapCreateConstraintEvent createConstraintEvent, boolean create) {
        if (!Boolean.TRUE.equals(commonDbConfig.getApplyForeignKey())) {
            return;
        }
        List<TapConstraint> constraintList = createConstraintEvent.getConstraintList();
        if (EmptyKit.isNotEmpty(constraintList)) {
            List<String> constraintSqlList = new ArrayList<>();
            TapConstraintException exception = new TapConstraintException(tapTable.getId());
            constraintList.forEach(c -> {
                String sql = getCreateConstraintSql(tapTable, c);
                if (create) {
                    try {
                        tapLogger.info("Create constraint sql: {}", sql);
                        jdbcContext.execute(sql);
                    } catch (Exception e) {
                        if (!exceptionCollector.violateConstraintName(e)) {
                            exception.addException(c, sql, e);
                        } else {
                            String rename = c.getName() + "_" + UUID.randomUUID().toString().replaceAll("-", "").substring(28);
                            c.setName(rename);
                            sql = getCreateConstraintSql(tapTable, c);
                            try {
                                tapLogger.info("Recreate constraint sql: {}", sql);
                                jdbcContext.execute(sql);
                            } catch (Exception e1) {
                                exception.addException(c, sql, e1);
                            }
                        }
                    }
                } else {
                    constraintSqlList.add(sql);
                }
            });
            if (!create) {
                createConstraintEvent.setConstraintSqlList(constraintSqlList);
            }
            if (EmptyKit.isNotEmpty(exception.getExceptions())) {
                throw exception;
            }
        }
    }

    protected TapIndex makeTapIndex(String key, List<DataMap> value) {
        TapIndex index = new TapIndex();
        index.setName(key);
        List<TapIndexField> fieldList = TapSimplify.list();
        value.forEach(v -> {
            TapIndexField field = new TapIndexField();
            field.setFieldAsc("1".equals(v.getString("isAsc")));
            field.setName(v.getString("columnName"));
            fieldList.add(field);
        });
        index.setUnique(value.stream().anyMatch(v -> ("1".equals(v.getString("isUnique")))));
        index.setPrimary(value.stream().anyMatch(v -> ("1".equals(v.getString("isPk")))));
        index.setIndexFields(fieldList);
        return index;
    }

    protected List<TapIndex> discoverIndex(String tableName) {
        List<TapIndex> tapIndexList = TapSimplify.list();
        List<DataMap> indexList;
        try {
            indexList = jdbcContext.queryAllIndexes(Collections.singletonList(tableName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> EmptyKit.isNotBlank(idx.getString("indexName")))
                .collect(Collectors.groupingBy(idx -> idx.getString("indexName"), LinkedHashMap::new, Collectors.toList()));
        indexMap.forEach((key, value) -> tapIndexList.add(makeTapIndex(key, value)));
        return tapIndexList;
    }

    protected TapConstraint makeTapConstraint(String key, List<DataMap> value) {
        TapConstraint tapConstraint = new TapConstraint(key, TapConstraint.ConstraintType.FOREIGN_KEY);
        value.forEach(f -> {
            tapConstraint.referencesTable(f.getString("referencesTableName"));
            tapConstraint.add(new TapConstraintMapping()
                    .foreignKey(f.getString("fk"))
                    .referenceKey(f.getString("rfk")));
            if (EmptyKit.isNotBlank(f.getString("onUpdate"))) {
                tapConstraint.onUpdate(f.getString("onUpdate"));
            }
            if (EmptyKit.isNotBlank(f.getString("onDelete"))) {
                tapConstraint.onDelete(f.getString("onDelete"));
            }
        });
        return tapConstraint;
    }

    protected List<TapConstraint> discoverConstraint(String tableName) {
        List<DataMap> constraintList;
        try {
            constraintList = jdbcContext.queryAllForeignKeys(Collections.singletonList(tableName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return makeForeignKey(constraintList, tableName);
    }

    protected void beforeWriteRecord(TapTable tapTable) throws SQLException {
        if (EmptyKit.isNull(writtenTableMap.get(tapTable.getId()))) {
            writtenTableMap.put(tapTable.getId(), DataMap.create());
        }
    }

    protected void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) throws SQLException {
        List<String> sqlList = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqlList) {
            return;
        }
        try {
            tapLogger.info("Field ddl sqls: {}", sqlList);
            jdbcContext.batchExecute(sqlList);
        } catch (SQLException e) {
            exceptionCollector.collectWritePrivileges("execute sqls: " + TapSimplify.toJson(sqlList), Collections.emptyList(), e);
            throw e;
        }
    }

    protected List<String> alterFieldAttr(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent)) {
            return null;
        }
        TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnAttr(commonDbConfig, tapAlterFieldAttributesEvent);
    }

    protected List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.dropColumn(commonDbConfig, tapDropFieldEvent);
    }

    protected List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnName(commonDbConfig, tapAlterFieldNameEvent);
    }

    protected List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.addColumn(commonDbConfig, tapNewFieldEvent);
    }

    protected String getSchemaAndTable(String tableId) {
        StringBuilder sb = new StringBuilder();
        char escapeChar = commonDbConfig.getEscapeChar();
        if (EmptyKit.isNotBlank(commonDbConfig.getSchema())) {
            sb.append(escapeChar).append(StringKit.escape(commonDbConfig.getSchema(), escapeChar)).append(escapeChar).append('.');
        }
        sb.append(escapeChar).append(StringKit.escape(tableId, escapeChar)).append(escapeChar);
        return sb.toString();
    }

    private String getCreateTableSql(TapTable tapTable, Boolean commentInField) {
        char escapeChar = commonDbConfig.getEscapeChar();
        StringBuilder sb = new StringBuilder("create table ");
        sb.append(getSchemaAndTable(tapTable.getId())).append('(').append(commonSqlMaker.buildColumnDefinition(tapTable, commentInField));
        Collection<String> primaryKeys = tapTable.primaryKeys();
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            sb.append(", primary key (").append(escapeChar)
                    .append(primaryKeys.stream().map(pk -> StringKit.escape(pk, escapeChar)).collect(Collectors.joining(escapeChar + "," + escapeChar)))
                    .append(escapeChar).append(')');
        }
        sb.append(')');
        if (commentInField && EmptyKit.isNotBlank(tapTable.getComment())) {
            commentOnTable(sb, tapTable);
        }
        return sb.toString();
    }

    protected void commentOnTable(StringBuilder sb, TapTable tapTable) {
        sb.append(" comment='").append(tapTable.getComment().replaceAll("'", "''")).append("'");
    }

    protected String getCreateIndexSql(TapTable tapTable, TapIndex tapIndex) {
        StringBuilder sb = new StringBuilder("create ");
        char escapeChar = commonDbConfig.getEscapeChar();
        if (tapIndex.isUnique()) {
            sb.append("unique ");
        }
        sb.append("index ");
        String indexName = DbKit.buildIndexName(tapTable.getId(), tapIndex, commonDbConfig.getMaxIndexNameLength());
        tapIndex.setName(indexName);
        sb.append(escapeChar).append(indexName).append(escapeChar);
        sb.append(" on ").append(getSchemaAndTable(tapTable.getId())).append('(')
                .append(tapIndex.getIndexFields().stream().map(f -> escapeChar + f.getName() + escapeChar + " " + (f.getFieldAsc() ? "asc" : "desc"))
                        .collect(Collectors.joining(","))).append(')');
        return sb.toString();
    }

    protected String getCreateConstraintSql(TapTable tapTable, TapConstraint tapConstraint) {
        char escapeChar = commonDbConfig.getEscapeChar();
        StringBuilder sb = new StringBuilder("alter table ");
        sb.append(getSchemaAndTable(tapTable.getId())).append(" add constraint ");
        if (EmptyKit.isNotBlank(tapConstraint.getName())) {
            sb.append(escapeChar).append(tapConstraint.getName()).append(escapeChar);
        } else {
            sb.append(escapeChar).append(DbKit.buildForeignKeyName(tapTable.getId(), tapConstraint, 32)).append(escapeChar);
        }
        sb.append(" foreign key (").append(escapeChar).append(tapConstraint.getMappingFields().stream().map(TapConstraintMapping::getForeignKey).collect(Collectors.joining(escapeChar + "," + escapeChar))).append(escapeChar).append(") references ")
                .append(getSchemaAndTable(tapConstraint.getReferencesTableName())).append('(').append(escapeChar).append(tapConstraint.getMappingFields().stream().map(TapConstraintMapping::getReferenceKey).collect(Collectors.joining(escapeChar + "," + escapeChar))).append(escapeChar).append(')');
        if (EmptyKit.isNotNull(tapConstraint.getOnUpdate())) {
            sb.append(" on update ").append(tapConstraint.getOnUpdate().toString().replaceAll("_", " "));
        }
        if (EmptyKit.isNotNull(tapConstraint.getOnDelete())) {
            sb.append(" on delete ").append(tapConstraint.getOnDelete().toString().replaceAll("_", " "));
        }
        return sb.toString();
    }

    private String getTableCommentSql(TapTable tapTable) {
        return "comment on table " + getSchemaAndTable(tapTable.getId()) +
                " is '" + tapTable.getComment().replace("'", "''") + '\'';
    }

    private String getColumnCommentSql(TapTable tapTable, TapField tapField) {
        char escapeChar = commonDbConfig.getEscapeChar();
        return "comment on column " + getSchemaAndTable(tapTable.getId()) + '.' +
                escapeChar + tapField.getName() + escapeChar +
                " is '" + tapField.getComment().replace("'", "''") + '\'';
    }

    private String getSelectSql(TapTable tapTable) {
        char escapeChar = commonDbConfig.getEscapeChar();
        return "select " + escapeChar + String.join(escapeChar + "," + escapeChar, tapTable.getNameFieldMap().keySet()) + escapeChar + " from " +
                getSchemaAndTable(tapTable.getId());
    }

    protected void runRawCommand(TapConnectorContext connectorContext, String command, TapTable tapTable, int eventBatchSize, Consumer<List<TapEvent>> eventsOffsetConsumer) throws Throwable {
        jdbcContext.query(command, resultSet -> {
            List<TapEvent> tapEvents = list();
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && resultSet.next()) {
                DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents);
                    tapEvents = list();
                }
            }
            if (EmptyKit.isNotEmpty(tapEvents)) {
                eventsOffsetConsumer.accept(tapEvents);
            }
        });
    }

    protected void batchReadWithoutOffset(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        if (Boolean.TRUE.equals(commonDbConfig.getHashSplit())) {
            batchReadWithHashSplit(tapConnectorContext, tapTable, offsetState, eventBatchSize, eventsOffsetConsumer);
        } else {
            batchReadWithoutHashSplit(tapConnectorContext, tapTable, offsetState, eventBatchSize, eventsOffsetConsumer);
        }
    }

    protected void batchReadWithoutHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        jdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            try {
                while (isAlive() && resultSet.next()) {
                    DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                    processDataMap(dataMap, tapTable);
                    tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                    if (tapEvents.size() == eventBatchSize) {
                        eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
                        tapEvents = list();
                    }
                }
            } catch (SQLException e) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
                exceptionCollector.revealException(e);
                throw e;
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
            }
        });
    }

    protected String getHashSplitStringSql(TapTable tapTable) {
        throw new UnsupportedOperationException("getHashSplitStringSql is not supported");
    }

    protected String getHashSplitModConditions(TapTable tapTable, int maxSplit, int currentSplit) {
        return "mod(" + getHashSplitStringSql(tapTable) + "," + maxSplit + ")=" + currentSplit;
    }

    protected String getBatchReadSelectSql(TapTable tapTable) {
        String columns = tapTable.getNameFieldMap().keySet().stream().map(c -> commonDbConfig.getEscapeChar() + StringKit.escape(c, commonDbConfig.getEscapeChar()) + commonDbConfig.getEscapeChar()).collect(Collectors.joining(","));
        return "SELECT " + columns + " FROM " + getSchemaAndTable(tapTable.getId());
    }

    protected void batchReadWithHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(commonDbConfig.getBatchReadThreadSize());
        ExecutorService executorService = Executors.newFixedThreadPool(commonDbConfig.getBatchReadThreadSize());
        try {
            for (int i = 0; i < commonDbConfig.getBatchReadThreadSize(); i++) {
                final int threadIndex = i;
                executorService.submit(() -> {
                    try {
                        for (int ii = threadIndex; ii < commonDbConfig.getMaxSplit(); ii += commonDbConfig.getBatchReadThreadSize()) {
                            String splitSql = sql + " WHERE " + getHashSplitModConditions(tapTable, commonDbConfig.getMaxSplit(), ii);
                            tapLogger.info("batchRead, splitSql[{}]: {}", ii + 1, splitSql);
                            int retry = 20;
                            while (retry-- > 0 && isAlive()) {
                                try {
                                    jdbcContext.query(splitSql, resultSet -> {
                                        List<TapEvent> tapEvents = list();
                                        //get all column names
                                        List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                                        while (isAlive() && resultSet.next()) {
                                            DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                                            processDataMap(dataMap, tapTable);
                                            tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                                            if (tapEvents.size() == eventBatchSize) {
                                                syncEventSubmit(tapEvents, eventsOffsetConsumer);
                                                tapEvents = list();
                                            }
                                        }
                                        //last events those less than eventBatchSize
                                        if (EmptyKit.isNotEmpty(tapEvents)) {
                                            syncEventSubmit(tapEvents, eventsOffsetConsumer);
                                        }
                                    });
                                    break;
                                } catch (Exception e) {
                                    if (retry == 0 || !(e instanceof SQLRecoverableException || e instanceof IOException)) {
                                        throw e;
                                    }
                                    tapLogger.warn("batchRead, splitSql[{}]: {} failed, retrying...", ii + 1, splitSql);
                                }
                            }
                        }
                    } catch (Exception e) {
                        throwable.set(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (EmptyKit.isNotNull(throwable.get())) {
                exceptionCollector.collectTerminateByServer(throwable.get());
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), throwable.get());
                exceptionCollector.revealException(throwable.get());
                throw throwable.get();
            }
        } finally {
            executorService.shutdown();
        }
    }

    protected synchronized void syncEventSubmit(List<TapEvent> eventList, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        eventsOffsetConsumer.accept(eventList, TapSimplify.list());
    }

    //for mysql type (with offset & limit)
    protected void queryByAdvanceFilterWithOffset(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = commonSqlMaker.buildSelectClause(table, filter, false) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(filter);
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            try {
                List<String> allColumn = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    DataMap dataMap = DbKit.getRowFromResultSet(resultSet, allColumn);
                    processDataMap(dataMap, table);
                    filterResults.add(dataMap);
                    if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                        consumer.accept(filterResults);
                        filterResults = new FilterResults();
                    }
                }
            } catch (SQLException e) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
                exceptionCollector.revealException(e);
                throw e;
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    //for oracle db2 type (with row_number)
    protected void queryByAdvanceFilterWithOffsetV2(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = commonSqlMaker.buildSelectClause(table, filter, true) + commonSqlMaker.buildRowNumberPreClause(filter) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilterV2(filter);
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            try {
                List<String> allColumn = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    allColumn.remove("ROWNO_");
                    DataMap dataMap = DbKit.getRowFromResultSet(resultSet, allColumn);
                    processDataMap(dataMap, table);
                    filterResults.add(dataMap);
                    if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                        consumer.accept(filterResults);
                        filterResults = new FilterResults();
                    }
                }
            } catch (SQLException e) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
                exceptionCollector.revealException(e);
                throw e;
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    //for SQL Server type (with OFFSET-FETCH)
    protected void queryByAdvanceFilterWithOffsetFetch(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = commonSqlMaker.buildSelectClause(table, filter, false) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilterWithOffsetFetch(filter);
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            try {
                List<String> allColumn = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    DataMap dataMap = DbKit.getRowFromResultSet(resultSet, allColumn);
                    processDataMap(dataMap, table);
                    filterResults.add(dataMap);
                    if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                        consumer.accept(filterResults);
                        filterResults = new FilterResults();
                    }
                }
            } catch (SQLException e) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
                exceptionCollector.revealException(e);
                throw e;
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    protected void beginTransaction(TapConnectorContext connectorContext) throws Throwable {
        isTransaction = true;
    }

    protected void commitTransaction(TapConnectorContext connectorContext) throws Throwable {
        transactionConnectionMap.computeIfPresent(Thread.currentThread().getName(), (k, v) -> {
            try {
                v.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        isTransaction = false;
    }

    protected void rollbackTransaction(TapConnectorContext connectorContext) throws Throwable {
        transactionConnectionMap.computeIfPresent(Thread.currentThread().getName(), (k, v) -> {
            try {
                v.rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        isTransaction = false;
    }

    protected void queryIndexes(TapConnectorContext connectorContext, TapTable table, Consumer<List<TapIndex>> consumer) {
        consumer.accept(discoverIndex(table.getId()));
    }

    protected void queryConstraint(TapConnectorContext connectorContext, TapTable table, Consumer<List<TapConstraint>> consumer) throws Throwable {
        consumer.accept(discoverConstraint(table.getId()));
    }

    protected void dropIndexes(TapConnectorContext connectorContext, TapTable table, TapDeleteIndexEvent deleteIndexEvent) throws SQLException {
        char escapeChar = commonDbConfig.getEscapeChar();
        List<String> dropIndexesSql = new ArrayList<>();
        deleteIndexEvent.getIndexNames().forEach(idx -> dropIndexesSql.add("drop index " + getSchemaAndTable(table.getId()) + "." + escapeChar + idx + escapeChar));
        if (EmptyKit.isNotEmpty(dropIndexesSql)) {
            tapLogger.info("Drop indexes sql: {}", dropIndexesSql);
        }
        jdbcContext.batchExecute(dropIndexesSql);
    }

    protected void dropConstraint(TapConnectorContext connectorContext, TapTable table, TapDropConstraintEvent tapDropConstraintEvent) throws SQLException {
        char escapeChar = commonDbConfig.getEscapeChar();
        List<String> dropConstraintsSql = new ArrayList<>();
        tapDropConstraintEvent.getConstraintList().forEach(fk -> dropConstraintsSql.add("alter table " + getSchemaAndTable(table.getId()) + " drop constraint " + escapeChar + fk.getName() + escapeChar));
        if (EmptyKit.isNotEmpty(dropConstraintsSql)) {
            tapLogger.info("Drop constraints sql: {}", dropConstraintsSql);
        }
        jdbcContext.batchExecute(dropConstraintsSql);
    }

    protected long countRawCommand(TapConnectorContext connectorContext, String command, TapTable tapTable) throws SQLException {
        AtomicLong count = new AtomicLong(0);
        if (EmptyKit.isNotBlank(command) && command.trim().toLowerCase().startsWith("select")) {
            jdbcContext.query("select count(1) from (" + command + ") as tmp", resultSet -> {
                if (resultSet.next()) {
                    count.set(resultSet.getLong(1));
                }
            });
        }
        return count.get();
    }

    protected long countByAdvanceFilter(TapConnectorContext connectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter) throws SQLException {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM " + getSchemaAndTable(tapTable.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(tapAdvanceFilter);
        jdbcContext.query(sql, resultSet -> {
            if (resultSet.next()) {
                count.set(resultSet.getLong(1));
            }
        });
        return count.get();
    }

    protected long countByAdvanceFilterV2(TapConnectorContext connectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter) throws SQLException {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM " + commonSqlMaker.buildRowNumberPreClause(tapAdvanceFilter) + getSchemaAndTable(tapTable.getId()) + commonSqlMaker.buildSqlByAdvanceFilterV2(tapAdvanceFilter);
        jdbcContext.query(sql, resultSet -> {
            if (resultSet.next()) {
                count.set(resultSet.getLong(1));
            }
        });
        return count.get();
    }

    protected long countByAdvanceFilterWithOffsetFetch(TapConnectorContext connectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter) throws SQLException {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM " + getSchemaAndTable(tapTable.getId()) + commonSqlMaker.buildSqlByAdvanceFilterWithOffsetFetch(tapAdvanceFilter);
        jdbcContext.query(sql, resultSet -> {
            if (resultSet.next()) {
                count.set(resultSet.getLong(1));
            }
        });
        return count.get();
    }

    protected List<String> getAfterUniqueAutoIncrementFields(TapTable tapTable, List<TapIndex> indexList) {
        return new ArrayList<>();
    }

    protected void executeCommand(TapConnectorContext connectorContext, TapExecuteCommand executeCommand, Consumer<ExecuteResult> consumer) throws Throwable {
        try {
            Map<String, Object> params = executeCommand.getParams();
            String command = executeCommand.getCommand();
            switch (command) {
                case "execute":
                case "executeQuery":
                    String sql = (String) params.get("sql");
                    int batchSize = params.get("batchSize") != null ? (int) params.get("batchSize") : 1000;
                    execute(sql, list -> consumer.accept(new ExecuteResult().result(list)), batchSize);
                    break;
                case "call":
                    String funcName = (String) params.get("funcName");
                    List<Map<String, Object>> callParams = (List<Map<String, Object>>) params.get("params");
                    consumer.accept(call(funcName, callParams));
                    break;
                default:
                    consumer.accept(new ExecuteResult<>().error(new IllegalArgumentException("Not supported command: " + command)));
            }
        } catch (Throwable e) {
            consumer.accept(new ExecuteResult<>().error(e));
        }
    }

    protected void execute(String sql, Consumer<Object> consumer, int batchSize) throws Throwable {
        try (Connection connection = jdbcContext.getConnection();
             Statement sqlStatement = connection.createStatement()) {
            sqlStatement.setFetchSize(batchSize > 0 ? batchSize : 1000); //protected from OM
            boolean hasResult = sqlStatement.execute(sql);
            if (!hasResult) {
                consumer.accept((long) sqlStatement.getUpdateCount());
            } else {
                while (isAlive() && hasResult) {
                    try (ResultSet resultSet = sqlStatement.getResultSet()) {
                        List<Map<String, Object>> list = TapSimplify.list();
                        String[] columnNames = DbKit.getColumnsFromResultSet(resultSet).toArray(new String[0]);
                        String[] columnTypes = DbKit.getColumnTypesFromResultSet(resultSet).toArray(new String[0]);
                        while (isAlive() && resultSet.next()) {
                            DataMap dataMap = filterData(resultSet, columnNames, columnTypes);
                            list.add(dataMap);
                            if (list.size() == batchSize) {
                                consumer.accept(list);
                                list = TapSimplify.list();
                            }
                        }
                        if (EmptyKit.isNotEmpty(list)) {
                            consumer.accept(list);
                        }
                    }
                    hasResult = sqlStatement.getMoreResults();
                }
            }
            connection.commit();
        }
    }

    public ExecuteResult<?> call(String funcName, List<Map<String, Object>> params) {
        if (EmptyKit.isEmpty(funcName)) {
            throw new IllegalArgumentException("procedure/function is null");
        }

        ExecuteResult<?> executeResult;

        funcName = funcName.trim();
        List<JdbcProcedureParam> outList = new ArrayList<>();

        try (Connection connection = jdbcContext.getConnection();
             CallableStatement callableStatement = createCallableStatement(funcName, params, connection, outList)) {
            if (callableStatement == null) {
                throw new RuntimeException("create callableStatement error");
            }
            boolean hasResult = callableStatement.execute();
            executeResult = new ExecuteResult<>().result(getOutputFromCall(outList, callableStatement, hasResult));
        } catch (Throwable e) {
            executeResult = new ExecuteResult<>().error(new RuntimeException(String.format("Execute database procedure/function %s error, message: %s", funcName, e.getMessage()), e));
        }
        return executeResult;
    }

    private CallableStatement createCallableStatement(String funcName, List<Map<String, Object>> params, Connection connection, List<JdbcProcedureParam> outList) throws Exception {

        CallableStatement callableStatement;
        boolean hasReturn = hasReturn(params);
        StringBuilder callStr = new StringBuilder();

        if (hasReturn) {
            callStr.append("{?=call ")
                    .append(funcName)
                    .append("(")
                    .append(StringKit.copyString("?", params.size() - 1, ","))
                    .append(")}");
        } else {
            callStr.append("{call ")
                    .append(funcName)
                    .append("(")
                    .append(StringKit.copyString("?", params.size(), ","))
                    .append(")}");
        }

        callableStatement = connection.prepareCall(callStr.toString());

        if (callableStatement == null) {
            return null;
        }

        setCallableStatementParameters(callableStatement, params, outList, connection);

        return callableStatement;
    }

    protected void setCallableStatementParameters(CallableStatement callableStatement, List<Map<String, Object>> params, List<JdbcProcedureParam> outList, Connection connection) throws Exception {
        if (callableStatement == null || params == null || params.size() == 0) {
            return;
        }
        for (int paramIndex = 1; paramIndex <= params.size(); paramIndex++) {
            Map<String, Object> paramMap = params.get(paramIndex - 1);
            if (paramMap == null || paramMap.isEmpty()) {
                throw new Exception("parameter wrong: cannot be empty");
            }

            Object objMode = paramMap.get(PARAMS_MODE);
            String mode = objMode == null ? MODE_IN : objMode.toString();
            Object value = paramMap.get(PARAMS_VALUE);
            Object objName = paramMap.get(PARAMS_NAME);
            String name = objName == null ? "param" + paramIndex : objName.toString().trim();
            Object objType = paramMap.get(PARAMS_TYPE);
            String type = objType == null ? "" : objType.toString();
            int jdbcType = type2JdbcType(type);

            if (mode.equalsIgnoreCase(MODE_IN) || mode.equalsIgnoreCase(MODE_IN_OUT)) {
                //帮我完善不同的setObject
                if (jdbcType == Types.TIMESTAMP) {
                    String dateFormat = DateUtil.determineDateFormat(value.toString());
                    if (dateFormat != null) {
                        Instant instant = LocalDateTime.parse(value.toString(), DateTimeFormatter.ofPattern(dateFormat))
                                .atZone(ZoneId.systemDefault())
                                .toInstant();
                        callableStatement.setTimestamp(paramIndex, Timestamp.from(instant));
                    } else {
                        callableStatement.setTimestamp(paramIndex, Timestamp.valueOf(value.toString()));
                    }
                } else {
                    callableStatement.setObject(paramIndex, value, jdbcType);
                }
            }

            if (mode.equalsIgnoreCase(MODE_OUT) || mode.equalsIgnoreCase(MODE_IN_OUT)) {
                JdbcProcedureParam jdbcProcedureParam = new JdbcProcedureParam(name, paramIndex, type, jdbcType);
                outList.add(jdbcProcedureParam);
                callableStatement.registerOutParameter(paramIndex, jdbcType);
            }
            if (mode.equalsIgnoreCase(MODE_RETURN)) {
                JdbcProcedureParam jdbcProcedureParam = new JdbcProcedureParam(name, 1, type, jdbcType);
                outList.add(jdbcProcedureParam);
                callableStatement.registerOutParameter(1, jdbcType);
            }
        }
    }

    protected Object getOutputFromCall(List<JdbcProcedureParam> outList, CallableStatement callableStatement, boolean hasResult) throws Exception {
        if (outList == null || callableStatement == null) {
            return null;
        }
        Map<String, Object> res = new HashMap<>();
        int resIndex = 1;
        while (hasResult) {
            try (ResultSet resultSet = callableStatement.getResultSet()) {
                List<Map<String, Object>> list = TapSimplify.list();
                String[] columnNames = DbKit.getColumnsFromResultSet(resultSet).toArray(new String[0]);
                String[] columnTypes = DbKit.getColumnTypesFromResultSet(resultSet).toArray(new String[0]);
                while (resultSet.next()) {
                    DataMap dataMap = filterData(resultSet, columnNames, columnTypes);
                    list.add(dataMap);
                }
                res.put("result" + resIndex++, list);
            }
            hasResult = callableStatement.getMoreResults();
        }
        for (JdbcProcedureParam param : outList) {
            String name = param.getName();
            int paramIndex = param.getIndex();
            String type = param.getType();

            try {
                Object out = callableStatement.getObject(paramIndex);
                out = handleValue(out);
                res.put(name, out);
            } catch (SQLException e) {
                throw new Exception("get value {param name: " + name + ", param index: " + paramIndex + ", param type: " + type + "} error: " + e.getMessage());
            }
        }

        return res;
    }

    protected Object handleValue(Object value) {
        try {
            if (value instanceof Clob) {
                value = DbKit.clobToString((Clob) value);
            } else if (value instanceof Blob) {
                value = DbKit.blobToBytes((Blob) value);
            } else if (value instanceof byte[]) {
                value = new String((byte[]) value);
            }
        } catch (Exception e) {
            throw new RuntimeException("handle value error: " + e.getMessage());
        }

        return value;
    }

    protected boolean hasReturn(List<Map<String, Object>> params) {
        if (params == null) {
            return false;
        }
        return params.stream().anyMatch(v -> v != null && MODE_RETURN.equalsIgnoreCase(String.valueOf(v.get(PARAMS_MODE))));
    }

    protected int type2JdbcType(String type) {

        switch (type) {
            case "varchar":
            case "varchar2":
            case "nvarchar2":
            case "tinytext":
            case "mediumtext":
            case "longtext":
            case "text":
                return Types.VARCHAR;
            case "char":
            case "nchar":
            case "enum":
            case "set":
                return Types.CHAR;
            case "long":
                return Types.LONGVARCHAR;
            case "number":
            case "numeric":
                return Types.NUMERIC;
            case "raw":
            case "varbinary":
                return Types.VARBINARY;
            case "longraw":
                return Types.LONGVARBINARY;
            case "date":
            case "time":
            case "datetime":
            case "timestamp":
                return Types.TIMESTAMP;
            case "clob":
                return Types.CLOB;
            case "bit":
                return Types.BIT;
            case "tinyint":
            case "bool":
            case "boolean":
                return Types.TINYINT;
            case "smallint":
                return Types.SMALLINT;
            case "mediumint":
            case "int":
            case "integer":
                return Types.INTEGER;
            case "bigint":
                return Types.BIGINT;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "decimal":
                return Types.DECIMAL;
            case "binary":
                return Types.BINARY;
            case "tinyblob":
            case "mediumblob":
            case "longblob":
            case "blob":
                return Types.BLOB;
            default:
                throw new IllegalArgumentException("Not supported:" + type);
        }
    }

    protected void filterColumns(TapTable tapTable, List<String> columns, List<String> columnTypes) {
        Iterator<String> columnIterator = columns.iterator();
        Iterator<String> typeIterator = columnTypes.iterator();
        while (columnIterator.hasNext() && typeIterator.hasNext()) {
            String column = columnIterator.next();
            typeIterator.next();
            if (!tapTable.getNameFieldMap().containsKey(column)) {
                columnIterator.remove();
                typeIterator.remove();
            }
        }
    }

    protected DataMap filterData(ResultSet resultSet, String[] fields, String[] columnTypes) throws SQLException {
        DataMap dataMap = new DataMap();
        for (int i = 0; i < fields.length; i++) {
            dataMap.put(fields[i], filterData(resultSet.getObject(fields[i]), columnTypes[i]));
        }
        return dataMap;
    }

    protected Object filterData(Object obj, String columnType) {
        return obj;
    }

    protected void executeCommandV2(TapConnectionContext connectionContext, String sqlType, String sql, Consumer<List<DataMap>> consumer) throws Throwable {
        switch (sqlType) {
            case "execute":
                jdbcContext.execute(sql);
                consumer.accept(null);
                break;
            case "query":
                jdbcContext.query(sql, resultSet -> consumer.accept(DbKit.getDataFromResultSet(resultSet)));
                break;
        }
    }

    public String exportEventSql(NormalWriteRecorder writeRecorder, TapConnectorContext connectorContext, TapEvent tapEvent, TapTable table) throws SQLException {
        if (tapEvent instanceof TapInsertRecordEvent) {
            return writeRecorder.getUpsertSql(((TapInsertRecordEvent) tapEvent).getAfter());
        } else if (tapEvent instanceof TapDeleteRecordEvent) {
            return writeRecorder.getDeleteSql(((TapDeleteRecordEvent) tapEvent).getBefore());
        }
        return null;
    }
}
