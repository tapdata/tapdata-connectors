package io.tapdata.connector.tdengine;

import com.google.common.collect.Lists;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.connector.tdengine.bean.TDengineColumn;
import io.tapdata.connector.tdengine.bean.TDengineOffset;
import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.connector.tdengine.ddl.TDengineDDLSqlGenerator;
import io.tapdata.connector.tdengine.subscribe.TDengineSubscribe;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("tdengine-spec.json")
public class TDengineConnector extends CommonDbConnector {

    private TDengineConfig tdengineConfig;
    private TDengineJdbcContext tdengineJdbcContext;
    private String connectionTimezone;
    private String tdengineVersion;
    private AtomicBoolean streamReadStarted = new AtomicBoolean(false);
    private ConsumerRecords<Map<String, Object>> firstRecords;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Exception {
        tdengineConfig = (TDengineConfig) new TDengineConfig().load(connectionContext.getConnectionConfig());
        isConnectorStarted(connectionContext, connectorContext -> {
            tdengineConfig.load(connectorContext.getNodeConfig());
            firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = UUID.randomUUID().toString().replace("-", "");
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
        });
        tapLogger = connectionContext.getLog();
        if (tdengineConfig.getFileLog()) {
            tapLogger.info("Starting Jdbc Logging, connectorId: {}", firstConnectorId);
            tdengineConfig.startJdbcLog(firstConnectorId);
        }
        tdengineJdbcContext = new TDengineJdbcContext(tdengineConfig);
        this.connectionTimezone = connectionContext.getConnectionConfig().getString("timezone");
        if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
            this.connectionTimezone = tdengineJdbcContext.timezone();
        }
        this.tdengineVersion = tdengineJdbcContext.queryVersion();
        commonDbConfig = tdengineConfig;
        jdbcContext = tdengineJdbcContext;
        commonSqlMaker = new CommonSqlMaker(tdengineConfig.getEscapeChar());
        ddlSqlGenerator = new TDengineDDLSqlGenerator();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        if (!tdengineConfig.getSupportWebSocket()) {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        streamReadStarted.set(false);
        EmptyKit.closeQuietly(tdengineJdbcContext);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "VARCHAR(1000)", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return toString(tapValue.getValue());
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "VARCHAR(1000)", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return toString(tapValue.getValue());
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "VARCHAR(1000)", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return toString(tapValue.getValue());
            }
            return null;
        });

        codecRegistry.registerFromTapValue(TapDateTimeValue.class, "TIMESTAMP", tapDateTimeValue -> {
            if (tapDateTimeValue.getValue() != null && tapDateTimeValue.getValue().getTimeZone() == null) {
                tapDateTimeValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            }
            return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, "TIMESTAMP", tapDateValue -> {
            if (tapDateValue.getValue() != null && tapDateValue.getValue().getTimeZone() == null) {
                tapDateValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            }
            return formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd");
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, "VARCHAR(20)", tapTimeValue -> {
            if (tapTimeValue.getValue() != null && tapTimeValue.getValue().getTimeZone() == null) {
                tapTimeValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            }
            return formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SSSSSS");
        });
        codecRegistry.registerFromTapValue(TapYearValue.class, "VARCHAR(4)", tapYearValue -> {
            if (tapYearValue.getValue() != null && tapYearValue.getValue().getTimeZone() == null) {
                tapYearValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            }
            return formatTapDateTime(tapYearValue.getValue(), "yyyy");
        });

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportCreateTableV2(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        // query
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        connectorFunctions.supportCountByPartitionFilterFunction(this::countByAdvanceFilter);

        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportReleaseExternalFunction(this::releaseExternal);
    }

    private String getTimestampColumn(TapTable tapTable) {
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        if (EmptyKit.isEmpty(primaryKeys) || primaryKeys.size() > 1) {
            return tapTable.getNameFieldMap().values().stream()
                    .filter(tapField -> "timestamp".equals(tapField.getDataType())).findFirst()
                    .orElseThrow(() -> new RuntimeException("no timestamp columns")).getName();
        } else {
            return primaryKeys.iterator().next();
        }

    }

    private CreateTableOptions createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (tdengineJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        String timestamp = getTimestampColumn(tapTable);
        String sql = "CREATE " + (tdengineConfig.getSupportSuperTable() ? "S" : "") + "TABLE IF NOT EXISTS `"
                + tdengineConfig.getDatabase() + "`.`" + tapTable.getId() + "` (";
        if (tdengineConfig.getSupportSuperTable()) {
            List<String> tags;
            if (EmptyKit.isEmpty(tdengineConfig.getSuperTableTags())) {
                if (EmptyKit.isNull(tapTable.getTableAttr().get("tags"))) {
                    tags = null;
                } else {
                    tags = (List<String>) tapTable.getTableAttr().get("tags");
                }
            } else {
                tags = tdengineConfig.getSuperTableTags();
            }
            sql += TDengineSqlMaker.buildColumnDefinition(tapTable, timestamp, tags);
            sql += ")";
            if (EmptyKit.isNotEmpty(tags)) {
                sql += " TAGS (" + tags.stream().map(v -> {
                    StringBuilder builder = new StringBuilder();
                    TapField tapField = tapTable.getNameFieldMap().get(v);
                    //ignore those which has no dataType
                    if (tapField.getDataType() == null) {
                        return "";
                    }
                    builder.append('`').append(tapField.getName()).append("` ").append(tapField.getDataType()).append(' ');
                    return builder.toString();
                }).collect(Collectors.joining(", ")) + ")";
            }
        } else {
            sql += TDengineSqlMaker.buildColumnDefinition(tapTable, timestamp, Collections.emptyList());
            sql += ")";
        }
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqls.add(" COMMENT '" + tapTable.getComment() + "'");
            }
            tapLogger.info("Create table sql: {}", sqls);
            tdengineJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    protected void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
            tapLogger.info("Clear table sql: DELETE FROM " + getSchemaAndTable(tapClearTableEvent.getTableId()));
            jdbcContext.execute("DELETE FROM " + getSchemaAndTable(tapClearTableEvent.getTableId()));
        }
    }

    protected void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws SQLException {
        String tableId = tapDropTableEvent.getTableId();
        List<String> dropSqls = new ArrayList<>();
        dropSqls.add(String.format("DROP TABLE IF EXISTS `%s`.`%s`", tdengineConfig.getDatabase(), tableId));
        dropSqls.add(String.format("DROP STABLE IF EXISTS `%s`.`%s`", tdengineConfig.getDatabase(), tableId));
        tapLogger.info("Drop table sql: {}", dropSqls);
        tdengineJdbcContext.batchExecute(dropSqls);
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        String timestamp = getTimestampColumn(tapTable);
        if (tdengineConfig.getSupportSuperTable()) {
            new TDengineSuperRecordWriter(tdengineJdbcContext, tapTable)
                    .write(tapRecordEvents, consumer, this::isAlive);
        } else {
            new TDengineRecordWriter(tdengineJdbcContext, tapTable, timestamp)
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .write(tapRecordEvents, consumer, this::isAlive);
        }
    }

    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        streamReadStarted.set(true);
        TapSimplify.sleep(2000);
        createTopic(tables, false);
        TDengineSubscribe tDengineSubscribe = new TDengineSubscribe(tdengineJdbcContext, tapConnectorContext.getLog(), tdengineVersion);
        tDengineSubscribe.init(tables, tapConnectorContext.getTableMap(), offset, batchSize, consumer);
        tDengineSubscribe.setFirstRecords(firstRecords);
        tDengineSubscribe.subscribe(this::isAlive, false, streamReadStarted);
    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        if (EmptyKit.isNotNull(startTime)) {
            throw new RuntimeException("TDEngine connector does not support timestamp to stream offset");
        }
        List<String> tables = new ArrayList<>();
        io.tapdata.entity.utils.cache.Iterator<Entry<TapTable>> iterator = tapConnectorContext.getTableMap().iterator();
        while (iterator.hasNext()) {
            tables.add(iterator.next().getValue().getId());
        }
        createTopic(tables, true);
        tapConnectorContext.getStateMap().put("tap_topic", tables);
        new Thread(() -> {
            TDengineSubscribe tDengineSubscribe = new TDengineSubscribe(tdengineJdbcContext, tapConnectorContext.getLog(), tdengineVersion);
            tDengineSubscribe.init(tables, tapConnectorContext.getTableMap(), null, 1, null);
            tDengineSubscribe.subscribe(this::isAlive, true, streamReadStarted);
            firstRecords = tDengineSubscribe.getFirstRecords();
        }).start();
        return new TDengineOffset();
    }

    private void createTopic(List<String> tables, boolean canReplace) throws SQLException {
        List<String> createSqls = new ArrayList<>();
        if (EmptyKit.isEmpty(tables)) {
            throw new RuntimeException("tables is empty");
        }
        for (String tableName : tables) {
            String topic = "tap_topic_" + tableName;
            if (canReplace) {
                createSqls.add(String.format("drop topic if exists `%s`", topic));
            }
            createSqls.add(String.format("create topic if not exists `%s` as select * from `%s`", topic, tableName));
        }
        tapLogger.info("Create topic sql: {}", createSqls);
        tdengineJdbcContext.batchExecute(createSqls);
    }

    private void dropTopic(List<String> tables) throws SQLException {
        List<String> dropSqls = new ArrayList<>();
        if (EmptyKit.isEmpty(tables)) {
            return;
        }
        for (String tableName : tables) {
            String topic = "tap_topic_" + tableName;
            dropSqls.add(String.format("drop topic if exists `%s`", topic));
        }
        tapLogger.info("Drop topic sql: {}", dropSqls);
        tdengineJdbcContext.batchExecute(dropSqls);
    }

    private void releaseExternal(TapConnectorContext tapConnectorContext) throws Exception {
        try {
            onStart(tapConnectorContext);
            dropTopic((List<String>) tapConnectorContext.getStateMap().get("tap_topic"));
        } finally {
            onStop(tapConnectorContext);
        }
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        //get table info
        List<DataMap> tableList = tdengineJdbcContext.queryAllTables(tables);
        //paginate by tableSize
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("table_name")).collect(Collectors.toList());
            List<DataMap> columnList = tdengineJdbcContext.queryAllColumns(subTableNames);
            Map<String, List<String>> tagsMap = tdengineJdbcContext.queryAllTags(subTableNames);
            //make up tapTable
            subList.forEach(subTable -> {
                //1、table name/comment
                String table = subTable.getString("table_name");
                TapTable tapTable = table(table);
                tapTable.setComment(subTable.getString("comment"));
                List<TapIndex> tapIndexList = TapSimplify.list();
                //2、table columns info
                AtomicInteger keyPos = new AtomicInteger(0);
                columnList.stream().filter(col -> table.equals(col.getString("table_name")))
                        .forEach(col -> {
                            TapField tapField = new TDengineColumn(col).getTapField(); //make up fields
                            tapField.setPos(keyPos.incrementAndGet());
                            if (col.getValue("is_primary", Boolean.FALSE)) {
                                tapField.setPrimaryKey(Boolean.TRUE);
                                tapField.setNullable(Boolean.FALSE);
                                tapField.setPrimaryKeyPos(1);
                                TapIndex tapIndex = new TapIndex();
                                String pk = String.format("pk_%s", tapField.getName());
                                tapIndex.setName(pk);
                                tapIndex.setPrimary(Boolean.TRUE);
                                TapIndexField tapIndexField = new TapIndexField();
                                tapIndexField.setName(tapField.getName());
                                tapIndexField.setFieldAsc(Boolean.FALSE);
                                tapIndex.setIndexFields(Collections.singletonList(tapIndexField));
                                tapIndexList.add(tapIndex);
                            } else {
                                tapField.setNullable(Boolean.TRUE);
                            }
                            tapTable.add(tapField);
                        });
                tapTable.setIndexList(tapIndexList);
                if (tagsMap.containsKey(table)) {
                    tapTable.setTableAttr(TapSimplify.map(TapSimplify.entry("tags", tagsMap.get(table))));
                }
                tapTableList.add(tapTable);
            });
            consumer.accept(tapTableList);
        });
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        tdengineConfig = (TDengineConfig) new TDengineConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(tdengineConfig.getConnectionString());
        try (
                TDengineTest tdengineTest = new TDengineTest(tdengineConfig, consumer)
        ) {
            tdengineTest.testOneByOne();

            return connectionOptions;
        }
    }

    protected void batchReadRestful(String sql, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, Timestamp min, Timestamp max) throws Throwable {
        long offset = 0;
        while (isAlive()) {
            String querySql;
            if (eventBatchSize > 0) {
                querySql = sql + " limit " + eventBatchSize + " offset " + offset;
            } else {
                querySql = sql;
            }
            List<TapEvent> tapEvents = list();
            List<String> columnNames = list();
            if (EmptyKit.isNull(min) && EmptyKit.isNull(max)) {
                jdbcContext.query(querySql, resultSet -> allOverResultSet(resultSet, columnNames, tapTable, tapEvents));
            } else {
                jdbcContext.prepareQuery(querySql, Arrays.asList(min, max), resultSet -> allOverResultSet(resultSet, columnNames, tapTable, tapEvents));
            }
            syncEventSubmit(tapEvents, eventsOffsetConsumer);
            if (eventBatchSize == 0 || eventBatchSize > 0 && tapEvents.size() < eventBatchSize) {
                break;
            }
            offset += eventBatchSize;
        }
    }

    private void allOverResultSet(ResultSet resultSet, List<String> columnNames, TapTable tapTable, List<TapEvent> tapEvents) throws SQLException {
        if (EmptyKit.isEmpty(columnNames)) {
            columnNames.addAll(DbKit.getColumnsFromResultSet(resultSet));
        }
        while (isAlive() && resultSet.next()) {
            DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
            processDataMap(dataMap, tapTable);
            tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
        }
    }

    protected void batchReadWithoutHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        try {
            batchReadRestful(sql, tapTable, eventBatchSize, eventsOffsetConsumer, null, null);
        } catch (SQLException e) {
            exceptionCollector.collectTerminateByServer(e);
            exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
            exceptionCollector.revealException(e);
            throw e;
        }
    }

    protected void batchReadWithHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        String timestampColumn = tapTable.primaryKeys().stream().findFirst().orElseThrow(() -> new RuntimeException("no primary key"));
        AtomicLong maxTimestamp = new AtomicLong();
        AtomicLong minTimestamp = new AtomicLong();
        jdbcContext.normalQuery(String.format("select `%s` from %s order by `%s` desc limit 1", timestampColumn, getSchemaAndTable(tapTable.getId()), timestampColumn), resultSet -> {
            if (resultSet.next()) {
                maxTimestamp.set(resultSet.getTimestamp(1).getTime());
            }
        });
        jdbcContext.normalQuery(String.format("select `%s` from %s order by `%s` limit 1", timestampColumn, getSchemaAndTable(tapTable.getId()), timestampColumn), resultSet -> {
            if (resultSet.next()) {
                minTimestamp.set(resultSet.getTimestamp(1).getTime());
            }
        });
        Timestamp[] splitTimestamps = new Timestamp[commonDbConfig.getMaxSplit()];
        for (int i = 0; i < splitTimestamps.length - 1; i++) {
            splitTimestamps[i] = new Timestamp(minTimestamp.get() + (maxTimestamp.get() - minTimestamp.get()) / commonDbConfig.getMaxSplit() * i);
        }
        splitTimestamps[commonDbConfig.getMaxSplit() - 1] = new Timestamp(maxTimestamp.get());
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(commonDbConfig.getBatchReadThreadSize());
        ExecutorService executorService = Executors.newFixedThreadPool(commonDbConfig.getBatchReadThreadSize());
        AtomicInteger concurrentIndex = new AtomicInteger(-1);
        try {
            for (int i = 0; i < commonDbConfig.getBatchReadThreadSize(); i++) {
                executorService.submit(() -> {
                    try {
                        while (isAlive()) {
                            int index = concurrentIndex.incrementAndGet();
                            String splitSql;
                            if (index >= commonDbConfig.getMaxSplit() - 1) {
                                break;
                            } else if (index == commonDbConfig.getMaxSplit() - 2) {
                                splitSql = sql + String.format(" WHERE `%s` >= ? AND `%s` <= ?", timestampColumn, timestampColumn);
                            } else {
                                splitSql = sql + String.format(" WHERE `%s` >= ? AND `%s` < ?", timestampColumn, timestampColumn);
                            }
                            tapLogger.info("batchRead, splitSql[{}], from {} to {}", splitSql, splitTimestamps[index], splitTimestamps[index + 1]);
                            batchReadRestful(splitSql, tapTable, eventBatchSize, eventsOffsetConsumer, splitTimestamps[index], splitTimestamps[index + 1]);
                        }
                    } catch (Throwable e) {
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

}
