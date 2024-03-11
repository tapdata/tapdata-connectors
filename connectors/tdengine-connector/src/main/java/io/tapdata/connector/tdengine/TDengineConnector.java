package io.tapdata.connector.tdengine;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
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
import io.tapdata.entity.logger.TapLogger;
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
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("tdengine-spec.json")
public class TDengineConnector extends ConnectorBase {
    private static final String TAG = TDengineConnector.class.getSimpleName();
    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;
    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;

    private TDengineConfig tdengineConfig;

    private TDengineJdbcContext tdengineJdbcContext;

    private String connectionTimezone;

    private TDengineDDLSqlGenerator ddlSqlGenerator;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Exception {
        tdengineConfig = (TDengineConfig) new TDengineConfig().load(connectionContext.getConnectionConfig());
        isConnectorStarted(connectionContext, connectorContext -> tdengineConfig.load(connectorContext.getNodeConfig()));
        tdengineJdbcContext = new TDengineJdbcContext(tdengineConfig);
        this.connectionTimezone = connectionContext.getConnectionConfig().getString("timezone");
        if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
            this.connectionTimezone = tdengineJdbcContext.timezone();
        }

        fieldDDLHandlers = new BiClassHandlers<>();
        ddlSqlGenerator = new TDengineDDLSqlGenerator();
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
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        // query
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);

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
                + tdengineConfig.getDatabase() + "`.`" + tapTable.getId() + "` (" +
                TDengineSqlMaker.buildColumnDefinition(tapTable, timestamp, tdengineConfig.getSupportSuperTable() ? tdengineConfig.getSuperTableTags() : Collections.emptyList());
        sql += ")";
        if (tdengineConfig.getSupportSuperTable()) {
            sql += " TAGS (" + tdengineConfig.getSuperTableTags().stream().map(v -> {
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
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqls.add(" COMMENT '" + tapTable.getComment() + "'");
            }
            tdengineJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String tableId = tapClearTableEvent.getTableId();
        if (tdengineJdbcContext.tableExists(tableId)) {
            String sql = String.format("DELETE FROM `%s`.`%s`", tdengineConfig.getDatabase(), tableId);
            tdengineJdbcContext.execute(sql);
        } else {
            TapLogger.warn(TAG, "Table \"{}.{}\" not exists, will skip clear table", tdengineConfig.getDatabase(), tableId);
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        String tableId = tapDropTableEvent.getTableId();
        String sql = String.format("DROP TABLE IF EXISTS `%s`.`%s`", tdengineConfig.getDatabase(), tableId);
        tdengineJdbcContext.execute(sql);
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
                    .write(tapRecordEvents, consumer);
        }
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Throwable {
        TDengineOffset tdengineOffset = new TDengineOffset();
        String sql = String.format("SELECT * FROM %s.%s", tdengineConfig.getDatabase(), tapTable.getId());
        tdengineJdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && resultSet.next()) {
                tapEvents.add(insertRecordEvent(DbKit.getRowFromResultSet(resultSet, columnNames), tapTable.getId()));
                if (tapEvents.size() == batchSize) {
                    consumer.accept(tapEvents, tdengineOffset);
                    tapEvents = list();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                consumer.accept(tapEvents, tdengineOffset);
            }
        });
    }

    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        createTopic(tables, false);
        TDengineSubscribe tDengineSubscribe = new TDengineSubscribe(tdengineJdbcContext, tapConnectorContext.getLog());
        tDengineSubscribe.init(tables, tapConnectorContext.getTableMap(), offset, batchSize, consumer);
        tDengineSubscribe.subscribe(this::isAlive);
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM " + tdengineConfig.getDatabase() + "." + tapTable.getId();
        tdengineJdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
        return count.get();
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
                createSqls.add(String.format("drop topic if exists %s", topic));
            }
            createSqls.add(String.format("create topic if not exists %s as select * from %s", topic, tableName));
        }
        tdengineJdbcContext.batchExecute(createSqls);
    }

    private void dropTopic(List<String> tables) throws SQLException {
        List<String> dropSqls = new ArrayList<>();
        if (EmptyKit.isEmpty(tables)) {
            return;
        }
        for (String tableName : tables) {
            String topic = "tap_topic_" + tableName;
            dropSqls.add(String.format("drop topic if exists %s", topic));
        }
        tdengineJdbcContext.batchExecute(dropSqls);
    }

    //one filter can only match one record
    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM \"" + tdengineConfig.getDatabase() + "\".\"" + tapTable.getId() + "\" WHERE " + new CommonSqlMaker().buildKeyAndValue(filter.getMatch(), "AND", "=");
            FilterResult filterResult = new FilterResult();
            try {
                tdengineJdbcContext.queryWithNext(sql, resultSet -> filterResult.setResult(DbKit.getRowFromResultSet(resultSet, columnNames)));
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = "SELECT * FROM \"" + tdengineConfig.getDatabase() + "\".\"" + table.getId() + "\" " + new CommonSqlMaker().buildSqlByAdvanceFilter(filter);
        tdengineJdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            while (resultSet.next()) {
                filterResults.add(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet)));
                if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                    consumer.accept(filterResults);
                    filterResults = new FilterResults();
                }
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        tdengineJdbcContext.queryAllTables(TapSimplify.list(), batchSize, listConsumer);
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        for (String sql : sqls) {
            try {
                TapLogger.info(TAG, "Execute ddl sql: " + sql);
                tdengineJdbcContext.execute(sql);
            } catch (Throwable e) {
                TapLogger.error(TAG, "Execute ddl sql failed: " + sql + ", error: ", e.getMessage(), e);
            }
        }
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

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        AtomicInteger tableCount = new AtomicInteger();
        tdengineJdbcContext.queryWithNext(String.format("SELECT COUNT(1) FROM information_schema.ins_tables where db_name = '%s'", tdengineConfig.getDatabase()), resultSet -> tableCount.set(resultSet.getInt(1)));
        TapLogger.warn(TAG, "tableCount: " + tableCount.get());
        return tableCount.get();
    }

    private List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.addColumn(tdengineConfig, tapNewFieldEvent);
    }

    private List<String> alterFieldAttr(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent)) {
            return null;
        }
        TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnAttr(tdengineConfig, tapAlterFieldAttributesEvent);
    }

    private List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.dropColumn(tdengineConfig, tapDropFieldEvent);
    }

    private List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnName(tdengineConfig, tapAlterFieldNameEvent);
    }

}
