package io.tapdata.connector.dws;

import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.postgres.PostgresConnector;
import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.ddl.PostgresDDLSqlGenerator;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.postgresql.geometric.*;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * PDK for Postgresql
 *
 * @author Jarad
 * @date 2022/4/18
 */
@TapConnectorClass("spec_dws.json")
public class DwsConnector extends PostgresConnector {
    protected DwsJdbcContext dwsJdbcContext;

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        initConnection(connectorContext);
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new PostgresColumn(dataMap).getTapField();
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(postgresConfig.getConnectionString());
        try (
                DwsTest dwsTest = new DwsTest(postgresConfig, consumer).initContext()
        ) {
            dwsTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //test
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        // target
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
//        connectorFunctions.supportQueryIndexes(this::queryIndexes);
//        connectorFunctions.supportDeleteIndex(this::dropIndexes);
        // query
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        connectorFunctions.supportCountByPartitionFilterFunction(this::countByAdvanceFilter);

        // ddl
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> postgresJdbcContext.getConnection(), this::isAlive, c));
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });

        codecRegistry.registerToTapValue(PgArray.class, (value, tapType) -> {
            PgArray pgArray = (PgArray) value;
            try (
                    ResultSet resultSet = pgArray.getResultSet()
            ) {
                return new TapArrayValue(DbKit.getDataArrayByColumnName(resultSet, "VALUE"));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(PgSQLXML.class, (value, tapType) -> {
            try {
                return new TapStringValue(((PgSQLXML) value).getString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(PGbox.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGcircle.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGline.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGlseg.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpath.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGobject.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpoint.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpolygon.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(UUID.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGInterval.class, (value, tapType) -> {
            //P1Y1M1DT12H12M12.312312S
            PGInterval pgInterval = (PGInterval) value;
            String interval = "P" + pgInterval.getYears() + "Y" +
                    pgInterval.getMonths() + "M" +
                    pgInterval.getDays() + "DT" +
                    pgInterval.getHours() + "H" +
                    pgInterval.getMinutes() + "M" +
                    pgInterval.getSeconds() + "S";
            return new TapStringValue(interval);
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
        codecRegistry.registerFromTapValue(TapYearValue.class, "character(4)", tapYearValue -> formatTapDateTime(tapYearValue.getValue(), "yyyy"));
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
        connectorFunctions.supportTransactionBeginFunction(this::beginTransaction);
        connectorFunctions.supportTransactionCommitFunction(this::commitTransaction);
        connectorFunctions.supportTransactionRollbackFunction(this::rollbackTransaction);
    }

    @Override
    protected CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        StringBuilder append = new StringBuilder();
        TapTable tapTable = createTableEvent.getTable();
        Collection<String> primaryKeys = tapTable.primaryKeys();
        if (EmptyKit.isNotEmpty(postgresConfig.getDistributedKey())) {
            if (EmptyKit.isEmpty(primaryKeys)) {
                append.append("distribute by hash(\"").append(String.join("\",\"", postgresConfig.getDistributedKey())).append("\")");
            } else {
                if (postgresConfig.getDistributedKey().stream().anyMatch(v -> !primaryKeys.contains(v))) {
                    throw new IllegalArgumentException("Primary Key must contain distributed key");
                } else if (postgresConfig.getDistributedKey().size() != primaryKeys.size()) {
                    append.append("distribute by hash(\"").append(String.join("\",\"", postgresConfig.getDistributedKey())).append("\")");
                }
            }
        } else if (EmptyKit.isNotEmpty(primaryKeys)) {
            append.append("distribute by hash(\"").append(String.join("\",\"", primaryKeys)).append("\")");
        }
        return createTable(connectorContext, createTableEvent, false, append.toString());
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        EmptyKit.closeQuietly(dwsJdbcContext);
    }

    //initialize jdbc context, slot name, version
    private void initConnection(TapConnectionContext connectionContext) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
        postgresConfig.load(connectionContext.getNodeConfig());
        dwsJdbcContext = new DwsJdbcContext(postgresConfig);
        commonDbConfig = postgresConfig;
        jdbcContext = dwsJdbcContext;
        commonSqlMaker = new DwsSqlMaker().closeNotNull(postgresConfig.getCloseNotNull());
        postgresVersion = dwsJdbcContext.queryVersion();
        ddlSqlGenerator = new PostgresDDLSqlGenerator();
        tapLogger = connectionContext.getLog();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        exceptionCollector = new PostgresExceptionCollector();
    }

    protected TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = postgresJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(new BigDecimal(dataMap.getString("rowcount")).longValue());
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("size")));
        return tableInfo;
    }

    private String getCreateIndexForPartitionTableSql(TapTable tapTable, TapIndex tapIndex) {
        StringBuilder sb = new StringBuilder("create ");
        char escapeChar = commonDbConfig.getEscapeChar();
        if (tapIndex.isUnique()) {
            sb.append("unique ");
        }
        sb.append("index ");
        if (EmptyKit.isNotBlank(tapIndex.getName())) {
            sb.append(escapeChar).append(tapIndex.getName()).append(escapeChar);
        } else {
            sb.append(escapeChar).append(DbKit.buildIndexName(tapTable.getId(), tapIndex, 63)).append(escapeChar);
        }
        sb.append(" on ").append(getSchemaAndTable(tapTable.getId())).append('(')
                .append(tapIndex.getIndexFields().stream().map(f -> escapeChar + f.getName() + escapeChar)
                        .collect(Collectors.joining(","))).append(") local");
        return sb.toString();
    }

    @Override
    protected void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) throws SQLException {
        //判断是否为分区表  SELECT * FROM PG_PARTITION where relname='tableName'
        if (postgresConfig.getIsPartition()) {
            //分区
            List<String> sqlList = TapSimplify.list();
            List<TapIndex> indexList = createIndexEvent.getIndexList()
                    .stream()
                    .filter(v -> discoverIndex(tapTable.getId())
                            .stream()
                            .noneMatch(i -> DbKit.ignoreCreateIndex(i, v)))
                    .collect(Collectors.toList());
            if (EmptyKit.isNotEmpty(indexList)) {
                indexList.stream().filter(i -> !i.isPrimary()).forEach(i ->
                        sqlList.add(getCreateIndexForPartitionTableSql(tapTable, i)));
            }
            jdbcContext.batchExecute(sqlList);

        } else {
            super.createIndex(connectorContext, tapTable, createIndexEvent);
        }
    }

    @Override
    public List<TapTable> discoverPartitionInfo(List<TapTable> tapTableList) {
        return tapTableList;
    }

    protected void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        boolean hasUniqueIndex;
        if (EmptyKit.isNull(writtenTableMap.get(tapTable.getId()))) {
            hasUniqueIndex = makeSureHasUnique(tapTable);
            writtenTableMap.put(tapTable.getId(), DataMap.create().kv(HAS_UNIQUE_INDEX, hasUniqueIndex));
        } else {
            hasUniqueIndex = writtenTableMap.get(tapTable.getId()).getValue(HAS_UNIQUE_INDEX, false);
        }
        String insertDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        if (isTransaction) {
            String threadName = Thread.currentThread().getName();
            Connection connection;
            if (transactionConnectionMap.containsKey(threadName)) {
                connection = transactionConnectionMap.get(threadName);
            } else {
                connection = postgresJdbcContext.getConnection();
                transactionConnectionMap.put(threadName, connection);
            }
            new DwsRecordWriter(dwsJdbcContext, connection, tapTable, hasUniqueIndex)
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setTapLogger(tapLogger)
                    .write(tapRecordEvents, writeListResultConsumer, this::isAlive);

        } else {
            new DwsRecordWriter(dwsJdbcContext, tapTable, hasUniqueIndex)
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setTapLogger(tapLogger)
                    .write(tapRecordEvents, writeListResultConsumer, this::isAlive);
        }
    }
}
