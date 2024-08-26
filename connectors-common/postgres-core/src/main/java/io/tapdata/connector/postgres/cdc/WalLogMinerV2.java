package io.tapdata.connector.postgres.cdc;

import com.google.common.collect.Lists;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.common.sqlparser.ResultDO;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.list;

public class WalLogMinerV2 {

    private final PostgresJdbcContext postgresJdbcContext;
    private final Log tapLogger;
    private StreamReadConsumer consumer;
    private int recordSize;
    private List<String> tableList;
    private boolean filterSchema;
    private Map<String, String> dataTypeMap;
    private String walSearchLsn;
    private String continuesLsn;
    private final AtomicReference<Throwable> threadException = new AtomicReference<>();
    private final PostgresCDCSQLParser sqlParser = new PostgresCDCSQLParser();
    private final PostgresConfig postgresConfig;
    private boolean withSchema;
    Map<String, List<String>> schemaTableMap;
    private String dropTransactionId;

    public WalLogMinerV2(PostgresJdbcContext postgresJdbcContext, Log tapLogger) {
        this.postgresJdbcContext = postgresJdbcContext;
        this.postgresConfig = (PostgresConfig) postgresJdbcContext.getConfig();
        this.tapLogger = tapLogger;
    }

    public WalLogMinerV2 watch(List<String> tableList, KVReadOnlyMap<TapTable> tableMap) {
        withSchema = false;
        this.tableList = tableList;
        filterSchema = tableList.size() > 50;
        this.dataTypeMap = new ConcurrentHashMap<>();
        tableList.forEach(tableName -> {
            TapTable table = tableMap.get(tableName);
            if (EmptyKit.isNotNull(table)) {
                dataTypeMap.putAll(table.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(v -> tableName + "." + v.getKey(), e -> e.getValue().getPureDataType())));
            }
        });
        return this;
    }

    public WalLogMinerV2 watch(Map<String, List<String>> schemaTableMap, KVReadOnlyMap<TapTable> tableMap) {
        withSchema = true;
        this.schemaTableMap = schemaTableMap;
        filterSchema = schemaTableMap.entrySet().stream().reduce(0, (a, b) -> a + b.getValue().size(), Integer::sum) > 50;
        this.dataTypeMap = new ConcurrentHashMap<>();
        schemaTableMap.forEach((schema, tables) -> tables.forEach(tableName -> {
            TapTable table = tableMap.get(schema + "." + tableName);
            if (EmptyKit.isNotNull(table)) {
                dataTypeMap.putAll(table.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(v -> schema + "." + tableName + "." + v.getKey(), e -> e.getValue().getPureDataType())));
            }
        }));
        return this;
    }

    public WalLogMinerV2 offset(Object offsetState) {
        String[] split = ((String) offsetState).split(",");
        this.walSearchLsn = split[0];
        this.continuesLsn = split[1];
        return this;
    }

    public WalLogMinerV2 registerConsumer(StreamReadConsumer consumer, int recordSize) {
        this.consumer = consumer;
        this.recordSize = recordSize;
        return this;
    }

    public void startMiner(Supplier<Boolean> isAlive) throws Throwable {
        try (
                ConcurrentProcessor<NormalRedo, NormalRedo> concurrentProcessor = TapExecutors.createSimple(8, 32, "wal-miner");
                Connection connection = postgresJdbcContext.getConnection();
                Statement statement = connection.createStatement()
        ) {
            Thread t = new Thread(() -> {
                consumer.streamReadStarted();
                NormalRedo lastRedo = null;
                AtomicReference<List<TapEvent>> events = new AtomicReference<>(list());
                while (isAlive.get()) {
                    try {
                        NormalRedo redo = concurrentProcessor.get(2, TimeUnit.SECONDS);
                        if (EmptyKit.isNotNull(redo)) {
                            if (EmptyKit.isNotNull(redo.getOperation())) {
                                lastRedo = redo;
                                events.get().add(createEvent(redo));
                                if (events.get().size() >= recordSize) {
                                    consumer.accept(events.get(), redo.getCdcSequenceStr());
                                    events.set(new ArrayList<>());
                                }
                            } else {
                                consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis())), redo.getCdcSequenceStr());
                            }
                        } else {
                            if (events.get().size() > 0) {
                                consumer.accept(events.get(), lastRedo.getCdcSequenceStr());
                                events.set(new ArrayList<>());
                            }
                        }
                    } catch (Exception e) {
                        threadException.set(e);
                    }
                }
            });
            t.setName("wal-miner-Consumer");
            t.start();
            AtomicReference<String> nextLsn = new AtomicReference<>();
            while (isAlive.get()) {
                if (EmptyKit.isNotNull(threadException.get())) {
                    consumer.streamReadEnded();
                    throw new RuntimeException(threadException.get());
                }
                try (ResultSet resultSet = statement.executeQuery(String.format(WALMINER_NEXT_LSN, walSearchLsn))) {
                    if (resultSet.next()) {
                        nextLsn.set(resultSet.getString(1));
                    }
                }
                if (walSearchLsn.equals(nextLsn.get())) {
                    NormalRedo heart = new NormalRedo();
                    heart.setCdcSequenceStr(walSearchLsn + "," + continuesLsn);
                    concurrentProcessor.runAsync(heart, r -> r);
                    TapSimplify.sleep(1000);
                    continue;
                }
                tapLogger.info("Start mining wal lsn range: {} - {}", walSearchLsn, nextLsn.get());
                while (isAlive.get()) {
                    try {
                        statement.execute(String.format(WALMINER_BY_LSN, walSearchLsn, nextLsn.get()));
                        break;
                    } catch (Exception e) {
                        try {
                            statement.execute(WALMINER_STOP);
                        } catch (Exception ignore) {
                        }
                        TapSimplify.sleep(2000);
                    }
                }
                String analysisSql = getAnalysisSql(continuesLsn);
                try (ResultSet resultSet = statement.executeQuery(analysisSql)) {
                    while (resultSet.next()) {
                        String relation = resultSet.getString("relation");
                        String schema = resultSet.getString("schema");
                        continuesLsn = resultSet.getString("start_lsn");
                        if (withSchema) {
                            if (filterSchema && !schemaTableMap.get(schema).contains(relation)) {
                                continue;
                            }
                        } else {
                            if (filterSchema && !tableList.contains(relation)) {
                                continue;
                            }
                        }
                        NormalRedo normalRedo = new NormalRedo();
                        normalRedo.setNameSpace(schema);
                        normalRedo.setTableName(relation);
                        normalRedo.setCdcSequenceStr(walSearchLsn + "," + continuesLsn);
                        collectRedo(normalRedo, resultSet);
                        //双活情形下，需要过滤_tap_double_active记录的同事务数据
                        if (Boolean.TRUE.equals(postgresConfig.getDoubleActive())) {
                            if ("_tap_double_active".equals(relation)) {
                                dropTransactionId = normalRedo.getTransactionId();
                                continue;
                            } else {
                                if (null != dropTransactionId) {
                                    if (dropTransactionId.equals(normalRedo.getTransactionId())) {
                                        continue;
                                    } else {
                                        dropTransactionId = null;
                                    }
                                }
                            }
                        }
                        concurrentProcessor.runAsync(normalRedo, r -> {
                            try {
                                if (parseRedo(r)) {
                                    return r;
                                }
                            } catch (Throwable e) {
                                threadException.set(e);
                            }
                            return null;
                        });
                    }
                }
                statement.execute(WALMINER_STOP);
                walSearchLsn = nextLsn.get();
            }
            consumer.streamReadEnded();
        }
    }

    private void collectRedo(NormalRedo normalRedo, ResultSet resultSet) throws SQLException {
        normalRedo.setOperation(resultSet.getString("sqlkind"));
        normalRedo.setTransactionId(resultSet.getString("xid"));
        normalRedo.setTimestamp(resultSet.getTimestamp("timestamp").getTime());
        normalRedo.setSqlRedo(resultSet.getString("op_text"));
        normalRedo.setSqlUndo(resultSet.getString("undo_text"));
    }

    private String getAnalysisSql(String startLsn) {
        if (withSchema) {
            if (filterSchema) {
                return String.format(MULTI_WALMINER_CONTENTS_SCHEMA, startLsn, String.join("','", schemaTableMap.keySet()));
            } else {
                return String.format(MULTI_WALMINER_CONTENTS_TABLE, startLsn, schemaTableMap.entrySet().stream().map(e ->
                        String.format("schema='%s' and relation in ('%s')", e.getKey(), String.join("','", e.getValue()))).collect(Collectors.joining(" or ")));
            }
        } else {
            if (filterSchema) {
                return String.format(WALMINER_CONTENTS_SCHEMA, startLsn, postgresConfig.getSchema());
            } else {
                return String.format(WALMINER_CONTENTS_TABLE, startLsn, postgresConfig.getSchema(), String.join("','", tableList));
            }
        }
    }

    private TapEvent createEvent(NormalRedo normalRedo) {
        if ("DDL".equals(normalRedo.getOperation())) {
            return null;
        }
        TapRecordEvent tapEvent;
        switch (normalRedo.getOperation()) {
            case "1":
                tapEvent = new TapInsertRecordEvent().init().after(normalRedo.getRedoRecord());
                break;
            case "2":
                tapEvent = new TapUpdateRecordEvent().init().after(normalRedo.getRedoRecord()).before(normalRedo.getUndoRecord());
                break;
            case "3":
                tapEvent = new TapDeleteRecordEvent().init().before(normalRedo.getRedoRecord());
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + normalRedo.getOperation());
        }
        tapEvent.setTableId(normalRedo.getTableName());
        tapEvent.setReferenceTime(normalRedo.getTimestamp());
        if (withSchema) {
            tapEvent.setNamespaces(Lists.newArrayList(normalRedo.getNameSpace(), normalRedo.getTableName()));
        }
        return tapEvent;
    }

    private boolean parseRedo(NormalRedo normalRedo) {
        String tableName;
        if (withSchema) {
            tableName = normalRedo.getNameSpace() + "." + normalRedo.getTableName();
        } else {
            tableName = normalRedo.getTableName();
        }
        ResultDO redo = sqlParser.from(normalRedo.getSqlRedo(), false);
        if (EmptyKit.isNotNull(redo)) {
            for (Map.Entry<String, Object> entry : redo.getData().entrySet()) {
                parseKeyAndValue(tableName, entry);
            }
            normalRedo.setRedoRecord(redo.getData());
        } else {
            return false;
        }
        if ("2".equals(normalRedo.getOperation())) {
            ResultDO undo = sqlParser.from(normalRedo.getSqlUndo(), true);
            for (Map.Entry<String, Object> entry : undo.getData().entrySet()) {
                parseKeyAndValue(tableName, entry);
            }
            normalRedo.setUndoRecord(undo.getData());
        }
        return true;
    }

    private void parseKeyAndValue(String tableName, Map.Entry<String, Object> stringObjectEntry) {
        Object value = stringObjectEntry.getValue();
        if (EmptyKit.isNull(value)) {
            return;
        }
        String key = tableName + "." + stringObjectEntry.getKey();
        String dataType = dataTypeMap.get(key);
        if (EmptyKit.isNull(dataType)) {
            return;
        }
        switch (dataType) {
            case "smallint":
            case "integer":
            case "bigint":
            case "numeric":
            case "real":
            case "double precision":
                stringObjectEntry.setValue(new BigDecimal((String) value));
                break;
            case "bit":
                if (value instanceof String && ((String) value).length() == 1) {
                    stringObjectEntry.setValue("1".equals(value));
                }
                break;
            case "bytea":
                stringObjectEntry.setValue(StringKit.toByteArray(((String) value).substring(2)));
                break;
            case "date":
                stringObjectEntry.setValue(LocalDate.parse((String) value).atStartOfDay());
                break;
            case "interval":
                String[] intervalArray = ((String) value).split(" ");
                StringBuilder stringBuilder = new StringBuilder("P");
                for (String s : intervalArray) {
                    switch (s) {
                        case "years":
                            stringBuilder.append("Y");
                            break;
                        case "mons":
                            stringBuilder.append("M");
                            break;
                        case "days":
                            stringBuilder.append("DT");
                            break;
                        default:
                            if (s.contains(":")) {
                                String[] timeArray = s.split(":");
                                if (timeArray.length != 3) {
                                    stringBuilder.append(s);
                                } else {
                                    stringBuilder.append(Integer.parseInt(timeArray[0])).append("H")
                                            .append(Integer.parseInt(timeArray[1])).append("M")
                                            .append(Double.parseDouble(timeArray[2])).append("S");
                                }
                            } else {
                                stringBuilder.append(s);
                            }
                            break;
                    }
                }
                stringObjectEntry.setValue(stringBuilder.toString());
                break;
            case "timestamp without time zone":
                stringObjectEntry.setValue(Timestamp.valueOf((String) value).toLocalDateTime().minusHours(postgresConfig.getZoneOffsetHour()));
                break;
            case "timestamp with time zone":
                String timestamp = ((String) value).substring(0, ((String) value).length() - 3);
                String timezone = ((String) value).substring(((String) value).length() - 3);
                stringObjectEntry.setValue(Timestamp.valueOf(timestamp).toLocalDateTime().atZone(TimeZone.getTimeZone("GMT" + timezone + ":00").toZoneId()));
                break;
            case "time without time zone":
                stringObjectEntry.setValue(LocalTime.parse((String) value).atDate(LocalDate.ofYearDay(1970, 1)).minusHours(postgresConfig.getZoneOffsetHour()));
                break;
            case "time with time zone":
                String time = ((String) value).substring(0, ((String) value).length() - 3);
                String zone = ((String) value).substring(((String) value).length() - 3);
                stringObjectEntry.setValue(LocalTime.parse(time).atDate(LocalDate.ofYearDay(1970, 1)).atZone(TimeZone.getTimeZone("GMT" + zone + ":00").toZoneId()));
                break;
        }
    }

    private static final String WALMINER_STOP = "select walminer_stop()";
    private static final String WALMINER_NEXT_LSN = "select pg_lsn_smaller(pg_lsn_pli('%s',10000000), pg_current_wal_lsn())";
    private static final String WALMINER_BY_LSN = "select walminer_by_lsn('%s', '%s', true)";
    private static final String WALMINER_CONTENTS_SCHEMA = "select * from walminer_contents where minerd=true and start_lsn>'%s' and schema='%s' order by start_lsn";
    private static final String WALMINER_CONTENTS_TABLE = "select * from walminer_contents where minerd=true and start_lsn>'%s' and schema='%s' and relation in ('%s') order by start_lsn";
    private static final String MULTI_WALMINER_CONTENTS_SCHEMA = "select * from walminer_contents where minerd=true and start_lsn>'%s' and schema in ('%s') order by start_lsn";
    private static final String MULTI_WALMINER_CONTENTS_TABLE = "select * from walminer_contents where minerd=true and start_lsn>'%s' and (%s) order by start_lsn";
}
