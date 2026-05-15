package io.tapdata.connector.postgres.cdc;

import com.google.common.collect.Lists;
import io.tapdata.common.sqlparser.ResultDO;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class AbstractWalLogMiner {

    protected final PostgresJdbcContext postgresJdbcContext;
    protected final Log tapLogger;
    protected StreamReadConsumer consumer;
    protected int recordSize;
    protected List<String> tableList;
    protected boolean filterSchema;
    private Map<String, String> pureDataTypeMap;
    private Map<String, String> dataTypeMap;
    protected final AtomicReference<Throwable> threadException = new AtomicReference<>();
    protected final PostgresCDCSQLParser sqlParser = new PostgresCDCSQLParser();
    protected final PostgresConfig postgresConfig;
    protected boolean withSchema;
    protected Map<String, List<String>> schemaTableMap;
    protected String dropTransactionId;
    protected String walLogDirectory;

    public AbstractWalLogMiner(PostgresJdbcContext postgresJdbcContext, Log tapLogger) {
        this.postgresJdbcContext = postgresJdbcContext;
        this.postgresConfig = (PostgresConfig) postgresJdbcContext.getConfig();
        this.tapLogger = tapLogger;
    }

    public AbstractWalLogMiner watch(List<String> tableList, KVReadOnlyMap<TapTable> tableMap) {
        withSchema = false;
        this.tableList = tableList;
        filterSchema = tableList.size() > 50;
        this.pureDataTypeMap = new ConcurrentHashMap<>();
        this.dataTypeMap = new ConcurrentHashMap<>();
        tableList.forEach(tableName -> {
            TapTable table = tableMap.get(tableName);
            if (EmptyKit.isNotNull(table)) {
                pureDataTypeMap.putAll(table.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(v -> tableName + "." + v.getKey(), e -> Optional.ofNullable(e.getValue().getPureDataType()).orElse(e.getValue().getDataType()))));
                dataTypeMap.putAll(table.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(v -> tableName + "." + v.getKey(), e -> e.getValue().getDataType())));
            }
        });
        tableList.addAll(getSubPartitionTables(tableMap, tableList));
        return this;
    }

    public AbstractWalLogMiner watch(Map<String, List<String>> schemaTableMap, KVReadOnlyMap<TapTable> tableMap) {
        withSchema = true;
        this.schemaTableMap = schemaTableMap;
        filterSchema = schemaTableMap.entrySet().stream().reduce(0, (a, b) -> a + b.getValue().size(), Integer::sum) > 50;
        this.pureDataTypeMap = new ConcurrentHashMap<>();
        this.dataTypeMap = new ConcurrentHashMap<>();
        schemaTableMap.forEach((schema, tables) -> {
            tables.forEach(tableName -> {
                TapTable table = tableMap.get(schema + "." + tableName);
                if (EmptyKit.isNotNull(table)) {
                    pureDataTypeMap.putAll(table.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(v -> tableName + "." + v.getKey(), e -> Optional.ofNullable(e.getValue().getPureDataType()).orElse(e.getValue().getDataType()))));
                    dataTypeMap.putAll(table.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(v -> tableName + "." + v.getKey(), e -> e.getValue().getDataType())));
                }
            });
            tables.addAll(getSubPartitionTables(tableMap, schema, tables));
        });
        return this;
    }

    public AbstractWalLogMiner withWalLogDirectory(String walLogDirectory) {
        this.walLogDirectory = walLogDirectory;
        return this;
    }

    public abstract AbstractWalLogMiner offset(Object offsetState);

    public abstract void startMiner(Supplier<Boolean> isAlive) throws Throwable;

    public AbstractWalLogMiner registerConsumer(StreamReadConsumer consumer, int recordSize) {
        this.consumer = consumer;
        this.recordSize = recordSize;
        return this;
    }

    protected void collectRedo(NormalRedo normalRedo, ResultSet resultSet) throws SQLException {
        normalRedo.setOperation(resultSet.getString("sqlkind"));
        normalRedo.setTransactionId(resultSet.getString("xid"));
        normalRedo.setTimestamp(resultSet.getTimestamp("timestamp").getTime());
        normalRedo.setSqlRedo(resultSet.getString("op_text"));
        normalRedo.setSqlUndo(resultSet.getString("undo_text"));
    }

    protected TapEvent createEvent(NormalRedo normalRedo) {
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

    protected boolean parseRedo(NormalRedo normalRedo) {
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

    protected void parseKeyAndValue(String tableName, Map.Entry<String, Object> stringObjectEntry) {
        Object value = stringObjectEntry.getValue();
        if (EmptyKit.isNull(value)) {
            return;
        }
        String key = tableName + "." + stringObjectEntry.getKey();
        String pureDataType = pureDataTypeMap.get(key);
        String dataType = dataTypeMap.get(key);
        if (EmptyKit.isNull(pureDataType)) {
            return;
        }
        switch (pureDataType) {
            case "ARRAY":
                String arrayString = String.valueOf(value);
                List<Object> array = new ArrayList<>();
                Arrays.stream(arrayString.substring(1, arrayString.length() - 1).split(",")).forEach(v -> {
                    array.add(parseType(v, StringKit.removeParentheses(dataType.replace("array", "").trim())));
                });
                stringObjectEntry.setValue(array);
                break;
            default:
                stringObjectEntry.setValue(parseType(value, pureDataType));
                break;
        }
    }

    private Object parseType(Object value, String dataType) {
        switch (dataType) {
            case "smallint":
            case "integer":
            case "bigint":
            case "numeric":
            case "money":
            case "real":
            case "double precision":
                return new BigDecimal((String) value);
            case "bit":
                if (value instanceof String && ((String) value).length() == 1) {
                    return "1".equals(value);
                }
            case "bytea":
                return StringKit.toByteArray(String.valueOf(value).substring(2));
            case "date":
                return LocalDate.parse((String) value).atStartOfDay();
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
                return stringBuilder.toString();
            case "timestamp without time zone":
            case "timestamp":
                return Timestamp.valueOf((String) value).toLocalDateTime().minusHours(postgresConfig.getZoneOffsetHour());
            case "timestamp with time zone":
                String timestamp = ((String) value).substring(0, ((String) value).length() - 3);
                String timezone = ((String) value).substring(((String) value).length() - 3);
                return Timestamp.valueOf(timestamp).toLocalDateTime().atZone(TimeZone.getTimeZone("GMT" + timezone + ":00").toZoneId());
            case "time without time zone":
            case "time":
                return LocalTime.parse((String) value).atDate(LocalDate.ofYearDay(1970, 1)).minusHours(postgresConfig.getZoneOffsetHour());
            case "time with time zone":
                String time = ((String) value).substring(0, ((String) value).length() - 3);
                String zone = ((String) value).substring(((String) value).length() - 3);
                return LocalTime.parse(time).atDate(LocalDate.ofYearDay(1970, 1)).atZone(TimeZone.getTimeZone("GMT" + zone + ":00").toZoneId());
        }
        return value;
    }

    protected static final String WALMINER_STOP = "select walminer_stop()";

    private List<String> getSubPartitionTables(KVReadOnlyMap<TapTable> tableMap, List<String> tables) {
        if (tableMap == null || EmptyKit.isEmpty(tables)) {
            return Collections.emptyList();
        }
        Iterator<Entry<TapTable>> iterator = tableMap.iterator();
        Map<String, TapTable> normalTableMap = new HashMap<>();
        while (iterator.hasNext()) {
            Entry<TapTable> entry = iterator.next();
            normalTableMap.put(entry.getKey(), entry.getValue());
        }
        List<String> subPartitionTableNames = new ArrayList<>();
        tables.forEach(table -> {
            TapTable tableInfo = normalTableMap.get(table);
            if (tableInfo != null && tableInfo.checkIsMasterPartitionTable()) {
                if (tableInfo.getPartitionInfo().getSubPartitionTableInfo() != null) {
                    List<String> subTableNames = tableInfo.getPartitionInfo().getSubPartitionTableInfo()
                            .stream().filter(Objects::nonNull)
                            .map(TapSubPartitionTableInfo::getTableName)
                            .filter(n -> !tables.contains(n))
                            .collect(Collectors.toList());
                    subTableNames.forEach(t -> tableMap.get(table).getNameFieldMap().forEach((k, field) -> {
                        pureDataTypeMap.put(t + "." + k, field.getPureDataType());
                        dataTypeMap.put(t + "." + k, field.getDataType());
                    }));
                    subPartitionTableNames.addAll(subTableNames);
                }
            }
        });
        return subPartitionTableNames;
    }

    private List<String> getSubPartitionTables(KVReadOnlyMap<TapTable> tableMap, String schema, List<String> tables) {
        if (tableMap == null || EmptyKit.isEmpty(tables)) {
            return Collections.emptyList();
        }
        Iterator<Entry<TapTable>> iterator = tableMap.iterator();
        Map<String, TapTable> normalTableMap = new HashMap<>();
        while (iterator.hasNext()) {
            Entry<TapTable> entry = iterator.next();
            normalTableMap.put(entry.getKey(), entry.getValue());
        }
        List<String> subPartitionTableNames = new ArrayList<>();
        tables.forEach(table -> {
            TapTable tableInfo = normalTableMap.get(schema + "." + table);
            if (tableInfo != null && tableInfo.checkIsMasterPartitionTable()) {
                if (tableInfo.getPartitionInfo().getSubPartitionTableInfo() != null) {
                    List<String> subTableNames = tableInfo.getPartitionInfo().getSubPartitionTableInfo()
                            .stream().filter(Objects::nonNull)
                            .map(TapSubPartitionTableInfo::getTableName)
                            .filter(n -> !tables.contains(n))
                            .collect(Collectors.toList());
                    subTableNames.forEach(t -> tableMap.get(schema + "." + table).getNameFieldMap().forEach((k, field) -> {
                        pureDataTypeMap.put(schema + "." + t + "." + k, field.getPureDataType());
                        dataTypeMap.put(t + "." + k, field.getDataType());
                    }));
                    subPartitionTableNames.addAll(subTableNames);
                }
            }
        });
        return subPartitionTableNames;
    }
}
