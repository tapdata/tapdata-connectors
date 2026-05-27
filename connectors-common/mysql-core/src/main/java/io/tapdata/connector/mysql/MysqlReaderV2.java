package io.tapdata.connector.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.collect.Lists;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.util.MySQLJsonParser;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.tapdata.base.ConnectorBase.list;

public class MysqlReaderV2 {

    private final MysqlConfig mysqlConfig;
    private final Log tapLogger;
    private List<String> tableList;
    private KVReadOnlyMap<TapTable> tableMap;
    private Object offsetState;
    private int recordSize;
    private StreamReadConsumer consumer;
    private final AtomicReference<Exception> exception = new AtomicReference<>();
    private final Map<Long, TableMapEventData> tableMapEventByTableId = new ConcurrentHashMap<>();
    private final Map<String, LinkedHashMap<String, String>> dataTypeMap = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Object[]>> enumDataTypeMap = new ConcurrentHashMap<>();
    private final TimeZone timeZone;
    private final DDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("`");
    private final DDLParserType ddlParserType = DDLParserType.MYSQL_CCJ_SQL_PARSER;
    protected Boolean withSchema = false;
    protected Map<String, List<String>> schemaTableMap;

    public MysqlReaderV2(MysqlJdbcContextV2 mysqlJdbcContext, Log tapLogger, TimeZone timeZone) {
        this.tapLogger = tapLogger;
        mysqlConfig = (MysqlConfig) mysqlJdbcContext.getConfig();
        this.timeZone = timeZone;
    }

    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        this.tableList = tableList;
        this.tableMap = tableMap;
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
    }

    public void multiInit(List<ConnectionConfigWithTables> connectionConfigWithTables, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        this.withSchema = true;
        schemaTableMap = new HashMap<>();
        for (ConnectionConfigWithTables withTables : connectionConfigWithTables) {
            schemaTableMap.compute(String.valueOf(withTables.getConnectionConfig().get("database")), (schema, tableList) -> {
                if (null == tableList) {
                    tableList = new ArrayList<>();
                }
                for (String tableName : withTables.getTables()) {
                    if (!tableList.contains(tableName)) {
                        tableList.add(tableName);
                    }
                }
                return tableList;
            });
        }
        this.tableMap = tableMap;
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
    }

    public void startMiner(Supplier<Boolean> isAlive) throws Throwable {
        BinaryLogClient client = new BinaryLogClient(mysqlConfig.getHost(), mysqlConfig.getPort(), mysqlConfig.getUser(), mysqlConfig.getPassword());
        AtomicReference<List<TapEvent>> events = new AtomicReference<>(list());
        AtomicReference<String> currentBinlogFile = new AtomicReference<>();

        try (ConcurrentProcessor<ScanEvent, OffsetEvent> concurrentProcessor = TapExecutors.createSimple(8, 32, "MysqlReader-Processor")) {
            client.setServerId(randomServerId());
            EventDeserializer eventDeserializer = new EventDeserializer();
            eventDeserializer.setCompatibilityMode(
                    EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO
            );
            client.setEventDeserializer(eventDeserializer);
            client.setHeartbeatInterval(3000);
            // 设置起始位置
            if (offsetState instanceof MysqlBinlogPosition) {
                MysqlBinlogPosition position = (MysqlBinlogPosition) offsetState;
                if (EmptyKit.isNotEmpty(position.getFilename())) {
                    client.setBinlogFilename(position.getFilename());
                    client.setBinlogPosition(position.getPosition());
                    currentBinlogFile.set(position.getFilename());
                    tapLogger.info("Starting from binlog position: {}/{}", position.getFilename(), position.getPosition());
                }
            }
            AtomicBoolean doubleActiveFilter = new AtomicBoolean(false);
            // 注册事件监听器
            client.registerEventListener(event -> {
                // 更新当前 binlog 位置
                EventType eventType = event.getHeader().getEventType();
                switch (eventType) {
                    case ROTATE:
                        RotateEventData rotateEventData = (RotateEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                        currentBinlogFile.set(rotateEventData.getBinlogFilename());
                        tapLogger.info("Binlog rotated to: {}/{}", rotateEventData.getBinlogFilename(), rotateEventData.getBinlogPosition());
                        break;
                    case TABLE_MAP:
                        handleTableMapEvent(event);
                        break;
                    case QUERY:
                        concurrentProcessor.runAsyncWithBlocking(new ScanEvent(event, currentBinlogFile.get()), this::emit);
                        return;
                    case XID:
                        if (doubleActiveFilter.get()) {
                            doubleActiveFilter.set(false);
                        }
                        break;
                }
                if (Boolean.TRUE.equals(mysqlConfig.getDoubleActive())) {
                    if (doubleActiveFilter.get()) {
                        return;
                    }
                    switch (eventType) {
                        case EXT_UPDATE_ROWS:
                        case UPDATE_ROWS:
                            UpdateRowsEventData eventData = (UpdateRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                            long tableId = eventData.getTableId();
                            Pair<String, String> databaseTableName = getDatabaseTableName(tableId);
                            String tableName = databaseTableName.getRight();
                            if ("_tap_double_active".equals(tableName)) {
                                doubleActiveFilter.set(true);
                                return;
                            }
                    }
                }
                // 异步处理事件
                concurrentProcessor.runAsync(new ScanEvent(event, currentBinlogFile.get()), this::emit);
            });

            consumer.streamReadStarted();
            Thread t = new Thread(() -> {
                try {
                    client.connect();
                } catch (Exception e) {
                    tapLogger.warn("Error connecting to MySQL: {}", e.getMessage(), e);
                    exception.set(e);
                }
            });
            t.setName("MysqlReader-Connector");
            t.start();
            MysqlBinlogPosition lastOffset = null;
            while (isAlive.get()) {
                if (EmptyKit.isNotNull(exception.get())) {
                    throw exception.get();
                }
                OffsetEvent offsetEvent = concurrentProcessor.get(2, TimeUnit.SECONDS);
                if (EmptyKit.isNotNull(offsetEvent)) {
                    events.get().addAll(offsetEvent.getTapEvents());
                    lastOffset = offsetEvent.getMysqlBinlogPosition();
                    if (events.get().size() >= recordSize) {
                        consumer.accept(events.get(), lastOffset);
                        events.set(new ArrayList<>());
                    }
                } else {
                    if (!events.get().isEmpty()) {
                        consumer.accept(events.get(), lastOffset);
                        events.set(list());
                    }
                }
            }
        } finally {
            consumer.streamReadEnded();
            client.disconnect();
        }
    }

    private OffsetEvent emit(ScanEvent scanEvent) {
        try {
            Event event = scanEvent.getEvent();
            EventHeader header = event.getHeader();
            EventType eventType = header.getEventType();

            // 处理 TABLE_MAP 事件，建立 tableId 到表信息的映射
            if (eventType == EventType.TABLE_MAP) {
                return null;
            }

            // 处理 ROTATE 事件，更新 binlog 文件名
            if (eventType == EventType.ROTATE) {
                return null;
            }

            if (eventType == EventType.QUERY) {
                if (((EventHeaderV4) header).getFlags() == 8) {
                    return null;
                }
                QueryEventData queryEventData = event.getData();
                long eventTime = header.getTimestamp();
                String ddl = StringKit.removeSqlNote(queryEventData.getSql());
                OffsetEvent offsetEvent = new OffsetEvent();
                List<TapEvent> ddlEvents = new ArrayList<>();
                try {
                    DDLFactory.ddlToTapDDLEvent(
                            ddlParserType,
                            ddl,
                            DDL_WRAPPER_CONFIG,
                            tableMap,
                            tapDDLEvent -> {
                                tapDDLEvent.setTime(System.currentTimeMillis());
                                tapDDLEvent.setReferenceTime(eventTime);
                                tapDDLEvent.setOriginDDL(ddl);
                                ddlEvents.add(tapDDLEvent);
                                tapLogger.info("Read DDL: " + ddl + ", about to be packaged as some event(s)");
                            }
                    );
                } catch (Throwable e) {
                    TapDDLEvent tapDDLEvent = new TapDDLUnknownEvent();
                    tapDDLEvent.setTime(System.currentTimeMillis());
                    tapDDLEvent.setReferenceTime(eventTime);
                    tapDDLEvent.setOriginDDL(ddl);
                    ddlEvents.add(tapDDLEvent);
                }
                offsetEvent.setTapEvent(ddlEvents);
                offsetEvent.setMysqlBinlogPosition(extractBinlogPosition(event, scanEvent.getFileName()));
                ddlEvents.forEach(e -> ddlFlush(((TapDDLEvent) e).getTableId()));
                return offsetEvent;
            }

            // 处理数据变更事件
            List<TapEvent> tapEvents;
            MysqlBinlogPosition position;

            switch (eventType) {
                case EXT_WRITE_ROWS:
                case WRITE_ROWS:
                    tapEvents = handleInsertEvent(event);
                    break;
                case EXT_UPDATE_ROWS:
                case UPDATE_ROWS:
                    tapEvents = handleUpdateEvent(event);
                    break;
                case EXT_DELETE_ROWS:
                case DELETE_ROWS:
                    tapEvents = handleDeleteEvent(event);
                    break;
                case HEARTBEAT:
                    tapEvents = Collections.singletonList(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis()));
                    break;
                default:
                    return null;
            }

            // 如果成功解析出 TapEvent，创建 OffsetEvent
            if (tapEvents != null) {
                position = extractBinlogPosition(event, scanEvent.getFileName());
                return new OffsetEvent(tapEvents, position);
            }

            return null;
        } catch (Exception e) {
            tapLogger.error("Error emitting event: {}, error: {}", scanEvent, e.getMessage(), e);
            return null;
        }
    }

    /**
     * 处理 TABLE_MAP 事件
     */
    private void handleTableMapEvent(Event event) {
        TableMapEventData tableMapEventData = (TableMapEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        long tableId = tableMapEventData.getTableId();
        String database = tableMapEventData.getDatabase();
        String table = tableMapEventData.getTable();

        // 保存映射关系
        tableMapEventByTableId.put(tableId, tableMapEventData);
        if (withSchema) {
            ddlFlush(database, table);
        } else {
            ddlFlush(table);
        }
        tapLogger.debug("Table map event: tableId={}, database={}, table={}", tableId, database, table);
    }

    private void ddlFlush(String table) {
        if (EmptyKit.isBlank(table)) {
            return;
        }
        if (Boolean.TRUE.equals(mysqlConfig.getDoubleActive()) && "_tap_double_active".equals(table)) {
            return;
        }
        LinkedHashMap<String, String> dataTypes = tableMap.get(table).getNameFieldMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> StringKit.removeParentheses(e.getValue().getDataType()),
                        (existing, replacement) -> existing, LinkedHashMap::new));
        dataTypeMap.put(table, dataTypes);
        Map<String, Object[]> enumMap = generateEnumMap(tableMap.get(table).getNameFieldMap().entrySet().stream().filter(v -> v.getValue().getDataType().startsWith("enum")));
        enumDataTypeMap.put(table, enumMap);
    }

    private void ddlFlush(String database, String table) {
        if (EmptyKit.isBlank(table)) {
            return;
        }
        if (Boolean.TRUE.equals(mysqlConfig.getDoubleActive()) && "_tap_double_active".equals(table)) {
            return;
        }
        LinkedHashMap<String, String> dataTypes = tableMap.get(database + "." + table).getNameFieldMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> StringKit.removeParentheses(e.getValue().getDataType()),
                        (existing, replacement) -> existing, LinkedHashMap::new));
        dataTypeMap.put(database + "." + table, dataTypes);
        Map<String, Object[]> enumMap = generateEnumMap(tableMap.get(database + "." + table).getNameFieldMap().entrySet().stream().filter(v -> v.getValue().getDataType().startsWith("enum")));
        enumDataTypeMap.put(database + "." + table, enumMap);
    }

    private Map<String, Object[]> generateEnumMap(Stream<Map.Entry<String, TapField>> stream) {
        return stream.collect(Collectors.toMap(Map.Entry::getKey, e -> {
            String enumType = e.getValue().getDataType();
            Object[] enumValues = enumType.substring("enum(".length(), enumType.length() - 1).split(",");
            for (int i = 0; i < enumValues.length; i++) {
                String element = ((String) enumValues[i]).trim();
                if (element.startsWith("'")) {
                    enumValues[i] = StringKit.removeHeadTail(element, "'", null);
                } else {
                    enumValues[i] = new BigDecimal(element);
                }
            }
            return enumValues;
        }));
    }

    /**
     * 处理 INSERT 事件
     */
    private List<TapEvent> handleInsertEvent(Event event) {
        WriteRowsEventData eventData = (WriteRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        return handleRowsEvent(event, eventData.getTableId(), eventData.getRows(), (row, ctx) -> {
            TapInsertRecordEvent insertEvent = new TapInsertRecordEvent();
            insertEvent.init();
            insertEvent.table(ctx.tableName);
            insertEvent.after(convertRowToMap(row, ctx.dataTypes, ctx.enumMap));
            insertEvent.setReferenceTime(ctx.timestamp);
            return insertEvent;
        });
    }

    /**
     * 处理 UPDATE 事件
     */
    private List<TapEvent> handleUpdateEvent(Event event) {
        UpdateRowsEventData eventData = (UpdateRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        return handleRowsEvent(event, eventData.getTableId(), eventData.getRows(), (row, ctx) -> {
            TapUpdateRecordEvent updateEvent = new TapUpdateRecordEvent();
            updateEvent.init();
            updateEvent.table(ctx.tableName);
            updateEvent.before(convertRowToMap(row.getKey(), ctx.dataTypes, ctx.enumMap));
            updateEvent.after(convertRowToMap(row.getValue(), ctx.dataTypes, ctx.enumMap));
            updateEvent.setReferenceTime(ctx.timestamp);
            return updateEvent;
        });
    }

    /**
     * 处理 DELETE 事件
     */
    private List<TapEvent> handleDeleteEvent(Event event) {
        DeleteRowsEventData eventData = (DeleteRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        return handleRowsEvent(event, eventData.getTableId(), eventData.getRows(), (row, ctx) -> {
            TapDeleteRecordEvent deleteEvent = new TapDeleteRecordEvent();
            deleteEvent.init();
            deleteEvent.table(ctx.tableName);
            deleteEvent.before(convertRowToMap(row, ctx.dataTypes, ctx.enumMap));
            deleteEvent.setReferenceTime(ctx.timestamp);
            return deleteEvent;
        });
    }

    private <R> List<TapEvent> handleRowsEvent(Event event, long tableId, List<R> rows, BiFunction<R, RowChangeContext, TapBaseEvent> mapper) {
        RowChangeContext ctx = prepareRowChangeContext(event, tableId);
        if (ctx == null) {
            return null;
        }
        if (rows == null || rows.isEmpty()) {
            return null;
        }
        List<TapEvent> tapEvents = new ArrayList<>();
        for (R row : rows) {
            TapBaseEvent tapEvent = mapper.apply(row, ctx);
            if (withSchema) {
                tapEvent.setNamespaces(Lists.newArrayList(ctx.database, ctx.tableName));
            }
            tapEvents.add(tapEvent);
        }
        return tapEvents;
    }

    private RowChangeContext prepareRowChangeContext(Event event, long tableId) {
        Pair<String, String> databaseTableName = getDatabaseTableName(tableId);
        String database = databaseTableName.getLeft();
        String tableName = databaseTableName.getRight();

        if (tableName == null || !isTableInObservation(database, tableName)) {
            return null;
        }

        TapTable tapTable = withSchema ? tableMap.get(database + "." + tableName) : tableMap.get(tableName);
        if (tapTable == null) {
            tapLogger.warn("Table {} not found in tableMap", tableName);
            return null;
        }

        return new RowChangeContext(database, tableName,
                (withSchema ? dataTypeMap.get(database + "." + tableName) : dataTypeMap.get(tableName)),
                (withSchema ? enumDataTypeMap.get(database + "." + tableName) : enumDataTypeMap.get(tableName)),
                event.getHeader().getTimestamp());
    }

    private static class RowChangeContext {
        final String database;
        final String tableName;
        final LinkedHashMap<String, String> dataTypes;
        final Map<String, Object[]> enumMap;
        final long timestamp;

        RowChangeContext(String database, String tableName, LinkedHashMap<String, String> dataTypes, Map<String, Object[]> enumMap, long timestamp) {
            this.database = database;
            this.tableName = tableName;
            this.dataTypes = dataTypes;
            this.enumMap = enumMap;
            this.timestamp = timestamp;
        }
    }

    /**
     * 将行数据数组转换为 Map
     */
    private Map<String, Object> convertRowToMap(Serializable[] row, LinkedHashMap<String, String> dataTypes, Map<String, Object[]> enumMap) {
        Map<String, Object> result = new LinkedHashMap<>();

        if (row == null || EmptyKit.isEmpty(dataTypes)) {
            return result;
        }

        // 获取字段名列表（按顺序）
        List<String> fieldNames = new ArrayList<>(dataTypes.keySet());

        // 将数组值映射到字段名
        for (int i = 0; i < row.length && i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            Object value = row[i];
            result.put(fieldName, filterValue(value, dataTypes.get(fieldName), enumMap.get(fieldName)));
        }

        return result;
    }

    private Object filterValue(Object value, String dataType, Object[] enumValues) {
        if (value == null) {
            return null;
        }
        switch (dataType) {
            case "time":
            case "date": {
                if (value instanceof Long) {
                    return Instant.ofEpochSecond(((Long) value) / 1000000, (((Long) value) % 1000000) * 1000);
                }
            }
            case "datetime": {
                if (value instanceof Long) {
                    return Instant.ofEpochSecond(((Long) value) / 1000000 - mysqlConfig.getZoneOffsetHour() * 60 * 60, ((Long) value % 1000000) * 1000);
                }
            }
            case "timestamp": {
                if (value instanceof Long) {
                    return Instant.ofEpochSecond(((Long) value) / 1000000 + timeZone.getRawOffset() / 1000, ((Long) value % 1000000) * 1000).atZone(ZoneOffset.UTC);
                }
            }
            case "bit":
                return ((BitSet) value).get(0);
            case "binary":
            case "varbinary":
                return String.valueOf(value).getBytes();
            case "json":
                return MySQLJsonParser.parseMySQLJsonBinary((byte[]) value);
            case "tinytext":
            case "mediumtext":
            case "text":
            case "longtext":
                return new String((byte[]) value);
            case "enum":
                return enumValues[(int) value - 1];
        }
        return value;
    }

    /**
     * 获取表名
     */
    private Pair<String, String> getDatabaseTableName(long tableId) {
        TableMapEventData eventData = tableMapEventByTableId.get(tableId);
        return Pair.of(eventData.getDatabase(), eventData.getTable());
    }

    /**
     * 检查表是否在监听列表中
     */
    private boolean isTableInObservation(String database, String tableName) {
        if (withSchema) {
            return schemaTableMap.containsKey(database) && schemaTableMap.get(database).contains(tableName);
        } else {
            return mysqlConfig.getDatabase().equals(database) && tableList.contains(tableName);
        }
    }

    /**
     * 提取 binlog 位置信息
     */
    private MysqlBinlogPosition extractBinlogPosition(Event event, String fileName) {
        EventHeaderV4 header = event.getHeader();
        long position = header.getNextPosition();
        return new MysqlBinlogPosition(fileName, position);
    }

    public int randomServerId() {
        int lowestServerId = 5400;
        int highestServerId = Integer.MAX_VALUE;
        return lowestServerId + new Random().nextInt(highestServerId - lowestServerId);
    }

    static class ScanEvent {

        private final Event event;
        private final String fileName;

        public ScanEvent(Event event, String fileName) {
            this.event = event;
            this.fileName = fileName;
        }

        public Event getEvent() {
            return event;
        }

        public String getFileName() {
            return fileName;
        }
    }

    static class OffsetEvent {

        private List<TapEvent> tapEvents;
        private MysqlBinlogPosition mysqlBinlogPosition;

        public OffsetEvent() {
        }

        public OffsetEvent(List<TapEvent> tapEvents, MysqlBinlogPosition mysqlBinlogPosition) {
            this.tapEvents = tapEvents;
            this.mysqlBinlogPosition = mysqlBinlogPosition;
        }

        public List<TapEvent> getTapEvents() {
            return tapEvents;
        }

        public MysqlBinlogPosition getMysqlBinlogPosition() {
            return mysqlBinlogPosition;
        }

        public void setTapEvent(List<TapEvent> tapEvents) {
            this.tapEvents = tapEvents;
        }

        public void setMysqlBinlogPosition(MysqlBinlogPosition mysqlBinlogPosition) {
            this.mysqlBinlogPosition = mysqlBinlogPosition;
        }
    }
}
