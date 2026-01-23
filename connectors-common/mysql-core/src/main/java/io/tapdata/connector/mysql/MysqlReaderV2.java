package io.tapdata.connector.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.util.MySQLJsonParser;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

            // 注册事件监听器
            client.registerEventListener(event -> {
                // 更新当前 binlog 位置
                EventType eventType = event.getHeader().getEventType();
                if (eventType == EventType.ROTATE) {
                    RotateEventData rotateEventData = (RotateEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                    currentBinlogFile.set(rotateEventData.getBinlogFilename());
                    tapLogger.info("Binlog rotated to: {}/{}", rotateEventData.getBinlogFilename(), rotateEventData.getBinlogPosition());
                } else if (eventType == EventType.TABLE_MAP) {
                    handleTableMapEvent(event);
                } else if (eventType == EventType.QUERY) {
                    concurrentProcessor.runAsyncWithBlocking(new ScanEvent(event, currentBinlogFile.get()), this::emit);
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
        ddlFlush(table);
        tapLogger.debug("Table map event: tableId={}, database={}, table={}", tableId, database, table);
    }

    private void ddlFlush(String table) {
        if (EmptyKit.isBlank(table)) {
            return;
        }
        LinkedHashMap<String, String> dataTypes = tableMap.get(table).getNameFieldMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> StringKit.removeParentheses(e.getValue().getDataType()),
                        (existing, replacement) -> existing, LinkedHashMap::new));
        dataTypeMap.put(table, dataTypes);
        Map<String, Object[]> enumMap = tableMap.get(table).getNameFieldMap().entrySet().stream().filter(v -> v.getValue().getDataType().startsWith("enum"))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
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
        enumDataTypeMap.put(table, enumMap);
    }

    /**
     * 处理 INSERT 事件
     */
    private List<TapEvent> handleInsertEvent(Event event) {
        WriteRowsEventData eventData = (WriteRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        long tableId = eventData.getTableId();
        String tableName = getTableName(tableId);

        if (tableName == null || !isTableInList(tableName)) {
            return null;
        }

        TapTable tapTable = tableMap.get(tableName);
        if (tapTable == null) {
            tapLogger.warn("Table {} not found in tableMap", tableName);
            return null;
        }

        List<Serializable[]> rows = eventData.getRows();
        if (rows == null || rows.isEmpty()) {
            return null;
        }
        List<TapEvent> tapEvents = new ArrayList<>();
        for (Serializable[] row : rows) {
            Map<String, Object> after = convertRowToMap(row, dataTypeMap.get(tableName), enumDataTypeMap.get(tableName));
            TapInsertRecordEvent insertEvent = new TapInsertRecordEvent();
            insertEvent.init();
            insertEvent.table(tableName);
            insertEvent.after(after);
            insertEvent.setReferenceTime(event.getHeader().getTimestamp());
            tapEvents.add(insertEvent);
        }

        return tapEvents;
    }

    /**
     * 处理 UPDATE 事件
     */
    private List<TapEvent> handleUpdateEvent(Event event) {
        UpdateRowsEventData eventData = (UpdateRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        long tableId = eventData.getTableId();
        String tableName = getTableName(tableId);

        if (tableName == null || !isTableInList(tableName)) {
            return null;
        }

        TapTable tapTable = tableMap.get(tableName);
        if (tapTable == null) {
            tapLogger.warn("Table {} not found in tableMap", tableName);
            return null;
        }

        List<Map.Entry<Serializable[], Serializable[]>> rows = eventData.getRows();
        if (rows == null || rows.isEmpty()) {
            return null;
        }
        List<TapEvent> tapEvents = new ArrayList<>();
        for (Map.Entry<Serializable[], Serializable[]> row : rows) {
            Map<String, Object> before = convertRowToMap(row.getKey(), dataTypeMap.get(tableName), enumDataTypeMap.get(tableName));
            Map<String, Object> after = convertRowToMap(row.getValue(), dataTypeMap.get(tableName), enumDataTypeMap.get(tableName));

            TapUpdateRecordEvent updateEvent = new TapUpdateRecordEvent();
            updateEvent.init();
            updateEvent.table(tableName);
            updateEvent.before(before);
            updateEvent.after(after);
            updateEvent.setReferenceTime(event.getHeader().getTimestamp());
            tapEvents.add(updateEvent);
        }

        return tapEvents;
    }

    /**
     * 处理 DELETE 事件
     */
    private List<TapEvent> handleDeleteEvent(Event event) {
        DeleteRowsEventData eventData = (DeleteRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        long tableId = eventData.getTableId();
        String tableName = getTableName(tableId);

        if (tableName == null || !isTableInList(tableName)) {
            return null;
        }

        TapTable tapTable = tableMap.get(tableName);
        if (tapTable == null) {
            tapLogger.warn("Table {} not found in tableMap", tableName);
            return null;
        }

        List<Serializable[]> rows = eventData.getRows();
        if (rows == null || rows.isEmpty()) {
            return null;
        }
        List<TapEvent> tapEvents = new ArrayList<>();
        for (Serializable[] row : rows) {
            Map<String, Object> before = convertRowToMap(row, dataTypeMap.get(tableName), enumDataTypeMap.get(tableName));

            TapDeleteRecordEvent deleteEvent = new TapDeleteRecordEvent();
            deleteEvent.init();
            deleteEvent.table(tableName);
            deleteEvent.before(before);
            deleteEvent.setReferenceTime(event.getHeader().getTimestamp());
            tapEvents.add(deleteEvent);
        }

        return tapEvents;
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
    private String getTableName(long tableId) {
        return tableMapEventByTableId.get(tableId).getTable();
    }

    /**
     * 检查表是否在监听列表中
     */
    private boolean isTableInList(String tableName) {
        if (tableList == null || tableList.isEmpty()) {
            return true; // 如果没有指定表列表，则监听所有表
        }

        // 支持 database.table 格式
        for (String table : tableList) {
            if (tableName.equals(table) || tableName.endsWith("." + table)) {
                return true;
            }
        }

        return false;
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
