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
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
                        ScanEvent queryEvent = new ScanEvent(event, currentBinlogFile.get());
                        queryEvent.setDdlEvents(parseDdlEvents(event));
                        String ddlDatabase = ((QueryEventData) EventDeserializer.EventDataWrapper.internal(event.getData()))
                                .getDatabase();
                        queryEvent.getDdlEvents().forEach(e -> applyDdlToCachedSchema((TapDDLEvent) e, ddlDatabase));
                        concurrentProcessor.runAsyncWithBlocking(queryEvent, this::emit);
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
                ScanEvent scanEvent = new ScanEvent(event, currentBinlogFile.get());
                if (eventType == EventType.WRITE_ROWS || eventType == EventType.EXT_WRITE_ROWS
                        || eventType == EventType.UPDATE_ROWS || eventType == EventType.EXT_UPDATE_ROWS
                        || eventType == EventType.DELETE_ROWS || eventType == EventType.EXT_DELETE_ROWS) {
                    snapshotSchemaForRow(scanEvent, event);
                }
                concurrentProcessor.runAsync(scanEvent, this::emit);
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
                OffsetEvent offsetEvent = new OffsetEvent();
                offsetEvent.setTapEvent(scanEvent.getDdlEvents());
                offsetEvent.setMysqlBinlogPosition(extractBinlogPosition(event, scanEvent.getFileName()));
                return offsetEvent;
            }

            // 处理数据变更事件
            List<TapEvent> tapEvents;
            MysqlBinlogPosition position;

            switch (eventType) {
                case EXT_WRITE_ROWS:
                case WRITE_ROWS:
                    tapEvents = handleInsertEvent(event, scanEvent);
                    break;
                case EXT_UPDATE_ROWS:
                case UPDATE_ROWS:
                    tapEvents = handleUpdateEvent(event, scanEvent);
                    break;
                case EXT_DELETE_ROWS:
                case DELETE_ROWS:
                    tapEvents = handleDeleteEvent(event, scanEvent);
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

    private List<TapEvent> parseDdlEvents(Event event) {
        if (((EventHeaderV4) event.getHeader()).getFlags() == 8) {
            return Collections.emptyList();
        }
        long eventTime = event.getHeader().getTimestamp();
        String ddl = StringKit.removeSqlNote(((QueryEventData) event.getData()).getSql());
        List<TapEvent> ddlEvents = new ArrayList<>();
        try {
            DDLFactory.ddlToTapDDLEvent(ddlParserType, ddl, DDL_WRAPPER_CONFIG, tableMap, tapDDLEvent -> {
                tapDDLEvent.setTime(System.currentTimeMillis());
                tapDDLEvent.setReferenceTime(eventTime);
                tapDDLEvent.setOriginDDL(ddl);
                ddlEvents.add(tapDDLEvent);
                tapLogger.info("Read DDL: " + ddl + ", about to be packaged as some event(s)");
            });
        } catch (Throwable error) {
            TapDDLEvent tapDDLEvent = new TapDDLUnknownEvent();
            tapDDLEvent.setTime(System.currentTimeMillis());
            tapDDLEvent.setReferenceTime(eventTime);
            tapDDLEvent.setOriginDDL(ddl);
            ddlEvents.add(tapDDLEvent);
        }
        return ddlEvents;
    }

    private void snapshotSchemaForRow(ScanEvent scanEvent, Event event) {
        long tableId;
        EventType eventType = event.getHeader().getEventType();
        if (eventType == EventType.WRITE_ROWS || eventType == EventType.EXT_WRITE_ROWS) {
            tableId = ((WriteRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData())).getTableId();
        } else if (eventType == EventType.UPDATE_ROWS || eventType == EventType.EXT_UPDATE_ROWS) {
            tableId = ((UpdateRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData())).getTableId();
        } else {
            tableId = ((DeleteRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData())).getTableId();
        }
        Pair<String, String> databaseTableName = getDatabaseTableName(tableId);
        String key = withSchema
                ? escapeDatabaseAndTable(databaseTableName.getLeft(), databaseTableName.getRight())
                : databaseTableName.getRight();
        LinkedHashMap<String, String> dataTypes = dataTypeMap.get(key);
        Map<String, Object[]> enumMap = enumDataTypeMap.get(key);
        if (dataTypes != null) scanEvent.setDataTypes(new LinkedHashMap<>(dataTypes));
        if (enumMap != null) scanEvent.setEnumMap(new HashMap<>(enumMap));
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
        if (!isTableInObservation(database, table)) {
            return;
        }
        if (withSchema) {
            ddlFlush(database, table);
        } else {
            ddlFlush(table);
        }
        tapLogger.debug("Table map event: tableId={}, database={}, table={}", tableId, database, table);
    }

    private void ddlFlush(String table) {
        refreshTableMetadata(table, table);
    }

    private void ddlFlush(String database, String table) {
        refreshTableMetadata(table, escapeDatabaseAndTable(database, table));
    }

    private void refreshTableMetadata(String table, String tableKey) {
        if (EmptyKit.isBlank(table)) {
            return;
        }
        if (Boolean.TRUE.equals(mysqlConfig.getDoubleActive()) && "_tap_double_active".equals(table)) {
            return;
        }
        // Keep the schema version advanced from binlog DDL events. The connector's
        // table map is updated by the downstream engine asynchronously, so reading
        // it for every TABLE_MAP event can map a post-ADD row using the old schema
        // (or a pre-DROP row using the new schema) during rapid DDL/DML sequences.
        if (dataTypeMap.containsKey(tableKey)) {
            return;
        }
        TapTable tapTable = tableMap.get(tableKey);
        if (tapTable == null || EmptyKit.isEmpty(tapTable.getNameFieldMap())) {
            tapLogger.debug("Skip refreshing metadata for table {}, table metadata is unavailable", tableKey);
            return;
        }
        LinkedHashMap<String, String> dataTypes = tapTable.getNameFieldMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> StringKit.removeParentheses(e.getValue().getDataType()),
                        (existing, replacement) -> existing, LinkedHashMap::new));
        dataTypeMap.put(tableKey, dataTypes);
        Map<String, Object[]> enumMap = generateEnumMap(tapTable.getNameFieldMap().entrySet().stream().filter(v -> v.getValue().getDataType().startsWith("enum")));
        enumDataTypeMap.put(tableKey, enumMap);
    }

    private void applyDdlToCachedSchema(TapDDLEvent ddlEvent, String database) {
        String tableId = ddlEvent.getTableId();
        if (EmptyKit.isBlank(tableId)) {
            return;
        }
        String tableKey = tableId;
        if (withSchema && EmptyKit.isNotBlank(database) && !dataTypeMap.containsKey(tableKey)) {
            tableKey = escapeDatabaseAndTable(database, tableId);
        }
        LinkedHashMap<String, String> current = dataTypeMap.get(tableKey);
        if (current == null) {
            TapTable tapTable = tableMap.get(tableKey);
            if (tapTable == null && withSchema) {
                tapTable = tableMap.get(tableId);
            }
            if (tapTable == null || EmptyKit.isEmpty(tapTable.getNameFieldMap())) {
                return;
            }
            current = tapTable.getNameFieldMap().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> StringKit.removeParentheses(e.getValue().getDataType()),
                            (existing, replacement) -> existing, LinkedHashMap::new));
        }
        LinkedHashMap<String, String> next = new LinkedHashMap<>(current);
        Map<String, Object[]> nextEnums = new HashMap<>(enumDataTypeMap.getOrDefault(tableKey, Collections.emptyMap()));
        if (ddlEvent instanceof TapNewFieldEvent) {
            List<TapField> fields = ((TapNewFieldEvent) ddlEvent).getNewFields();
            if (fields != null) {
                for (TapField field : fields) {
                    if (field != null && EmptyKit.isNotBlank(field.getName())) {
                        Integer position = ddlColumnPosition(String.valueOf(ddlEvent.getOriginDDL()), field.getName(), next);
                        insertFieldAtPosition(next, field.getName(), StringKit.removeParentheses(field.getDataType()),
                                position == null ? field.getPos() : position);
                        nextEnums.remove(field.getName());
                        if (field.getDataType() != null && field.getDataType().startsWith("enum")) {
                            nextEnums.put(field.getName(), parseEnumValues(field.getDataType()));
                        }
                    }
                }
            }
        } else if (ddlEvent instanceof TapDropFieldEvent) {
            String fieldName = ((TapDropFieldEvent) ddlEvent).getFieldName();
            next.remove(fieldName);
            nextEnums.remove(fieldName);
        } else if (ddlEvent instanceof TapAlterFieldNameEvent) {
            io.tapdata.entity.event.ddl.entity.ValueChange<String> change = ((TapAlterFieldNameEvent) ddlEvent).getNameChange();
            if (change != null && next.containsKey(change.getBefore())) {
                LinkedHashMap<String, String> renamed = new LinkedHashMap<>();
                next.forEach((name, type) -> renamed.put(name.equals(change.getBefore()) ? change.getAfter() : name, type));
                next = renamed;
                Object[] enumValues = nextEnums.remove(change.getBefore());
                if (enumValues != null) nextEnums.put(change.getAfter(), enumValues);
            }
        } else if (ddlEvent instanceof TapAlterFieldAttributesEvent) {
            TapAlterFieldAttributesEvent attributes = (TapAlterFieldAttributesEvent) ddlEvent;
            io.tapdata.entity.event.ddl.entity.ValueChange<String> typeChange = attributes.getDataTypeChange();
            if (typeChange != null && EmptyKit.isNotBlank(typeChange.getAfter()) && next.containsKey(attributes.getFieldName())) {
                next.put(attributes.getFieldName(), StringKit.removeParentheses(typeChange.getAfter()));
                nextEnums.remove(attributes.getFieldName());
                if (typeChange.getAfter().startsWith("enum")) {
                    nextEnums.put(attributes.getFieldName(), parseEnumValues(typeChange.getAfter()));
                }
            }
        }
        dataTypeMap.put(tableKey, next);
        enumDataTypeMap.put(tableKey, nextEnums);
    }

    private void insertFieldAtPosition(LinkedHashMap<String, String> fields, String name, String dataType, Integer position) {
        fields.remove(name);
        if (position == null || position < 1 || position > fields.size() + 1) {
            fields.put(name, dataType);
            return;
        }
        LinkedHashMap<String, String> reordered = new LinkedHashMap<>();
        int currentPosition = 1;
        boolean inserted = false;
        for (Map.Entry<String, String> field : fields.entrySet()) {
            if (!inserted && currentPosition++ == position) {
                reordered.put(name, dataType);
                inserted = true;
            }
            reordered.put(field.getKey(), field.getValue());
        }
        if (!inserted) {
            reordered.put(name, dataType);
        }
        fields.clear();
        fields.putAll(reordered);
    }

    private Integer ddlColumnPosition(String ddl, String fieldName, LinkedHashMap<String, String> currentFields) {
        if (EmptyKit.isBlank(ddl)) {
            return null;
        }
        Pattern addColumn = Pattern.compile("(?is)\\bADD\\s+(?:COLUMN\\s+)?(?:IF\\s+NOT\\s+EXISTS\\s+)?[`\\\"\\[]?([\\w$]+)[`\\\"\\]]?\\b([^;]*)");
        Matcher matcher = addColumn.matcher(ddl);
        while (matcher.find()) {
            if (!fieldName.equalsIgnoreCase(matcher.group(1))) {
                continue;
            }
            String definitionTail = matcher.group(2);
            if (Pattern.compile("(?i)\\bFIRST\\s*(?:,|;|$)").matcher(definitionTail).find()) {
                return 1;
            }
            Matcher after = Pattern.compile("(?i)\\bAFTER\\s+[`\\\"\\[]?([\\w$]+)[`\\\"\\]]?\\s*(?:,|;|$)")
                    .matcher(definitionTail);
            if (after.find()) {
                int position = 1;
                for (String existing : currentFields.keySet()) {
                    if (existing.equalsIgnoreCase(after.group(1))) {
                        return position + 1;
                    }
                    position++;
                }
            }
            return null;
        }
        return null;
    }

    private Object[] parseEnumValues(String enumType) {
        String[] rawValues = enumType.substring("enum(".length(), enumType.length() - 1).split(",");
        Object[] values = new Object[rawValues.length];
        for (int i = 0; i < rawValues.length; i++) {
            String value = rawValues[i].trim();
            values[i] = value.startsWith("'") ? StringKit.removeHeadTail(value, "'", null) : new BigDecimal(value);
        }
        return values;
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
    private List<TapEvent> handleInsertEvent(Event event, ScanEvent scanEvent) {
        WriteRowsEventData eventData = (WriteRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        return handleRowsEvent(event, eventData.getTableId(), eventData.getRows(), scanEvent, (row, ctx) -> {
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
    private List<TapEvent> handleUpdateEvent(Event event, ScanEvent scanEvent) {
        UpdateRowsEventData eventData = (UpdateRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        return handleRowsEvent(event, eventData.getTableId(), eventData.getRows(), scanEvent, (row, ctx) -> {
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
    private List<TapEvent> handleDeleteEvent(Event event, ScanEvent scanEvent) {
        DeleteRowsEventData eventData = (DeleteRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
        return handleRowsEvent(event, eventData.getTableId(), eventData.getRows(), scanEvent, (row, ctx) -> {
            TapDeleteRecordEvent deleteEvent = new TapDeleteRecordEvent();
            deleteEvent.init();
            deleteEvent.table(ctx.tableName);
            deleteEvent.before(convertRowToMap(row, ctx.dataTypes, ctx.enumMap));
            deleteEvent.setReferenceTime(ctx.timestamp);
            return deleteEvent;
        });
    }

    private <R> List<TapEvent> handleRowsEvent(Event event, long tableId, List<R> rows, ScanEvent scanEvent, BiFunction<R, RowChangeContext, TapBaseEvent> mapper) {
        RowChangeContext ctx = prepareRowChangeContext(event, tableId, scanEvent);
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

    private RowChangeContext prepareRowChangeContext(Event event, long tableId, ScanEvent scanEvent) {
        Pair<String, String> databaseTableName = getDatabaseTableName(tableId);
        String database = databaseTableName.getLeft();
        String tableName = databaseTableName.getRight();

        if (tableName == null || !isTableInObservation(database, tableName)) {
            return null;
        }

        TapTable tapTable = withSchema ? tableMap.get(escapeDatabaseAndTable(database, tableName)) : tableMap.get(tableName);
        if (tapTable == null) {
            tapLogger.warn("Table {} not found in tableMap", tableName);
            return null;
        }

        return new RowChangeContext(database, tableName,
                scanEvent.getDataTypes() != null ? scanEvent.getDataTypes()
                        : (withSchema ? dataTypeMap.get(escapeDatabaseAndTable(database, tableName)) : dataTypeMap.get(tableName)),
                scanEvent.getEnumMap() != null ? scanEvent.getEnumMap()
                        : (withSchema ? enumDataTypeMap.get(escapeDatabaseAndTable(database, tableName)) : enumDataTypeMap.get(tableName)),
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

    private String escapeDatabaseAndTable(String database, String table) {
        return StringKit.escape(database, '.') + "." + StringKit.escape(table, '.');
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
        private List<TapEvent> ddlEvents;
        private LinkedHashMap<String, String> dataTypes;
        private Map<String, Object[]> enumMap;

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

        public List<TapEvent> getDdlEvents() { return ddlEvents; }
        public void setDdlEvents(List<TapEvent> ddlEvents) { this.ddlEvents = ddlEvents; }
        public LinkedHashMap<String, String> getDataTypes() { return dataTypes; }
        public void setDataTypes(LinkedHashMap<String, String> dataTypes) { this.dataTypes = dataTypes; }
        public Map<String, Object[]> getEnumMap() { return enumMap; }
        public void setEnumMap(Map<String, Object[]> enumMap) { this.enumMap = enumMap; }
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
