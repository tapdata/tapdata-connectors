package io.tapdata.connector.postgres.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.config.PostgresDebeziumConfig;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffsetStorage;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.error.PostgresErrorCode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.exception.TapCodeException;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.kit.NumberKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.codehaus.plexus.util.StringUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * CDC runner for Postgresql
 *
 * @author Jarad
 * @date 2022/5/13
 */
public class PostgresCdcRunner extends DebeziumCdcRunner {

    private static final String TAG = PostgresCdcRunner.class.getSimpleName();
    private final PostgresConfig postgresConfig;
    private final PostgresJdbcContext postgresJdbcContext;
    private final TapConnectorContext connectorContext;
    private PostgresDebeziumConfig postgresDebeziumConfig;
    private int recordSize;
    private StreamReadConsumer consumer;
    private final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
    protected TimeZone timeZone;
    private String dropTransactionId = null;
    private boolean withSchema = false;
    private final Map<String, Boolean> replicaFull = new HashMap<>();
    private static final DDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("\"");
    private final DDLParserType ddlParserType = DDLParserType.POSTGRES_CCJ_SQL_PARSER;

    // ── CDC delayed LSN advancement ──────────────────────────────────────
    //
    // Design: slot LSN is NOT advanced on every batch. Instead the connector
    // maintains an in-memory offset and writes {commit_time -> full sourceOffset}
    // checkpoints to stateMap periodically. After N hours the oldest checkpoint is
    // used to advance the slot, and then removed. This keeps WAL retention
    // bounded to ~N hours while still allowing point-in-time CDC backfill via
    // findNearestCheckpointBefore (which scans the checkpoint index).
    //
    // Crash safety: slot is advanced BEFORE the KV entry is deleted, so a
    // crash between the two steps still has the WAL covered.

    private static final long CDC_CHECKPOINT_INTERVAL_MS = 3 * 60 * 1000; // 3 min, shortened for testing
    private static final String CDC_CP_PREFIX = "cdc_cp_";          // per-checkpoint key
    private static final String CDC_CP_INDEX = "cdc_cp_index";      // sorted ts list
    private static final String CDC_LAST_CP_TIME = "cdc_last_cp_time"; // wall-clock of last checkpoint

    /** Cache of last checkpoint wall-clock time — avoids stateMap read on every flushOffset. */
    private final AtomicLong cachedLastCpTime = new AtomicLong();

    private volatile Long filterStartTimeMs;


    public PostgresCdcRunner(PostgresJdbcContext postgresJdbcContext, TapConnectorContext connectorContext) throws SQLException {
        this.postgresJdbcContext = postgresJdbcContext;
        this.postgresConfig = (PostgresConfig) postgresJdbcContext.getConfig();
        this.connectorContext = connectorContext;
        if (postgresConfig.getOldVersionTimezone()) {
            this.timeZone = postgresJdbcContext.queryTimeZone();
        } else {
            this.timeZone = TimeZone.getTimeZone("GMT" + postgresConfig.getTimezone());
        }
    }

    public PostgresCdcRunner useSlot(String slotName) {
        this.runnerName = slotName;
        return this;
    }

    private static final String PG_QUERY_REPLICA_FULL = "select relname from pg_class where relnamespace=(select oid from pg_namespace where nspname='%s') and relreplident='f' and relname in ('%s')";

    private Map<String, Boolean> getReplicaFullTables(String schema, List<String> tables) {
        Map<String, Boolean> replicaFull = new HashMap<>();
        if (EmptyKit.isEmpty(tables)) {
            return replicaFull;
        }
        try {
            postgresJdbcContext.query(String.format(PG_QUERY_REPLICA_FULL, schema, String.join("','", tables)), resultSet -> {
                while (resultSet.next()) {
                    replicaFull.put(schema + "." + resultSet.getString(1), true);
                }
            });
        } catch (Exception ignored) {
        }
        return replicaFull;
    }

    public PostgresCdcRunner watch(List<String> observedTableList) {
        withSchema = false;
        if (postgresConfig.getDoubleActive()) {
            observedTableList.add("_tap_double_active");
        }
        replicaFull.putAll(getReplicaFullTables(postgresConfig.getSchema(), observedTableList));
        appendSubPartitionTables(connectorContext, observedTableList);
        postgresDebeziumConfig = new PostgresDebeziumConfig()
                .use(postgresConfig)
                .use(timeZone)
                .useSlot(runnerName)
                .watch(observedTableList);
        return this;
    }

    public PostgresCdcRunner watch(Map<String, List<String>> schemaTableMap) {
        withSchema = true;
        if (postgresConfig.getDoubleActive()) {
            schemaTableMap.computeIfPresent(postgresConfig.getSchema(), (k, v) -> {
                v.add("_tap_double_active");
                return v;
            });
        }
        schemaTableMap.forEach((schema, tables) -> replicaFull.putAll(getReplicaFullTables(schema, tables)));
        appendSubPartitionTables(connectorContext, schemaTableMap);
        postgresDebeziumConfig = new PostgresDebeziumConfig()
                .use(postgresConfig)
                .use(timeZone)
                .useSlot(runnerName)
                .watch(schemaTableMap);
        return this;
    }

    public PostgresCdcRunner offset(Object offsetState) {
        if (EmptyKit.isNull(offsetState)) {
            this.filterStartTimeMs = null;  // clear any stale time filter
            PostgresOffsetStorage.DBZ_OFFSET.put(runnerName, new PostgresOffset());
        } else if (offsetState instanceof PostgresOffset) {
            this.filterStartTimeMs = null;  // clear any stale time filter
            PostgresOffsetStorage.DBZ_OFFSET.put(runnerName, (PostgresOffset) offsetState);
        } else if (offsetState instanceof Long) {
            // Time-based CDC: offsetState is a timestamp in epoch milliseconds
            Long targetTimestampMs = (Long) offsetState;
            try {
                // Fast-scan WAL headers to find LSN corresponding to the target timestamp
                Long startLsn = seekWalToTimestamp(targetTimestampMs);
                if (startLsn != null) {
                    PostgresOffset resolvedOffset = new PostgresOffset();
                    resolvedOffset.setOffsetValue(startLsn);
                    this.filterStartTimeMs = targetTimestampMs;
                    PostgresOffsetStorage.DBZ_OFFSET.put(runnerName, resolvedOffset);
                    TapLogger.info(TAG, "Resolved timestamp {} ms to LSN: {}",
                            targetTimestampMs, formatLsn(startLsn));
                } else {
                    throw new RuntimeException("Failed to find LSN for timestamp: " + targetTimestampMs
                            + " - WAL may have been cleaned up or timestamp is in the future");
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to seek WAL to timestamp " + targetTimestampMs, e);
            }
        } else {
            throw new IllegalArgumentException(
                    "Unsupported offset type: " + offsetState.getClass().getName()
                    + " — expected PostgresOffset or Long (timestamp ms for time-based CDC)");
        }
        return this;
    }

    public AtomicReference<Throwable> getThrowable() {
        return throwableAtomicReference;
    }

    public void registerConsumer(StreamReadConsumer consumer, int recordSize) {
        this.recordSize = recordSize;
        this.consumer = consumer;
        //build debezium engine
        this.engine = (EmbeddedEngine) EmbeddedEngine.create()
                .using(postgresDebeziumConfig.create())
                .using(new DebeziumEngine.ConnectorCallback() {
                    @Override
                    public void taskStarted() {
                        DebeziumEngine.ConnectorCallback.super.taskStarted();
                        consumer.streamReadStarted();
                    }

                    @Override
                    public void taskStopped() {
                        DebeziumEngine.ConnectorCallback.super.taskStopped();
                        consumer.streamReadEnded();
                    }
                })
//                .using(this.getClass().getClassLoader())
//                .using(Clock.SYSTEM)
//                .notifying(this::consumeRecord)
                .using((numberOfMessagesSinceLastCommit, timeSinceLastCommit) ->
                        numberOfMessagesSinceLastCommit >= recordSize || timeSinceLastCommit.getSeconds() >= 5)
                .notifying(this::consumeRecords).using((result, message, throwable) -> {
                    if (result) {
                        if (StringUtils.isNotBlank(message)) {
                            TapLogger.info(TAG, "CDC engine stopped: " + message);
                        } else {
                            TapLogger.info(TAG, "CDC engine stopped");
                        }
                    } else {
                        if (null != throwable) {
                            if (StringUtils.isNotBlank(message)) {
                                throwableAtomicReference.set(new RuntimeException(message, throwable));
                            } else {
                                throwableAtomicReference.set(new RuntimeException(throwable));
                            }
                        } else {
                            throwableAtomicReference.set(new RuntimeException(message));
                        }
                    }
                    consumer.streamReadEnded();
                })
                .build();
    }

    @Override
    public void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) throws InterruptedException {
        super.consumeRecords(sourceRecords, committer);
        List<TapEvent> eventList = TapSimplify.list();
        Map<String, ?> offset = null;
        for (SourceRecord sr : sourceRecords) {
            offset = sr.sourceOffset();
            // PG use micros to indicate the time but in pdk api we use millis
            Object tsUsecObj = offset.get("ts_usec");
            Long referenceTime = tsUsecObj instanceof Number
                    ? ((Number) tsUsecObj).longValue() / 1000
                    : null;

            Struct struct = ((Struct) sr.value());
            if (struct == null) {
                continue;
            }
            // ── heartbeat: always emit regardless of time filter ──
            // Debezium heartbeats may not carry ts_usec; they use
            // ts_ms instead. Process them before the time filter
            // so idle-period offset advancement is not blocked.
            if ("io.debezium.connector.common.Heartbeat".equals(sr.valueSchema().name())) {
                eventList.add(new HeartbeatEvent().init().referenceTime(struct.getInt64("ts_ms")));
                continue;
            }
            // ── time-based CDC filter ──────────────────────────────
            // Skip DML/DDL events whose commit time is before the
            // requested start time. This handles the gap between the
            // checkpoint LSN and the target offsetStartTime.
            if (referenceTime == null) {
                continue; // malformed DML record (no ts_usec) — skip
            }
            if (filterStartTimeMs != null && referenceTime < filterStartTimeMs) {
                continue;
            }
            if (EmptyKit.isNull(sr.valueSchema().field("op"))) {
                continue;
            }
            String op = struct.getString("op");
            String lsn = String.valueOf(offset.get("lsn"));
            String table = struct.getStruct("source").getString("table");
            String schema = struct.getStruct("source").getString("schema");

            // ── DDL Trigger: intercept DDL audit table records ──
            if (Boolean.TRUE.equals(postgresConfig.getDdlTriggerEnable())
                    && isDdlAuditRecord(schema, table)) {
                // The audit table only ever has INSERT (and DELETE) — we only care about INSERT
                if ("c".equals(op)) {
                    Struct after = struct.getStruct("after");
                    if (after != null) {
                        String ddlSchema = after.getString("c_schema");
                        String ddlQuery = after.getString("c_ddlqry");
                        if (ddlQuery != null && !ddlQuery.isEmpty()) {
                            PostgresOffset ddlOffset = new PostgresOffset();
                            ddlOffset.setSourceOffset(TapSimplify.toJson(offset));
                            try {
                                DDLFactory.ddlToTapDDLEvent(
                                        ddlParserType,
                                        ddlQuery,
                                        DDL_WRAPPER_CONFIG,
                                        connectorContext.getTableMap(),
                                        tapDDLEvent -> {
                                            tapDDLEvent.setTime(System.currentTimeMillis());
                                            tapDDLEvent.setReferenceTime(referenceTime);
                                            tapDDLEvent.setOriginDDL(ddlQuery);
                                            tapDDLEvent.setExactlyOnceId(lsn);
                                            if (withSchema && EmptyKit.isNotBlank(ddlSchema) && EmptyKit.isNotBlank(tapDDLEvent.getTableId())) {
                                                tapDDLEvent.setNamespaces(Arrays.asList(ddlSchema, tapDDLEvent.getTableId()));
                                            }
                                            consumer.accept(Collections.singletonList(tapDDLEvent), ddlOffset);
                                        }
                                );
                            } catch (Throwable e) {
                                TapDDLEvent tapDDLEvent = new TapDDLUnknownEvent();
                                tapDDLEvent.setTime(System.currentTimeMillis());
                                tapDDLEvent.setReferenceTime(referenceTime);
                                tapDDLEvent.setOriginDDL(ddlQuery);
                                tapDDLEvent.setExactlyOnceId(lsn);
                                if (withSchema && EmptyKit.isNotBlank(ddlSchema) && EmptyKit.isNotBlank(tapDDLEvent.getTableId())) {
                                    tapDDLEvent.setNamespaces(Arrays.asList(ddlSchema, tapDDLEvent.getTableId()));
                                }
                                consumer.accept(Collections.singletonList(tapDDLEvent), ddlOffset);
                            }
                        }
                    }
                }
                // Don't process audit table records as normal DML — skip to next record
                continue;
            }

            //双活情形下，需要过滤_tap_double_active记录的同事务数据
            if (Boolean.TRUE.equals(postgresConfig.getDoubleActive())) {
                if ("_tap_double_active".equals(table)) {
                    dropTransactionId = String.valueOf(sr.sourceOffset().get("transaction_id"));
                    continue;
                } else {
                    if (null != dropTransactionId) {
                        if (dropTransactionId.equals(String.valueOf(sr.sourceOffset().get("transaction_id")))) {
                            continue;
                        } else {
                            dropTransactionId = null;
                        }
                    }
                }
            }
            Struct after = struct.getStruct("after");
            Struct before;
            if (Boolean.TRUE.equals(replicaFull.get(schema + "." + table)) || EmptyKit.isNull(sr.key())) {
                before = struct.getStruct("before");
            } else {
                before = (Struct) sr.key();
            }
            TapRecordEvent event = null;
            switch (op) { //snapshot.mode = 'never'
                case "c": //after running --insert
                case "r": //after slot but before running --read
                    event = new TapInsertRecordEvent().init().table(table).after(getMapFromStruct(after));
                    break;
                case "d": //after running --delete
                    event = new TapDeleteRecordEvent().init().table(table).before(getMapFromStruct(before));
                    break;
                case "u": //after running --update
                    event = new TapUpdateRecordEvent().init().table(table).after(getMapFromStruct(after)).before(getMapFromStruct(before));
                    break;
                default:
                    break;
            }
            if (EmptyKit.isNotNull(event)) {
                event.setReferenceTime(referenceTime);
                event.setExactlyOnceId(lsn);
                if (withSchema) {
                    event.setNamespaces(Lists.newArrayList(schema, table));
                }
            }
            eventList.add(event);
            if (eventList.size() >= recordSize) {
                PostgresOffset postgresOffset = new PostgresOffset();
                postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
                consumer.accept(eventList, postgresOffset);
                eventList = TapSimplify.list();
            }
        }
        if (EmptyKit.isNotEmpty(eventList)) {
            PostgresOffset postgresOffset = new PostgresOffset();
            postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
            consumer.accept(eventList, postgresOffset);
        }
    }

    private DataMap getMapFromStruct(Struct struct) {
        DataMap dataMap = new DataMap();
        if (EmptyKit.isNull(struct)) {
            return dataMap;
        }
        struct.schema().fields().forEach(field -> {
            Object obj = struct.getWithoutDefault(field.name());
            if (isDebeziumUnavailableValue(obj)) {
                return;
            }
            if (obj instanceof ByteBuffer) {
                obj = struct.getBytes(field.name());
            } else if (obj instanceof Struct) {
                obj = new BigDecimal(NumberKit.debeziumBytes2long(((Struct) obj).getBytes("value"))).divide(BigDecimal.TEN.pow((int) ((Struct) obj).get("scale")));
            } else if (obj instanceof String && EmptyKit.isNotNull(field.schema().name())) {
                if (field.schema().name().endsWith("ZonedTimestamp")) {
                    obj = Instant.parse((String) obj).atZone(ZoneOffset.UTC);
                } else if (field.schema().name().endsWith("ZonedTime")) {
                    obj = LocalTime.parse(((String) obj).replace("Z", "")).atDate(LocalDate.ofYearDay(1970, 1)).atZone(ZoneOffset.UTC);
                }
            }
            dataMap.put(field.name(), obj);
        });
        return dataMap;
    }

    private static final byte[] DEBEZIUM_UNAVAILABLE_BYTES = {
            95, 95, 100, 101, 98, 101, 122, 105, 117, 109, 95, 117, 110, 97,
            118, 97, 105, 108, 97, 98, 108, 101, 95, 118, 97, 108, 117, 101
    };

    private static final String DEBEZIUM_UNAVAILABLE_HEX = "5F5F646562657A69756D5F756E617661696C61626C655F76616C7565";

    public static boolean isDebeziumUnavailableValue(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj instanceof byte[]) {
            return Arrays.equals((byte[]) obj, DEBEZIUM_UNAVAILABLE_BYTES);
        }

        if (obj instanceof String) {
            String str = (String) obj;
            if ("__debezium_unavailable_value".equals(str)) {
                return true;
            }
            if (DEBEZIUM_UNAVAILABLE_HEX.equalsIgnoreCase(str)) {
                return true;
            }
        }

        return "__debezium_unavailable_value".equals(obj.toString());
    }

    /**
     * Append sub partition tables for master tables
     *
     * @param connectorContext node context
     * @param tables           watch table, can be partition table or normal table, only process partition table
     */
    private void appendSubPartitionTables(TapConnectorContext connectorContext, List<String> tables) {

        if (connectorContext == null || EmptyKit.isEmpty(tables)) {
            return;
        }
        KVReadOnlyMap<TapTable> tableMap = connectorContext.getTableMap();
        tables.addAll(getSubPartitionTables(tableMap, tables));
    }

    /**
     * Append sub partition tables for master tables
     *
     * @param connectorContext node context
     * @param schemaTableMap   schema and table map, can be partition table or normal table, only process partition table
     */
    private void appendSubPartitionTables(TapConnectorContext connectorContext, Map<String, List<String>> schemaTableMap) {
        if (connectorContext == null || EmptyKit.isEmpty(schemaTableMap)) {
            return;
        }
        schemaTableMap.forEach((schema, tables) ->
                tables.addAll(getSubPartitionTables(connectorContext.getTableMap(), tables))
        );
    }

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
                    subPartitionTableNames.addAll(
                            tableInfo.getPartitionInfo().getSubPartitionTableInfo()
                                    .stream().filter(Objects::nonNull)
                                    .map(TapSubPartitionTableInfo::getTableName)
                                    .filter(n -> !tables.contains(n))
                                    .collect(Collectors.toList())
                    );
                }
            }
        });
        return subPartitionTableNames;
    }

    // ── DDL Trigger (Attunity-style event trigger capture) ───────────────

    /**
     * Set up the DDL event trigger artifacts in the source database.
     * <p>
     * Creates three objects:
     * <ol>
     *   <li>{@code _tapdata_ddl_audit} — audit table that briefly holds DDL statements</li>
     *   <li>{@code _tapdata_intercept_ddl()} — event trigger function</li>
     *   <li>{@code _tapdata_intercept_ddl} — event trigger on {@code ddl_command_end}</li>
     * </ol>
     * <p>
     * The event trigger writes DDL to the audit table (which creates WAL records
     * that Debezium captures), then immediately deletes the row to keep the table empty.
     * <p>
     * <b>Only works with logical replication plugins</b> (pgoutput, wal2json, decoderbufs).
     * Requires superuser privileges. The audit table must be included in the Debezium
     * table whitelist.
     *
     * @param schema the schema where DDL artifacts will be created
     * @throws TapCodeException if creation fails
     */
    public void setupDdlTrigger(String schema) {
        if (!Boolean.TRUE.equals(postgresConfig.getDdlTriggerEnable())) {
            return;
        }
        String plugin = postgresConfig.getLogPluginName();
        if (!"pgoutput".equals(plugin) && !"wal2json".equals(plugin) && !"decoderbufs".equals(plugin)) {
            throw new TapCodeException(PostgresErrorCode.DDL_TRIGGER_UNSUPPORTED_PLUGIN,
                    "DDL trigger requires a logical replication plugin (pgoutput, wal2json, decoderbufs), but current plugin is: " + plugin)
                    .dynamicDescriptionParameters(plugin);
        }
        // Use the configured DDL trigger schema, or fall back to the connection schema
        String targetSchema = EmptyKit.isNotBlank(postgresConfig.getDdlTriggerSchema())
                ? postgresConfig.getDdlTriggerSchema()
                : "public";

        TapLogger.info(TAG, "Setting up DDL event trigger in schema: {}", targetSchema);

        // 0. Check superuser privilege (required for CREATE EVENT TRIGGER)
        try {
            checkSuperuserPrivilege();
        } catch (SQLException e) {
            throw new TapCodeException(PostgresErrorCode.CREATE_DDL_TRIGGER_FAILED,
                    "Failed to verify superuser privilege: " + e.getMessage(), e)
                    .dynamicDescriptionParameters(e.getMessage());
        }

        // Create all three artifacts in a single transaction for atomicity
        try (java.sql.Connection conn = postgresJdbcContext.getConnection()) {
            boolean autoCommit = conn.getAutoCommit();
            try {
                conn.setAutoCommit(false);
                try (java.sql.Statement stmt = conn.createStatement()) {
                    // 1. Create audit table (idempotent)
                    String createTableSql = PostgresJdbcContext.buildCreateDdlAuditTableSql(targetSchema);
                    TapLogger.debug(TAG, "Executing: {}", createTableSql);
                    stmt.execute(createTableSql);

                    // 2. Create event trigger function (idempotent)
                    String createFuncSql = PostgresJdbcContext.buildCreateDdlTriggerFunctionSql(targetSchema);
                    TapLogger.debug(TAG, "Executing: {}", createFuncSql);
                    stmt.execute(createFuncSql);

                    // 3. Create event trigger if it doesn't already exist
                    String checkTriggerSql = PostgresJdbcContext.buildCheckDdlEventTriggerSql();
                    boolean triggerExists = false;
                    try (java.sql.ResultSet rs = stmt.executeQuery(checkTriggerSql)) {
                        if (rs.next()) {
                            triggerExists = rs.getInt(1) > 0;
                        }
                    }
                    if (!triggerExists) {
                        String createTriggerSql = PostgresJdbcContext.buildCreateDdlEventTriggerSql(targetSchema);
                        TapLogger.debug(TAG, "Executing: {}", createTriggerSql);
                        stmt.execute(createTriggerSql);
                    } else {
                        TapLogger.info(TAG, "DDL event trigger already exists, skipping creation");
                    }
                }
                conn.commit();
                TapLogger.info(TAG, "DDL event trigger setup complete in schema: {}", targetSchema);
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException ignored) {
                }
                throw new TapCodeException(PostgresErrorCode.CREATE_DDL_TRIGGER_FAILED,
                        "Failed to set up DDL event trigger artifacts in schema " + targetSchema + ": " + e.getMessage(), e)
                        .dynamicDescriptionParameters(e.getMessage());
            } finally {
                try {
                    conn.setAutoCommit(autoCommit);
                } catch (SQLException ignored) {
                }
            }
        } catch (SQLException e) {
            throw new TapCodeException(PostgresErrorCode.CREATE_DDL_TRIGGER_FAILED,
                    "Failed to obtain database connection for DDL trigger setup: " + e.getMessage(), e)
                    .dynamicDescriptionParameters(e.getMessage());
        }
    }

    /**
     * Verify that the current database user has superuser privileges.
     * <p>
     * Event triggers in PostgreSQL require superuser to create.
     * Fails fast with a clear error message if the user is not a superuser.
     */
    private void checkSuperuserPrivilege() throws SQLException {
        final boolean[] isSuperuser = {false};
        postgresJdbcContext.queryWithNext(
                "SELECT usesuper FROM pg_user WHERE usename = current_user",
                rs -> isSuperuser[0] = rs.getBoolean("usesuper"));
        if (!isSuperuser[0]) {
            throw new TapCodeException(PostgresErrorCode.CREATE_DDL_TRIGGER_FAILED,
                    "DDL event trigger creation requires PostgreSQL superuser privileges, " +
                            "but the current user is not a superuser. " +
                            "Connect as a superuser (e.g., 'postgres') or grant superuser to the current user.");
        }
    }

    /**
     * Clean up the DDL event trigger artifacts from the source database.
     * <p>
     * Best-effort cleanup — errors are logged but not rethrown so they
     * don't block the connector shutdown.
     *
     * @param schema the schema where DDL artifacts were created
     */
    public void cleanupDdlTrigger(String schema) {
        if (!Boolean.TRUE.equals(postgresConfig.getDdlTriggerEnable())) {
            return;
        }
        String targetSchema = EmptyKit.isNotBlank(postgresConfig.getDdlTriggerSchema())
                ? postgresConfig.getDdlTriggerSchema()
                : "public";

        TapLogger.info(TAG, "Cleaning up DDL event trigger artifacts from schema: {}", targetSchema);

        // Drop order: trigger → function → table (dependency order)
        ErrorKit.ignoreAnyError(() -> {
            String sql = PostgresJdbcContext.buildDropDdlEventTriggerSql();
            TapLogger.debug(TAG, "Executing: {}", sql);
            postgresJdbcContext.execute(sql);
            TapLogger.debug(TAG, "Dropped DDL event trigger");
        });
        ErrorKit.ignoreAnyError(() -> {
            String sql = PostgresJdbcContext.buildDropDdlTriggerFunctionSql(targetSchema);
            TapLogger.debug(TAG, "Executing: {}", sql);
            postgresJdbcContext.execute(sql);
            TapLogger.debug(TAG, "Dropped DDL trigger function");
        });
        ErrorKit.ignoreAnyError(() -> {
            String sql = PostgresJdbcContext.buildDropDdlAuditTableSql(targetSchema);
            TapLogger.debug(TAG, "Executing: {}", sql);
            postgresJdbcContext.execute(sql);
            TapLogger.debug(TAG, "Dropped DDL audit table");
        });

        TapLogger.info(TAG, "DDL event trigger cleanup complete");
    }

    /**
     * Check whether the current Debezium record represents a DDL event
     * (i.e., an INSERT into the DDL audit table).
     */
    private boolean isDdlAuditRecord(String schema, String table) {
        String auditSchema = EmptyKit.isNotBlank(postgresConfig.getDdlTriggerSchema())
                ? postgresConfig.getDdlTriggerSchema()
                : "public";
        return PostgresJdbcContext.DDL_AUDIT_TABLE.equals(table)
                && auditSchema.equals(schema);
    }

    public void processFlushOffset(PostgresOffset offset) {
        String sourceOffset = offset.getSourceOffset();
        if (sourceOffset == null) {
            return;
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> offsetMap = objectMapper.readValue(sourceOffset, Map.class);
            if (getKeepWalHoursOrZero() == 0) {
                flushOffset(offsetMap);
                return;
            }
            // ── extract LSN ──────────────────────────────────────────
            Object lsnObj = offsetMap.get("lsn");
            if (lsnObj == null) return;
            long lsn = lsnObj instanceof Number
                    ? ((Number) lsnObj).longValue()
                    : parseLsnToLong(lsnObj.toString());

            // ── extract commit timestamp (from WAL, not wall-clock) ──
            Object tsObj = offsetMap.get("ts_usec"); // Debezium PG connector field
            Long commitTimeUsec = parseLongValue(tsObj);
            long commitTimeMs = commitTimeUsec == null
                    ? System.currentTimeMillis()
                    : commitTimeUsec / 1000;

            // ── periodic checkpoint ──────────────────────────────────
            long now = System.currentTimeMillis();
            long lastCpTime = cachedLastCpTime.get();
            if (lastCpTime == 0) {
                Object lastCpObj = connectorContext.getStateMap().get(CDC_LAST_CP_TIME);
                Long persistedLastCpTime = parseLongValue(lastCpObj);
                lastCpTime = persistedLastCpTime == null ? 0 : persistedLastCpTime;
                if (lastCpTime > 0) {
                    cachedLastCpTime.compareAndSet(0, lastCpTime);
                    lastCpTime = cachedLastCpTime.get(); // read the settled value
                }
            }
            if (lastCpTime == 0 || (now - lastCpTime) >= CDC_CHECKPOINT_INTERVAL_MS) {
                persistCheckpoint(commitTimeMs, lsn, sourceOffset, now);
                cleanExpiredCheckpoints(now);
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, "processFlushOffset error: {}", e.getMessage(), e);
        }
    }

    // ── persist a single {commit_time → full sourceOffset} checkpoint to stateMap ──────

    private void persistCheckpoint(long commitTimeMs, long lsn, String sourceOffset, long now) {
        String cpKey = CDC_CP_PREFIX + commitTimeMs;
        connectorContext.getStateMap().put(cpKey, sourceOffset);

        // Maintain a sorted index so we can binary-search later
        List<Long> timestamps = readCheckpointIndex();
        if (!timestamps.contains(commitTimeMs)) {
            timestamps.add(commitTimeMs);
            Collections.sort(timestamps);
            writeCheckpointIndex(timestamps);
        }
        connectorContext.getStateMap().put(CDC_LAST_CP_TIME, now);
        cachedLastCpTime.set(now);
        TapLogger.debug(TAG, "CDC checkpoint persisted: commitTime={}, lsn={}, indexSize={}",
                commitTimeMs, lsnToString(lsn), timestamps.size());
    }

    // ── expire checkpoints outside the window, keeping the cutoff anchor ─

    private void cleanExpiredCheckpoints(long now) {
        int keepWalHours = getKeepWalHoursOrZero();
        long retentionMs = keepWalHours * 60 * 60 * 1000L;
        long cutoff = now - retentionMs;

        List<Long> timestamps = readCheckpointIndex();
        if (timestamps.isEmpty()) return;

        int anchorIndex = findLastCheckpointIndexBefore(timestamps, cutoff);
        if (anchorIndex < 0) return; // nothing expired yet

        Long anchorTime = timestamps.get(anchorIndex);
        Map<String, Object> anchorOffset = readCheckpointSourceOffset(anchorTime);
        if (anchorOffset == null) {
            // Orphaned or unreadable anchor — remove the bad index entry and retry later.
            connectorContext.getStateMap().put(CDC_CP_PREFIX + anchorTime, null);
            timestamps.remove(anchorIndex);
            writeCheckpointIndex(timestamps);
            return;
        }
        Long anchorLsn = extractLsn(anchorOffset);
        if (anchorLsn == null) {
            // Corrupt checkpoint: LSN is unreadable. Remove it from the index
            // so it doesn't block future cleanups, but DON'T touch the slot.
            TapLogger.warn(TAG, "CDC checkpoint at {} has an unreadable LSN — removing from index without advancing slot", anchorTime);
            connectorContext.getStateMap().put(CDC_CP_PREFIX + anchorTime, null);
            timestamps.remove(anchorIndex);
            writeCheckpointIndex(timestamps);
            return;
        }

        // Step 1: advance slot FIRST (crash-safe — if we crash here,
        //         the checkpoint is still in stateMap and WAL is preserved).
        try {
            flushOffset(anchorOffset);
            TapLogger.info(TAG, "CDC slot advanced to checkpoint at {}, LSN {}",
                    anchorTime, lsnToString(anchorLsn));
        } catch (Exception e) {
            TapLogger.warn(TAG, "CDC failed to advance slot LSN: {}", e.getMessage());
            return; // don't delete checkpoint if slot not advanced
        }

        // Step 2: NOW delete checkpoints older than the anchor.
        if (anchorIndex > 0) {
            List<Long> expired = new ArrayList<>(timestamps.subList(0, anchorIndex));
            for (Long expiredTime : expired) {
                connectorContext.getStateMap().put(CDC_CP_PREFIX + expiredTime, null);
            }
            timestamps.subList(0, anchorIndex).clear();
            writeCheckpointIndex(timestamps);
        }

        // Guard against index bloat: warn if index exceeds the expected size
        // for the configured retention window (one entry per checkpoint interval).
        long maxExpected = (keepWalHours * 60 * 60 * 1000L) / CDC_CHECKPOINT_INTERVAL_MS + 5;
        if (timestamps.size() > maxExpected * 2) {
            TapLogger.warn(TAG, "CDC checkpoint index has {} entries, exceeding the expected maximum of {} "
                    + "for a {}h retention window — old checkpoints may not be cleaning up correctly",
                    timestamps.size(), maxExpected, keepWalHours);
        }

        TapLogger.debug(TAG, "CDC expired checkpoints before anchor {}, remaining: {}",
                anchorTime, timestamps.size());
    }

    // ── stateMap index helpers ───────────────────────────────────────────

    private List<Long> readCheckpointIndex() {
        Object raw = connectorContext.getStateMap().get(CDC_CP_INDEX);
        if (raw instanceof String && !((String) raw).isEmpty()) {
            String[] parts = ((String) raw).split(",");
            List<Long> list = new ArrayList<>(parts.length);
            for (String p : parts) {
                try { list.add(Long.parseLong(p.trim())); } catch (NumberFormatException ignored) {}
            }
            return list;
        }
        return new ArrayList<>();
    }

    private void writeCheckpointIndex(List<Long> timestamps) {
        connectorContext.getStateMap().put(CDC_CP_INDEX,
                timestamps.stream().map(String::valueOf).collect(Collectors.joining(",")));
    }

    public Map<String, Object> readCheckpointSourceOffset(Long checkpointTime) {
        Object raw = connectorContext.getStateMap().get(CDC_CP_PREFIX + checkpointTime);
        if (raw == null) return null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            if (raw instanceof String) {
                return objectMapper.readValue((String) raw, Map.class);
            }
            if (raw instanceof Map) {
                return objectMapper.convertValue(raw, Map.class);
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, "Failed to read CDC checkpoint source offset at {}: {}", checkpointTime, e.getMessage());
        }
        return null;
    }

    // ── binary search / LSN / retention helpers ───────────────────────────

    private static int findLastCheckpointIndexBefore(List<Long> timestamps, long targetTimeMs) {
        int lo = 0, hi = timestamps.size() - 1;
        int best = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long midVal = timestamps.get(mid);
            if (midVal <= targetTimeMs) {
                best = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return best;
    }

    private Long extractLsn(Map<String, Object> sourceOffset) {
        if (sourceOffset == null) return null;
        Object lsnObj = sourceOffset.get("lsn");
        if (lsnObj == null) return null;
        try {
            return lsnObj instanceof Number
                    ? ((Number) lsnObj).longValue()
                    : parseLsnToLong(lsnObj.toString());
        } catch (Exception e) {
            TapLogger.warn(TAG, "Failed to parse CDC checkpoint LSN {}: {}", lsnObj, e.getMessage());
            return null;
        }
    }

    // ── LSN conversion helpers ──────────────────────────────────────────
    // Debezium stores LSN as a numeric long; human-readable form is "seg/off".

    private static Long parseLongValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String && !((String) value).trim().isEmpty()) {
            try {
                return Long.parseLong(((String) value).trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private int getKeepWalHoursOrZero() {
        Integer keepWalHours = postgresConfig.getKeepWalHours();
        return keepWalHours == null ? 0 : keepWalHours;
    }

    private static long parseLsnToLong(String lsnStr) {
        if (lsnStr.contains("/")) {
            String[] parts = lsnStr.split("/", 2);
            long seg = Long.parseUnsignedLong(parts[0], 16);
            long off = Long.parseUnsignedLong(parts[1], 16);
            return (seg << 32) | off;
        }
        return Long.parseUnsignedLong(lsnStr);
    }

    private static String lsnToString(long lsn) {
        long seg = lsn >>> 32;
        long off = lsn & 0xFFFFFFFFL;
        return Long.toHexString(seg) + "/" + Long.toHexString(off);
    }

    // ── WAL timestamp seeking ─────────────────────────────────────────────────

    /**
     * Fast-scan physical WAL to find the first COMMIT record at or after the target timestamp.
     * Uses header-only parsing via pg_walinspect (PG 15+) for performance.
     *
     * @param targetTimestampMs Target timestamp in epoch milliseconds
     * @return LSN (as Long) of the first qualifying COMMIT, or null if not found
     * @throws Exception if WAL reading fails
     */
    private Long seekWalToTimestamp(long targetTimestampMs) throws Exception {

        // Get slot's restart_lsn as the starting point for scanning
        Long restartLsn = getSlotRestartLsn(runnerName);
        if (restartLsn == null) {
            throw new RuntimeException("Cannot find restart_lsn for replication slot: " + runnerName);
        }

        TapLogger.info(TAG, "Seeking WAL from restart_lsn {} to find timestamp >= {} ms",
                formatLsn(restartLsn), targetTimestampMs);

        // Try using pg_walinspect extension if available (PostgreSQL 15+)
        Long lsn = trySeekViaWalInspect(restartLsn, targetTimestampMs);
        if (lsn != null) {
            return lsn;
        }

        // Fallback: use restart_lsn as conservative starting point
        TapLogger.warn(TAG, "pg_walinspect not available - using restart_lsn as fallback. " +
                "Install pg_walinspect extension for precise timestamp seeking.");
        return restartLsn;
    }

    /**
     * Try to find LSN using pg_get_wal_records_info from pg_walinspect extension (PG 15+).
     * This scans WAL records by parsing their headers and filtering for COMMIT records
     * with timestamps >= the target.
     *
     * @param startLsn LSN to start scanning from (typically slot's restart_lsn)
     * @param targetTimestampMs Target timestamp in milliseconds
     * @return LSN of first COMMIT >= timestamp, or null if not found / extension unavailable
     */
    private Long trySeekViaWalInspect(long startLsn, long targetTimestampMs) {
        try {
            // Check if pg_walinspect extension is installed
            final boolean[] hasExtension = {false};
            postgresJdbcContext.queryWithNext(
                    "SELECT 1 FROM pg_extension WHERE extname = 'pg_walinspect'",
                    rs -> hasExtension[0] = true
            );

            if (!hasExtension[0]) {
                TapLogger.debug(TAG, "pg_walinspect extension not installed");
                return null;
            }

            // Scan WAL records from startLsn to current WAL position
            // Filter for Transaction COMMIT records and parse their timestamps
            String sql = String.format(
                    "SELECT start_lsn, description FROM pg_get_wal_records_info('%s', pg_current_wal_lsn()) " +
                    "WHERE resource_manager = 'Transaction' AND description LIKE 'COMMIT%%' " +
                    "ORDER BY start_lsn LIMIT 1000",
                    formatLsn(startLsn)
            );

            final Long[] foundLsn = {null};
            postgresJdbcContext.queryWithNext(sql, rs -> {
                while (rs.next()) {
                    String lsnStr = rs.getString("start_lsn");
                    String desc = rs.getString("description");

                    // Parse commit timestamp from description field
                    // Format: "COMMIT 2026-07-08 15:30:45.123456 UTC"
                    Long commitTimeMs = parseCommitTimestamp(desc);
                    if (commitTimeMs != null && commitTimeMs >= targetTimestampMs) {
                        TapLogger.info(TAG, "Found target LSN via pg_walinspect: {} (commit time: {} ms)",
                                lsnStr, commitTimeMs);
                        foundLsn[0] = parseLsnToLong(lsnStr);
                        break;  // Found it, stop scanning
                    }
                }
            });

            if (foundLsn[0] == null) {
                // If we scanned 1000 records and didn't find it, the timestamp might be too far ahead
                TapLogger.warn(TAG, "Scanned 1000 COMMIT records but didn't find timestamp >= {} ms",
                        targetTimestampMs);
            }
            return foundLsn[0];
        } catch (Exception e) {
            TapLogger.debug(TAG, "Failed to seek via pg_walinspect: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Parse commit timestamp from pg_walinspect description string.
     * Expected format: "COMMIT 2026-07-08 15:30:45.123456 UTC"
     *
     * @param description COMMIT record description from pg_walinspect
     * @return Timestamp in epoch milliseconds, or null if parsing fails
     */
    private Long parseCommitTimestamp(String description) {
        try {
            // Extract timestamp using regex: "COMMIT YYYY-MM-DD HH:MM:SS.mmmmmm"
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                    "COMMIT (\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+)");
            java.util.regex.Matcher matcher = pattern.matcher(description);

            if (!matcher.find()) {
                return null;
            }

            String timestampStr = matcher.group(1);

            // Parse using java.time (assumes UTC timezone)
            java.time.format.DateTimeFormatter formatter =
                    java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            java.time.LocalDateTime localDateTime = java.time.LocalDateTime.parse(timestampStr, formatter);
            return localDateTime.atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (Exception e) {
            TapLogger.debug(TAG, "Failed to parse commit timestamp from description: {}", description);
            return null;
        }
    }

    /**
     * Get the restart_lsn for the given replication slot.
     * This is the earliest LSN that the slot guarantees to have available.
     *
     * @param slotName Name of the replication slot
     * @return restart_lsn as Long, or null if slot not found
     * @throws Exception if query fails
     */
    private Long getSlotRestartLsn(String slotName) throws Exception {
        String sql = "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = '" + slotName + "'";

        final Long[] result = {null};
        postgresJdbcContext.queryWithNext(sql, rs -> {
            String lsnStr = rs.getString("restart_lsn");
            if (lsnStr != null) {
                result[0] = parseLsnToLong(lsnStr);
            }
        });
        return result[0];
    }

    /**
     * Format Long LSN to PostgreSQL string format (uppercase hex).
     *
     * @param lsn LSN as Long
     * @return LSN in PostgreSQL format (e.g. "1C/926AE1C0")
     */
    private String formatLsn(long lsn) {
        return String.format("%X/%X", (lsn >> 32) & 0xFFFFFFFFL, lsn & 0xFFFFFFFFL);
    }
}
