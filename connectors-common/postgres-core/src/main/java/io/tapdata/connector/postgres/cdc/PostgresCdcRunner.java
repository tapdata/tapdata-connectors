package io.tapdata.connector.postgres.cdc;

import com.google.common.collect.Lists;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.StopConnectorException;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.config.PostgresDebeziumConfig;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
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
import io.tapdata.exception.TapCodeException;
import io.tapdata.kit.ErrorKit;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
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
    private PostgresOffset postgresOffset;
    private int recordSize;
    private StreamReadConsumer consumer;
    private final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
    protected TimeZone timeZone;
    private String dropTransactionId = null;
    private boolean withSchema = false;
    private final Map<String, Boolean> replicaFull = new HashMap<>();
    private static final DDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("\"");
    private final DDLParserType ddlParserType = DDLParserType.POSTGRES_CCJ_SQL_PARSER;


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
            postgresOffset = new PostgresOffset();
        } else {
            this.postgresOffset = (PostgresOffset) offsetState;
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
            try {
                offset = sr.sourceOffset();
                // PG use micros to indicate the time but in pdk api we use millis
                Long referenceTime = (Long) offset.get("ts_usec") / 1000;
                Struct struct = ((Struct) sr.value());
                if (struct == null) {
                    continue;
                }
                if ("io.debezium.connector.common.Heartbeat".equals(sr.valueSchema().name())) {
                    eventList.add(new HeartbeatEvent().init().referenceTime(((Struct) sr.value()).getInt64("ts_ms")));
                    continue;
                } else if (EmptyKit.isNull(sr.valueSchema().field("op"))) {
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
                            String ddlTag = after.getString("c_tag");
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
            } catch (StopConnectorException | StopEngineException ex) {
                throw ex;
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
                : schema;

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
                try { conn.rollback(); } catch (SQLException ignored) { }
                throw new TapCodeException(PostgresErrorCode.CREATE_DDL_TRIGGER_FAILED,
                        "Failed to set up DDL event trigger artifacts in schema " + targetSchema + ": " + e.getMessage(), e)
                        .dynamicDescriptionParameters(e.getMessage());
            } finally {
                try { conn.setAutoCommit(autoCommit); } catch (SQLException ignored) { }
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
                : schema;

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
     * Returns the schema-qualified name of the DDL audit table.
     * Used to detect DDL events in {@link #consumeRecords}.
     */
    public String getDdlAuditTableFullName() {
        if (!Boolean.TRUE.equals(postgresConfig.getDdlTriggerEnable())) {
            return null;
        }
        String schema = EmptyKit.isNotBlank(postgresConfig.getDdlTriggerSchema())
                ? postgresConfig.getDdlTriggerSchema()
                : postgresConfig.getSchema();
        return schema + "." + PostgresJdbcContext.DDL_AUDIT_TABLE;
    }

    /**
     * Check whether the current Debezium record represents a DDL event
     * (i.e., an INSERT into the DDL audit table).
     */
    private boolean isDdlAuditRecord(String schema, String table) {
        String auditSchema = EmptyKit.isNotBlank(postgresConfig.getDdlTriggerSchema())
                ? postgresConfig.getDdlTriggerSchema()
                : postgresConfig.getSchema();
        return PostgresJdbcContext.DDL_AUDIT_TABLE.equals(table)
                && auditSchema.equals(schema);
    }
}
