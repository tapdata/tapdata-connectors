package io.tapdata.connector.postgres.cdc;

import com.google.common.collect.Lists;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.StopConnectorException;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.config.PostgresDebeziumConfig;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffsetStorage;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.NumberKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
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
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CDC runner for Postgresql
 *
 * @author Jarad
 * @date 2022/5/13
 */
public class PostgresCdcRunner extends DebeziumCdcRunner {

    private static final String TAG = PostgresCdcRunner.class.getSimpleName();
    private final PostgresConfig postgresConfig;
    private PostgresDebeziumConfig postgresDebeziumConfig;
    private PostgresOffset postgresOffset;
    private int recordSize;
    private StreamReadConsumer consumer;
    private final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
    protected TimeZone timeZone;
    private String dropTransactionId = null;
    private boolean withSchema = false;

    public PostgresCdcRunner(PostgresJdbcContext postgresJdbcContext) throws SQLException {
        this.postgresConfig = (PostgresConfig) postgresJdbcContext.getConfig();
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

    public PostgresCdcRunner watch(List<String> observedTableList) {
        withSchema = false;
        if (postgresConfig.getDoubleActive()) {
            observedTableList.add("_tap_double_active");
        }
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
        PostgresOffsetStorage.postgresOffsetMap.put(runnerName, postgresOffset);
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
                    committer.markProcessed(sr);
                    continue;
                } else if (EmptyKit.isNull(sr.valueSchema().field("op"))) {
                    continue;
                }
                String op = struct.getString("op");
                String lsn = String.valueOf(offset.get("lsn"));
                String table = struct.getStruct("source").getString("table");
                String schema = struct.getStruct("source").getString("schema");
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
                Struct before = struct.getStruct("before");
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
                committer.markProcessed(sr);
                if (eventList.size() >= recordSize) {
                    postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
                    consumer.accept(eventList, postgresOffset);
                    PostgresOffsetStorage.postgresOffsetMap.put(runnerName, postgresOffset);
                    committer.markBatchFinished();
                    eventList = TapSimplify.list();
                }
            } catch (StopConnectorException | StopEngineException ex) {
                committer.markProcessed(sr);
                throw ex;
            }
        }
        if (EmptyKit.isNotEmpty(eventList)) {
            postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
            consumer.accept(eventList, postgresOffset);
            PostgresOffsetStorage.postgresOffsetMap.put(runnerName, postgresOffset);
            committer.markBatchFinished();
        }
    }

    private DataMap getMapFromStruct(Struct struct) {
        DataMap dataMap = new DataMap();
        if (EmptyKit.isNull(struct)) {
            return dataMap;
        }
        struct.schema().fields().forEach(field -> {
            Object obj = struct.getWithoutDefault(field.name());
            if (obj instanceof ByteBuffer) {
                obj = struct.getBytes(field.name());
            } else if (obj instanceof Struct) {
                obj = BigDecimal.valueOf(NumberKit.bytes2long(((Struct) obj).getBytes("value")), (int) ((Struct) obj).get("scale"));
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
}
