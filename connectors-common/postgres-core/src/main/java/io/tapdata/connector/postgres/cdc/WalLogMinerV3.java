package io.tapdata.connector.postgres.cdc;

import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.list;

public class WalLogMinerV3 extends AbstractWalLogMiner {

    private String timestamp;

    public WalLogMinerV3(PostgresJdbcContext postgresJdbcContext, Log tapLogger) {
        super(postgresJdbcContext, tapLogger);
    }

    public WalLogMinerV3 offset(Object offsetState) {
        this.timestamp = (String) offsetState;
        return this;
    }

    public void startMiner(Supplier<Boolean> isAlive) throws Throwable {
        ConcurrentProcessor<NormalRedo, NormalRedo> concurrentProcessor = TapExecutors.createSimple(8, 32, "wal-miner");
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
        AtomicReference<String> nextTimestamp = new AtomicReference<>();
        Connection connection = postgresJdbcContext.getConnection();
        Statement statement = connection.createStatement();
        try {
            while (isAlive.get()) {
                if (EmptyKit.isNotNull(threadException.get())) {
                    consumer.streamReadEnded();
                    throw new RuntimeException(threadException.get());
                }
                try (ResultSet resultSet = statement.executeQuery(String.format(WALMINER_CURRENT_TIMESTAMP))) {
                    if (resultSet.next()) {
                        nextTimestamp.set(getWalminerNextTimestamp(resultSet.getTimestamp(1), timestamp));
                    }
                }
                tapLogger.info("Start mining wal lsn timestamp: {} - {}", timestamp, nextTimestamp.get());
                while (isAlive.get()) {
                    try {
                        statement.execute(String.format(WALMINER_BY_TIMESTAMP, timestamp, nextTimestamp.get()));
                        break;
                    } catch (Exception e) {
                        try {
                            statement.execute(WALMINER_STOP);
                        } catch (Exception ignore) {
                            tapLogger.warn("Walminer by lsn occurs error, change statement and retry: from {} to {}", timestamp, nextTimestamp.get());
                            EmptyKit.closeQuietly(statement);
                            EmptyKit.closeQuietly(connection);
                            connection = postgresJdbcContext.getConnection();
                            statement = connection.createStatement();
                        }
                        TapSimplify.sleep(2000);
                    }
                }
                String analysisSql = getAnalysisSql();
                try (ResultSet resultSet = statement.executeQuery(analysisSql)) {
                    while (resultSet.next()) {
                        String relation = resultSet.getString("relation");
                        String schema = resultSet.getString("schema");
                        timestamp = resultSet.getString("timestamp");
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
                        normalRedo.setCdcSequenceStr(timestamp);
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
                timestamp = nextTimestamp.get();
            }
        } finally {
            statement.close();
            connection.close();
            concurrentProcessor.close();
            consumer.streamReadEnded();
        }
    }

    private String getAnalysisSql() {
        if (withSchema) {
            if (filterSchema) {
                return String.format(MULTI_WALMINER_CONTENTS_SCHEMA, String.join("','", schemaTableMap.keySet()));
            } else {
                return String.format(MULTI_WALMINER_CONTENTS_TABLE, schemaTableMap.entrySet().stream().map(e ->
                        String.format("schema='%s' and relation in ('%s')", e.getKey(), String.join("','", e.getValue()))).collect(Collectors.joining(" or ")));
            }
        } else {
            if (filterSchema) {
                return String.format(WALMINER_CONTENTS_SCHEMA, postgresConfig.getSchema());
            } else {
                return String.format(WALMINER_CONTENTS_TABLE, postgresConfig.getSchema(), String.join("','", tableList));
            }
        }
    }

    private String getWalminerNextTimestamp(Timestamp now, String timestamp) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long t = sdf.parse(timestamp).getTime();
        long n = now.getTime();
        return sdf.format(n - t > 10 * 60 * 1000L ? (t + 10 * 60 * 1000L) : n);
    }

    private static final String WALMINER_CURRENT_TIMESTAMP = "select clock_timestamp()";
    private static final String WALMINER_BY_TIMESTAMP = "select walminer_by_time('%s', '%s', true)";
    private static final String WALMINER_CONTENTS_SCHEMA = "select * from walminer_contents where minerd=true and schema='%s' order by start_lsn";
    private static final String WALMINER_CONTENTS_TABLE = "select * from walminer_contents where minerd=true and schema='%s' and relation in ('%s') order by start_lsn";
    private static final String MULTI_WALMINER_CONTENTS_SCHEMA = "select * from walminer_contents where minerd=true and schema in ('%s') order by start_lsn";
    private static final String MULTI_WALMINER_CONTENTS_TABLE = "select * from walminer_contents where minerd=true and (%s) order by start_lsn";
}
