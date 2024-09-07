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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.list;

public class WalLogMinerV2 extends AbstractWalLogMiner {

    private String startLsn;
    private String commitLsnTemp;
    private String commitLsn;
    private String sqlno;

    public WalLogMinerV2(PostgresJdbcContext postgresJdbcContext, Log tapLogger) {
        super(postgresJdbcContext, tapLogger);
    }

    public WalLogMinerV2 offset(Object offsetState) {
        String[] split = ((String) offsetState).split(",");
        this.startLsn = split[0];
        this.commitLsn = split[1];
        this.sqlno = split[2];
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
        AtomicReference<String> nextLsn = new AtomicReference<>();
        int retry = 0;
        Connection connection = postgresJdbcContext.getConnection();
        connection.setAutoCommit(true);
        Statement statement = connection.createStatement();
        try {
            while (isAlive.get()) {
                if (EmptyKit.isNotNull(threadException.get())) {
                    consumer.streamReadEnded();
                    throw new RuntimeException(threadException.get());
                }
                try (ResultSet resultSet = statement.executeQuery(String.format(WALMINER_CURRENT_LSN))) {
                    if (resultSet.next()) {
                        nextLsn.set(getWalminerNextLsn(resultSet.getString(1), startLsn));
                    }
                }
                if (startLsn.equals(nextLsn.get())) {
                    NormalRedo heart = new NormalRedo();
                    heart.setCdcSequenceStr(startLsn + "," + commitLsn + "," + sqlno);
                    concurrentProcessor.runAsync(heart, r -> r);
                    TapSimplify.sleep(1000);
                    continue;
                }
                tapLogger.info("Start mining wal lsn range: {} - {}", startLsn, nextLsn.get());
                boolean addFile = false;
                while (isAlive.get()) {
                    try {
                        statement.execute(String.format(WALMINER_BY_LSN, startLsn, nextLsn.get()));
                        break;
                    } catch (Exception e) {
                        try {
                            if (e instanceof SQLException && "XX000".equals(((SQLException) e).getSQLState())) {
                                tapLogger.warn("check point not found, retry use add file from {} to {}", startLsn, nextLsn.get());
                                String startFile = null;
                                String endFile = null;
                                try (ResultSet resultSet = statement.executeQuery(String.format(WALMINER_GET_WAL_NAME, startLsn))) {
                                    if (resultSet.next()) {
                                        startFile = resultSet.getString(1);
                                    }
                                }
                                try (ResultSet resultSet = statement.executeQuery(String.format(WALMINER_GET_WAL_NAME, nextLsn.get()))) {
                                    if (resultSet.next()) {
                                        endFile = resultSet.getString(1);
                                    }
                                }
                                List<String> walFileNames = getWalFileNames(startFile, endFile);
                                if (EmptyKit.isNotEmpty(walFileNames)) {
                                    addFile = true;
                                    for (String walFileName : walFileNames) {
                                        statement.execute(String.format(WALMINER_WAL_ADD, walLogDirectory + walFileName));
                                    }
                                    statement.execute(WALMINER_ALL);
                                } else {
                                    statement.execute(String.format(WALMINER_BY_LSN_VAGUE, startLsn, nextLsn.get()));
                                }
                                break;
                            } else {
                                statement.execute(WALMINER_STOP);
                            }
                        } catch (Exception ignore) {
                            tapLogger.warn("Walminer by lsn occurs error, change statement and retry: from {} to {}", startLsn, nextLsn.get());
                            EmptyKit.closeQuietly(statement);
                            EmptyKit.closeQuietly(connection);
                            connection = postgresJdbcContext.getConnection();
                            connection.setAutoCommit(true);
                            statement = connection.createStatement();
                        }
                        TapSimplify.sleep(2000);
                    }
                }
                String analysisSql;
                if (addFile) {
                    analysisSql = getAnalysisSqlForFile(startLsn, nextLsn.get());
                } else {
                    analysisSql = getAnalysisSql(startLsn);
                }

                try (ResultSet resultSet = statement.executeQuery(analysisSql)) {
                    while (resultSet.next()) {
                        String relation = resultSet.getString("relation");
                        String schema = resultSet.getString("schema");
                        startLsn = resultSet.getString("start_lsn");
                        String commitLsnTemp = resultSet.getString("commit_lsn");
                        String sqlnoTemp = resultSet.getString("sqlno");
                        if (commitLsnTemp.equals(commitLsn) && Integer.parseInt(sqlnoTemp) <= Integer.parseInt(sqlno)) {
                            continue;
                        }
                        commitLsn = commitLsnTemp;
                        sqlno = sqlnoTemp;
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
                        normalRedo.setCdcSequenceStr(startLsn + "," + commitLsn + "," + sqlno);
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
                try (ResultSet resultSet = statement.executeQuery("select max(start_lsn||''),max(commit_lsn||'') from walminer_contents")) {
                    if (resultSet.next()) {
                        if (EmptyKit.isNotNull(resultSet.getString(1))) {
                            startLsn = resultSet.getString(1);
                            commitLsn = resultSet.getString(2);
                        }
                    }
                }
                if (commitLsn.equals(commitLsnTemp)) {
                    retry++;
                    if (retry > 5) {
                        tapLogger.info("Walminer retry more than 5 times, skip lsn");
                        startLsn = nextLsn.get();
                    }
                } else {
                    commitLsnTemp = commitLsn;
                    retry = 0;
                }
            }
        } finally {
            statement.close();
            connection.close();
            concurrentProcessor.close();
            consumer.streamReadEnded();
        }
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

    private List<String> getWalFileNames(String startFile, String endFile) {
        if (EmptyKit.isNull(startFile) || EmptyKit.isNull(endFile)) {
            return Collections.emptyList();
        }
        List<String> walFileNames = new ArrayList<>();
        try {
            postgresJdbcContext.query(String.format(WALMINER_LIST_FILES, startFile, endFile), rs -> {
                while (rs.next()) {
                    walFileNames.add(rs.getString(1));
                }
            });
        } catch (Exception e) {
            return Collections.emptyList();
        }
        return walFileNames;
    }

    private String getAnalysisSqlForFile(String startLsn, String nextLsn) {
        if (withSchema) {
            if (filterSchema) {
                return String.format(MULTI_WALMINER_CONTENTS_SCHEMA_FILE, startLsn, nextLsn, String.join("','", schemaTableMap.keySet()));
            } else {
                return String.format(MULTI_WALMINER_CONTENTS_TABLE_FILE, startLsn, nextLsn, schemaTableMap.entrySet().stream().map(e ->
                        String.format("schema='%s' and relation in ('%s')", e.getKey(), String.join("','", e.getValue()))).collect(Collectors.joining(" or ")));
            }
        } else {
            if (filterSchema) {
                return String.format(WALMINER_CONTENTS_SCHEMA_FILE, startLsn, nextLsn, postgresConfig.getSchema());
            } else {
                return String.format(WALMINER_CONTENTS_TABLE_FILE, startLsn, nextLsn, postgresConfig.getSchema(), String.join("','", tableList));
            }
        }
    }

    private String getWalminerNextLsn(String currentLsn, String walSearchLsn) {
        //16进制转10进制
        long current = getLongValueFromLsn(currentLsn);
        long next = getLongValueFromLsn(walSearchLsn);
        String hexStr = Long.toHexString(current - next > 10000000 ? next + 10000000 : current).toUpperCase();
        if (hexStr.length() > 8) {
            return hexStr.substring(0, hexStr.length() - 8) + "/" + hexStr.substring(hexStr.length() - 8);
        } else {
            return "0/" + hexStr;
        }
    }

    private long getLongValueFromLsn(String lsn) {
        String[] array = lsn.split("/");
        return Long.parseLong(array[0], 16) * 4294967296L + Long.parseLong(array[1], 16);
    }

    private static final String WALMINER_LIST_FILES = "SELECT name FROM pg_ls_waldir() where name between '%s' and '%s'";
    private static final String WALMINER_GET_WAL_NAME = "select pg_walfile_name('%s')";
    private static final String WALMINER_WAL_ADD = "select walminer_wal_add('%s')";
    private static final String WALMINER_ALL = "select walminer_all()";
    private static final String WALMINER_CURRENT_LSN = "select pg_current_wal_lsn()";
    private static final String WALMINER_BY_LSN_VAGUE = "select walminer_by_lsn('%s', '%s')";
    private static final String WALMINER_BY_LSN = "select walminer_by_lsn('%s', '%s', true)";
    private static final String WALMINER_CONTENTS_SCHEMA = "select * from walminer_contents where minerd=true and commit_lsn>'%s' and schema='%s' order by start_lsn";
    private static final String WALMINER_CONTENTS_TABLE = "select * from walminer_contents where minerd=true and commit_lsn>'%s' and schema='%s' and relation in ('%s') order by start_lsn";
    private static final String MULTI_WALMINER_CONTENTS_SCHEMA = "select * from walminer_contents where minerd=true and commit_lsn>'%s' and schema in ('%s') order by start_lsn";
    private static final String MULTI_WALMINER_CONTENTS_TABLE = "select * from walminer_contents where minerd=true and commit_lsn>'%s' and (%s) order by start_lsn";
    private static final String WALMINER_CONTENTS_SCHEMA_FILE = "select * from walminer_contents where minerd=true and commit_lsn>'%s' and commit_lsn<='%s' and schema='%s' order by start_lsn";
    private static final String WALMINER_CONTENTS_TABLE_FILE = "select * from walminer_contents where minerd=true and commit_lsn>'%s' and commit_lsn<='%s' and schema='%s' and relation in ('%s') order by start_lsn";
    private static final String MULTI_WALMINER_CONTENTS_SCHEMA_FILE = "select * from walminer_contents where minerd=true and commit_lsn>'%s' and commit_lsn<='%s' and schema in ('%s') order by start_lsn";
    private static final String MULTI_WALMINER_CONTENTS_TABLE_FILE = "select * from walminer_contents where minerd=true and commit_lsn>'%s' and commit_lsn<='%s' and (%s) order by start_lsn";
}
