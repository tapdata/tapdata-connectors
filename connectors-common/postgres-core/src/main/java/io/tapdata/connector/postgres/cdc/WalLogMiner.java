package io.tapdata.connector.postgres.cdc;

import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.exception.TapPdkOffsetOutOfLogEx;
import io.tapdata.kit.EmptyKit;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.list;

public class WalLogMiner extends AbstractWalLogMiner {

    private String walFile;
    private String walDir;
    private String walLsn;

    public WalLogMiner(PostgresJdbcContext postgresJdbcContext, Log tapLogger) {
        super(postgresJdbcContext, tapLogger);
    }

    public WalLogMiner offset(Object offsetState) {
        if (offsetState instanceof String) {
            String[] fileAndLsn = ((String) offsetState).split(",");
            if (fileAndLsn.length == 2) {
                String dirFile = fileAndLsn[0];
                this.walFile = dirFile.substring(dirFile.lastIndexOf("/") + 1);
                this.walDir = dirFile.substring(0, dirFile.lastIndexOf("/") + 1);
                this.walLsn = fileAndLsn[1];
            } else {
                throw new TapPdkOffsetOutOfLogEx("postgres", offsetState, new IllegalStateException("offsetState is not in the format of [file,lsn]"));
            }
        } else {
            throw new TapPdkOffsetOutOfLogEx("postgres", offsetState, new IllegalStateException("offsetState must be a string"));
        }
        return this;
    }

    public void startMiner(Supplier<Boolean> isAlive) throws Throwable {
        boolean first = true;
        try (
                ConcurrentProcessor<NormalRedo, NormalRedo> concurrentProcessor = TapExecutors.createSimple(8, 32, "wal-miner");
                Connection connection = postgresJdbcContext.getConnection();
                Statement statement = connection.createStatement();
                PreparedStatement preparedStatement = connection.prepareStatement(WALMINER_FIND_NEXT_WAL)
        ) {
            Timestamp lastModified;
            try (ResultSet resultSet = statement.executeQuery(String.format(WALMINER_FIND_WAL, walFile))) {
                if (resultSet.next()) {
                    lastModified = resultSet.getTimestamp("modification");
                } else {
                    throw new TapPdkOffsetOutOfLogEx("postgres", walDir + walFile + "," + walLsn, new IllegalStateException("wal file not found"));
                }
            }
            Thread t = new Thread(() -> {
                consumer.streamReadStarted();
                NormalRedo lastRedo = null;
                AtomicReference<List<TapEvent>> events = new AtomicReference<>(list());
                while (isAlive.get()) {
                    try {
                        NormalRedo redo = concurrentProcessor.get(2, TimeUnit.SECONDS);
                        if (EmptyKit.isNotNull(redo)) {
                            lastRedo = redo;
                            events.get().add(createEvent(redo));
                            if (events.get().size() >= recordSize) {
                                consumer.accept(events.get(), redo.getCdcSequenceStr());
                                events.set(new ArrayList<>());
                            }
                        } else {
                            if (events.get().size() > 0) {
                                consumer.accept(events.get(), lastRedo.getCdcSequenceStr());
                                events.set(new ArrayList<>());
                            }
                        }
                    } catch (Exception e) {
                        threadException.set(e);
                        return;
                    }
                }
            });
            t.setName("wal-miner-Consumer");
            t.start();
            boolean print = true;
            while (isAlive.get()) {
                if (EmptyKit.isNotNull(threadException.get())) {
                    consumer.streamReadEnded();
                    throw new RuntimeException(threadException.get());
                }
                if (first) {
                    first = false;
                } else {
                    preparedStatement.clearParameters();
                    preparedStatement.setTimestamp(1, lastModified);
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        if (resultSet.next()) {
                            lastModified = resultSet.getTimestamp("modification");
                            String name = resultSet.getString("name");
                            print = !walFile.equals(name);
                            walFile = name;
                        } else {
                            TapSimplify.sleep(1000);
                            continue;
                        }
                    }
                }
                statement.execute(String.format(WALMINER_WAL_ADD, walDir + walFile));
                statement.execute(WALMINER_ALL);
                String analysisSql = getAnalysisSql(walLsn);
                if (print) {
                    tapLogger.info("Start mining wal file: " + walDir + walFile);
                }
                try (ResultSet resultSet = statement.executeQuery(analysisSql)) {
                    while (resultSet.next()) {
                        String relation = resultSet.getString("relation");
                        String schema = resultSet.getString("schema");
                        walLsn = resultSet.getString("start_lsn");
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
                        normalRedo.setCdcSequenceStr(walDir + walFile + "," + walLsn);
                        collectRedo(normalRedo, resultSet);
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
            }
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

    private static final String WALMINER_FIND_NEXT_WAL = "SELECT * FROM pg_ls_waldir() where modification>? order by modification limit 1";
    private static final String WALMINER_FIND_WAL = "select * from pg_ls_waldir() where name='%s'";
    private static final String WALMINER_WAL_ADD = "select walminer_wal_add('%s')";
    private static final String WALMINER_ALL = "select walminer_all()";
    private static final String WALMINER_CONTENTS_SCHEMA = "select * from walminer_contents where minerd=true and start_lsn>'%s' and schema='%s'";
    private static final String WALMINER_CONTENTS_TABLE = "select * from walminer_contents where minerd=true and start_lsn>'%s' and schema='%s' and relation in ('%s')";
    private static final String MULTI_WALMINER_CONTENTS_SCHEMA = "select * from walminer_contents minerd=true and where start_lsn>'%s' and schema in ('%s')";
    private static final String MULTI_WALMINER_CONTENTS_TABLE = "select * from walminer_contents minerd=true and where start_lsn>'%s' and (%s)";
}
