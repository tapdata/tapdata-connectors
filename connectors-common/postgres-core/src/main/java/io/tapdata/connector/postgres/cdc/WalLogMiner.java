package io.tapdata.connector.postgres.cdc;

import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.exception.TapPdkOffsetOutOfLogEx;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.list;

public class WalLogMiner {

    private PostgresJdbcContext postgresJdbcContext;
    private Log tapLogger;
    private StreamReadConsumer consumer;
    private int recordSize;
    private List<String> tableList;
    private boolean filterSchema;
    private KVReadOnlyMap<TapTable> tableMap;
    private String walFile;
    private String walDir;
    private String walLsn;
    private AtomicReference<Throwable> threadException = new AtomicReference<>();
    private PostgresCDCSQLParser sqlParser = new PostgresCDCSQLParser();


    public WalLogMiner(PostgresJdbcContext postgresJdbcContext, Log tapLogger) {
        this.postgresJdbcContext = postgresJdbcContext;
        this.tapLogger = tapLogger;
    }

    public WalLogMiner watch(List<String> tableList, KVReadOnlyMap<TapTable> tableMap) {
        this.tableList = tableList;
        filterSchema = tableList.size() > 50;
        this.tableMap = tableMap;
        return this;
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

    public WalLogMiner registerConsumer(StreamReadConsumer consumer, int recordSize) {
        this.consumer = consumer;
        this.recordSize = recordSize;
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
                    }
                }
            });
            t.setName("Db2GrpcLogMiner-Consumer");
            t.start();
            while (isAlive.get()) {
                if (EmptyKit.isNotNull(threadException.get())) {
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
                            walFile = resultSet.getString("name");
                        } else {
                            TapSimplify.sleep(1000);
                            continue;
                        }
                    }
                }
                statement.execute(String.format(WALMINER_WAL_ADD, walDir + walFile));
                statement.execute(WALMINER_ALL);
                try (ResultSet resultSet = statement.executeQuery(getAnalysisSql(filterSchema, walLsn, postgresJdbcContext.getConfig().getSchema(), tableList))) {
                    while (resultSet.next()) {
                        String relation = resultSet.getString("relation");
                        if (filterSchema && !tableList.contains(relation)) {
                            continue;
                        }
                        NormalRedo normalRedo = new NormalRedo();
                        normalRedo.setTableName(relation);
                        collectRedo(normalRedo, resultSet);
                        concurrentProcessor.runAsync(normalRedo, r -> {
                            try {
                                parseRedo(r);
                                return r;
                            } catch (Throwable e) {
                                threadException.set(e);
                                return null;
                            }
                        });
                    }
                }
            }
        }
    }

    private void collectRedo(NormalRedo normalRedo, ResultSet resultSet) throws SQLException {
        normalRedo.setOperation(resultSet.getString("sqlkind"));
        normalRedo.setCdcSequenceStr(walDir + walFile + "," + resultSet.getString("start_lsn"));
        normalRedo.setTransactionId(resultSet.getString("xid"));
        normalRedo.setTimestamp(resultSet.getTimestamp("timestamp").getTime());
        normalRedo.setSqlRedo(resultSet.getString("op_text"));
        normalRedo.setSqlUndo(resultSet.getString("undo_text"));
    }

    private String getAnalysisSql(boolean filterSchema, String startLsn, String schema, List<String> tableList) {
        if (filterSchema) {
            return String.format(WALMINER_CONTENTS_SCHEMA, startLsn, schema);
        } else {
            return String.format(WALMINER_CONTENTS_TABLE, startLsn, schema, String.join("','", tableList));
        }
    }

    private TapEvent createEvent(NormalRedo normalRedo) {
        TapEvent tapEvent;
        switch (normalRedo.getOperation()) {
            case "1":
                tapEvent = new TapInsertRecordEvent().init().after(normalRedo.getRedoRecord()).referenceTime(normalRedo.getTimestamp());
                break;
            case "2":
                tapEvent = new TapUpdateRecordEvent().init().after(normalRedo.getRedoRecord()).before(normalRedo.getUndoRecord()).referenceTime(normalRedo.getTimestamp());
                break;
            case "3":
                tapEvent = new TapDeleteRecordEvent().init().before(normalRedo.getRedoRecord()).referenceTime(normalRedo.getTimestamp());
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + normalRedo.getOperation());
        }
        return tapEvent;
    }

    private void parseRedo(NormalRedo normalRedo) {
        normalRedo.setRedoRecord(sqlParser.from(normalRedo.getSqlRedo(), false).getData());
        normalRedo.setUndoRecord(sqlParser.from(normalRedo.getSqlUndo(), true).getData());
    }

    private static final String WALMINER_FIND_NEXT_WAL = "SELECT * FROM pg_ls_waldir() where modification>? order by modification limit 1";
    private static final String WALMINER_FIND_WAL = "select * from pg_ls_waldir() where name='%s'";
    private static final String WALMINER_WAL_ADD = "select walminer_wal_add('%s')";
    private static final String WALMINER_ALL = "select walminer_all()";
    private static final String WALMINER_CONTENTS_SCHEMA = "select * from walminer_contents where start_lsn>'%s' and schema='%s'";
    private static final String WALMINER_CONTENTS_TABLE = "select * from walminer_contents where start_lsn>'%s' and schema='%s' and relation in ('%s')";
}
