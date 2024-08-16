package io.tapdata.connector.postgres.cdc;

import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.exception.TapPdkOffsetOutOfLogEx;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.sql.*;
import java.util.List;
import java.util.function.Supplier;

public class WalLogMiner {

    private PostgresJdbcContext postgresJdbcContext;
    private Log tapLogger;
    private StreamReadConsumer consumer;
    private int recordSize;
    private List<String> tableList;
    private KVReadOnlyMap<TapTable> tableMap;
    private String walFile;
    private String walDir;
    private String walLsn;


    public WalLogMiner(PostgresJdbcContext postgresJdbcContext, Log tapLogger) {
        this.postgresJdbcContext = postgresJdbcContext;
        this.tapLogger = tapLogger;
    }

    public WalLogMiner watch(List<String> tableList, KVReadOnlyMap<TapTable> tableMap) {
        this.tableList = tableList;
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
                Connection connection = postgresJdbcContext.getConnection();
                Statement statement = connection.createStatement();
                PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM pg_ls_waldir() where modification>? order by modification limit 1")
        ) {
            Timestamp lastModified;
            try (ResultSet resultSet = statement.executeQuery(String.format("select * from pg_ls_waldir() where name='%s'", walFile))) {
                if (resultSet.next()) {
                    lastModified = resultSet.getTimestamp("modification");
                } else {
                    throw new TapPdkOffsetOutOfLogEx("postgres", walDir + walFile + "," + walLsn, new IllegalStateException("wal file not found"));
                }
            }
            while (isAlive.get()) {
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
                statement.execute(String.format("select walminer_wal_add('%s')", walDir + walFile));
                statement.execute("select walminer_all()");
                try (ResultSet resultSet = statement.executeQuery(String.format("select * from walminer_contents where start_lsn>'%s'", walLsn))) {
                    while (resultSet.next()) {
                        walLsn = resultSet.getString("start_lsn");
                        String tableName = resultSet.getString("relation");
                        String schema = resultSet.getString("schema");
                        String sqlRedo = resultSet.getString("op_text");
                        tapLogger.info("wal log redo: " + sqlRedo);
                    }
                }
            }
        }
    }
}
