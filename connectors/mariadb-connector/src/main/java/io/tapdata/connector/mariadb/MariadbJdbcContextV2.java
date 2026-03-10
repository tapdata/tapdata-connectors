package io.tapdata.connector.mariadb;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;

import java.util.concurrent.atomic.AtomicReference;

public class MariadbJdbcContextV2  extends MysqlJdbcContextV2 {


    public MariadbJdbcContextV2(CommonDbConfig config) {
        super(config);
    }

    public MysqlBinlogPosition readBinlogPosition() throws Throwable {
        AtomicReference<MysqlBinlogPosition> mysqlBinlogPositionAtomicReference = new AtomicReference<>();
        normalQuery("SHOW MASTER STATUS", rs -> {
            if (rs.next()) {
                String binlogFilename = rs.getString(1);
                long binlogPosition = rs.getLong(2);
                mysqlBinlogPositionAtomicReference.set(new MysqlBinlogPosition(binlogFilename, binlogPosition));
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    String gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                    mysqlBinlogPositionAtomicReference.get().setGtidSet(gtidSet);
                }
            }
        });
        return mysqlBinlogPositionAtomicReference.get();
    }
}
