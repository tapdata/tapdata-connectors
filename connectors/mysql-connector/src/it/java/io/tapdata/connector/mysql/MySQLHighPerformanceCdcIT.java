package io.tapdata.connector.mysql;

import org.junit.jupiter.api.extension.ExtendWith;

/** High-performance binlog configuration, restricted to CDC streamRead scenarios. */
@ExtendWith(MySQLCdcOnlyCondition.class)
public class MySQLHighPerformanceCdcIT extends MySQLConnectorIT {
    @Override
    protected boolean highPerformanceCdcEnabled() {
        return true;
    }
}
