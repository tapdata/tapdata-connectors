package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.kit.EmptyKit;

import java.io.Serializable;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

public class TidbJdbcContext extends MysqlJdbcContextV2 implements Serializable {
    public static final String SAFE_GC_POINT_SQL = "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME IN ('tikv_gc_safe_point', 'tikv_gc_enable')";

    public TidbJdbcContext(CommonDbConfig config) {
        super(config);
    }


    public long querySafeGcPoint() throws SQLException {
        AtomicReference<Long> safePointCollector = new AtomicReference<>();
        query(SAFE_GC_POINT_SQL, r -> {
            while (r.next()) {
                if ("tikv_gc_safe_point".equalsIgnoreCase(r.getString("VARIABLE_NAME"))) {
                    String variableValue = r.getString("VARIABLE_VALUE");
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS Z");
                    ZonedDateTime zonedDateTime = ZonedDateTime.parse(variableValue, formatter);
                    safePointCollector.set(zonedDateTime.toInstant().toEpochMilli());
                    break;
                }
            }
        });
        return null != safePointCollector.get() ? safePointCollector.get() : System.currentTimeMillis();
    }

    public String queryGcLifeTime() throws SQLException {
        AtomicReference<String> gcLifeTimeCollector = new AtomicReference<>();
        query("SHOW GLOBAL VARIABLES like 'tidb_gc_life_time'", r -> {
            while (r.next()) {
                if ("tidb_gc_life_time".equalsIgnoreCase(r.getString("Variable_name"))) {
                    gcLifeTimeCollector.set(r.getString("Value"));
                    break;
                }
            }
        });
        return gcLifeTimeCollector.get();
    }

    public TimeZone queryTimeZone() throws SQLException {
        AtomicReference<Long> timeOffset = new AtomicReference<>();
        queryWithNext("SELECT ROUND(TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) / 3600) AS timeoffset",
                resultSet -> timeOffset.set(resultSet.getLong(1)));
        DecimalFormat decimalFormat = new DecimalFormat("00");
        if (timeOffset.get() >= 0) {
            return TimeZone.getTimeZone(ZoneId.of("+" + decimalFormat.format(timeOffset.get()) + ":00"));
        } else {
            return TimeZone.getTimeZone(ZoneId.of(decimalFormat.format(timeOffset.get()) + ":00"));
        }
    }
}
