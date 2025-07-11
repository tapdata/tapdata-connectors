package io.tapdata.connector.tidb.exception;

import io.tapdata.connector.mysql.MysqlExceptionCollector;
import io.tapdata.connector.mysql.constant.DeployModeEnum;
import io.tapdata.exception.TapPdkRetryableEx;
import io.tapdata.exception.TapPdkTerminateByServerEx;
import io.tapdata.kit.ErrorKit;

import java.io.EOFException;
import java.sql.SQLException;

public class TidbExceptionCollector extends MysqlExceptionCollector {

    private final static String pdkId = "tidb";

    @Override
    protected String getPdkId() {
        return pdkId;
    }

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof SQLException && "08003".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkTerminateByServerEx(getPdkId(), ErrorKit.getLastCause(cause));
        }
    }
}
