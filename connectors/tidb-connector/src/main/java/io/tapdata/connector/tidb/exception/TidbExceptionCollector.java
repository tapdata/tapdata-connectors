package io.tapdata.connector.tidb.exception;

import io.tapdata.connector.mysql.MysqlExceptionCollector;


import io.tapdata.exception.TapPdkTerminateByServerEx;
import io.tapdata.kit.ErrorKit;


import java.sql.SQLException;

public class TidbExceptionCollector extends MysqlExceptionCollector {

    private static final String PDK_ID = "tidb";

    @Override
    protected String getPdkId() {
        return PDK_ID;
    }

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof SQLException && "08003".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkTerminateByServerEx(getPdkId(), ErrorKit.getLastCause(cause));
        }
    }
}
