package io.tapdata.connector.starrocks;

import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.connector.starrocks.streamload.exception.StarrocksRuntimeException;
import io.tapdata.exception.TapPdkReadMissingPrivilegesEx;
import io.tapdata.exception.TapPdkTerminateByServerEx;
import io.tapdata.exception.TapPdkUserPwdInvalidEx;
import io.tapdata.exception.TapPdkWriteMissingPrivilegesEx;
import io.tapdata.kit.ErrorKit;

import java.sql.SQLException;
import java.util.List;

public class StarrocksExceptionCollector extends AbstractExceptionCollector implements ExceptionCollector {
    private final static String pdkId = "starrocks";

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof SQLException && "08003".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkTerminateByServerEx(pdkId, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        if (cause instanceof SQLException && "28000".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkUserPwdInvalidEx(pdkId, username, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof SQLException && "42000".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkReadMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof SQLException && "42000".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkWriteMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
        if(cause instanceof StarrocksRuntimeException && cause.getMessage().toLowerCase().contains("access denied")){
            throw new TapPdkWriteMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }
}
