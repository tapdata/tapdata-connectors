package io.tapdata.connector.doris;

import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.connector.doris.streamload.exception.DorisRuntimeException;
import io.tapdata.exception.TapPdkReadMissingPrivilegesEx;
import io.tapdata.exception.TapPdkTerminateByServerEx;
import io.tapdata.exception.TapPdkUserPwdInvalidEx;
import io.tapdata.exception.TapPdkWriteMissingPrivilegesEx;
import io.tapdata.kit.ErrorKit;

import java.sql.SQLException;
import java.util.List;

public class DorisExceptionCollector extends AbstractExceptionCollector implements ExceptionCollector {
    private static final String PDK_ID = "doris";

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof SQLException && "08003".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkTerminateByServerEx(PDK_ID, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        if (cause instanceof SQLException && "28000".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkUserPwdInvalidEx(PDK_ID, username, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof SQLException && "42000".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkReadMissingPrivilegesEx(PDK_ID, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof SQLException && "42000".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkWriteMissingPrivilegesEx(PDK_ID, operation, privileges, ErrorKit.getLastCause(cause));
        }
        if(cause instanceof DorisRuntimeException && cause.getMessage().toLowerCase().contains("access denied")){
            throw new TapPdkWriteMissingPrivilegesEx(PDK_ID, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }
}
