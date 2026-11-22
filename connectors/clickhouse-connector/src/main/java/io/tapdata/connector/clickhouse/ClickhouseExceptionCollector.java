package io.tapdata.connector.clickhouse;

import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.exception.*;
import io.tapdata.kit.ErrorKit;

import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickhouseExceptionCollector extends AbstractExceptionCollector implements ExceptionCollector {

    private static final String PDK_ID = "clickhouse";

    @Override
    protected String getPdkId() {
        return PDK_ID;
    }

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof SQLException && ((SQLException) cause).getErrorCode() == 394) {
            throw new TapPdkTerminateByServerEx(PDK_ID, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        if (cause instanceof SQLException && ((SQLException) cause).getErrorCode() == 516) {
            throw new TapPdkUserPwdInvalidEx(PDK_ID, username, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof SQLException && ((SQLException) cause).getErrorCode() == 497) {
            throw new TapPdkReadMissingPrivilegesEx(PDK_ID, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof SQLException && ((SQLException) cause).getErrorCode() == 497) {
            throw new TapPdkWriteMissingPrivilegesEx(PDK_ID, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWriteType(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        //lots of types will be parsed
    }

    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        //string will be truncated, number will be rounded or cross the range
    }

    @Override
    public void collectViolateNull(String targetFieldName, Throwable cause) {
        if (cause instanceof SQLException && "HY000".equals(((SQLException) cause).getSQLState())) {
            Pattern pattern = Pattern.compile("Cannot set null to non-nullable column #\\d+ \\[(.*)]");
            Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
            String fieldName = null;
            if (matcher.find()) {
                fieldName = matcher.group(1);
            }
            throw new TapPdkViolateNullableEx(PDK_ID, fieldName, ErrorKit.getLastCause(cause));
        }
    }

}
