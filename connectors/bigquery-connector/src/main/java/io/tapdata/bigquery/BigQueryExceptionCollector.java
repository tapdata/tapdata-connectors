package io.tapdata.bigquery;


import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.exception.*;
import io.tapdata.kit.ErrorKit;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class BigQueryExceptionCollector extends AbstractExceptionCollector implements ExceptionCollector {
    private final static String pdkId = "bigquery";

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        if(cause.getMessage() != null && cause.getMessage().contains("Invalid JWT Signature")){
            throw new TapPdkUserPwdInvalidEx(pdkId,username, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) {
        if(cause.getMessage() != null && cause.getMessage().contains("Access Denied")){
            throw new TapPdkWriteMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
       if(cause.getMessage() != null && cause.getMessage().contains("Access Denied")){
           throw new TapPdkWriteMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
       }
    }

    @Override
    public void collectWriteType(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        if( cause instanceof Exceptions.AppendSerializtionError && cause.getMessage() != null && cause.getMessage().contains("Append serialization failed for writer")){
            String error =  ((Exceptions.AppendSerializtionError) cause).getRowIndexToErrorMessage().get(0);
            String regex = "Field (.*) failed to convert to (\\w+)";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(error);
            String targetName = null;
            String targetType = null;
            if (matcher.find()) {
                targetName = matcher.group(1);
                targetType = matcher.group(2);
                throw new TapPdkWriteTypeEx(pdkId, targetName, targetType, data, ErrorKit.getLastCause(cause));
            }
        }
    }

    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        //number length
        if (cause.getCause() != null && cause.getCause() instanceof Exceptions.AppendSerializtionError) {
            String error =  ((Exceptions.AppendSerializtionError) cause.getCause()).getRowIndexToErrorMessage().get(0);
            String regex = "Field (\\w+): (.*) has maximum length (\\d+) but got a value with length (\\d+)";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(error);
            if (matcher.find()) {
                throw new TapPdkWriteLengthEx(pdkId, matcher.group(1), matcher.group(2), data, ErrorKit.getLastCause(cause.getCause()));
            }

        }

    }

    @Override
    public void collectViolateNull(String targetFieldName, Throwable cause) {
        if (cause instanceof Exceptions.AppendSerializtionError &&  cause.getMessage() != null && cause.getMessage().contains("Append serialization failed for writer")) {
            String error = ((Exceptions.AppendSerializtionError) cause).getRowIndexToErrorMessage().get(0);
            String regex = "JSONObject does not have the required field (.*)";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(error);
            if (matcher.find()) {
                throw new TapPdkViolateNullableEx(pdkId, matcher.group(1), ErrorKit.getLastCause(cause));
            }
        }
    }
}
