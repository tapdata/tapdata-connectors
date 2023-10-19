package io.tapdata.connector.elasticsearch;

import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.exception.*;
import io.tapdata.kit.ErrorKit;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ElasticsearchExceptionCollector extends AbstractExceptionCollector implements ExceptionCollector {
    private final static String pdkId = "elasticsearch";

    private final static String documents_length = "documents in the index cannot exceed";

    private final static List<RestStatus> terminate = new ArrayList<RestStatus>(
            Arrays.asList(RestStatus.GATEWAY_TIMEOUT,RestStatus.TOO_MANY_REQUESTS,RestStatus.BAD_GATEWAY));

    @Override
    protected String getPdkId() {
        return pdkId;
    }

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof ElasticsearchStatusException &&
                terminate.contains(((ElasticsearchStatusException) cause).status())){
            throw new TapPdkTerminateByServerEx(pdkId, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        if (cause instanceof ElasticsearchStatusException && RestStatus.UNAUTHORIZED==((ElasticsearchStatusException) cause).status()) {
            throw new TapPdkUserPwdInvalidEx(pdkId, username,ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof ElasticsearchStatusException && RestStatus.FORBIDDEN==((ElasticsearchStatusException) cause).status()) {
            throw new TapPdkWriteMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWriteType(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        if(cause instanceof ElasticsearchException && ((ElasticsearchException) cause).getDetailedMessage().contains("mapper_parsing_exception")){
            String pattern = "field \\[(.*?)\\]";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(((ElasticsearchException) cause).getDetailedMessage());
            String fieldName = "";
            while (m.find()) {
                fieldName = m.group(1);
            }
            throw new TapPdkWriteTypeEx(pdkId, fieldName, null, data, ErrorKit.getLastCause(cause));
        }

    }
    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        //documents length
        if(cause instanceof ElasticsearchException
                && ((ElasticsearchException) cause).getDetailedMessage().contains("illegal_argument_exception")
                &&((ElasticsearchException) cause).getDetailedMessage().contains(documents_length)){
            throw new TapPdkWriteLengthEx(pdkId, "documents", null, data, ErrorKit.getLastCause(cause));
        }

    }
}
