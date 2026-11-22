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
    private static final String PDK_ID = "elasticsearch";

    private static final String DOCUMENTS_LENGTH = "documents in the index cannot exceed";

    private static final List<RestStatus> terminate = new ArrayList<>(
            Arrays.asList(RestStatus.GATEWAY_TIMEOUT,RestStatus.TOO_MANY_REQUESTS,RestStatus.BAD_GATEWAY));

    @Override
    protected String getPdkId() {
        return PDK_ID;
    }

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof ElasticsearchStatusException &&
                terminate.contains(((ElasticsearchStatusException) cause).status())){
            throw new TapPdkTerminateByServerEx(PDK_ID, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        if (cause instanceof ElasticsearchStatusException && RestStatus.UNAUTHORIZED==((ElasticsearchStatusException) cause).status()) {
            throw new TapPdkUserPwdInvalidEx(PDK_ID, username,ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof ElasticsearchStatusException && RestStatus.FORBIDDEN==((ElasticsearchStatusException) cause).status()) {
            throw new TapPdkWriteMissingPrivilegesEx(PDK_ID, operation, privileges, ErrorKit.getLastCause(cause));
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
            throw new TapPdkWriteTypeEx(PDK_ID, fieldName, null, data, ErrorKit.getLastCause(cause));
        }

    }
    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        //documents length
        if(cause instanceof ElasticsearchException
                && ((ElasticsearchException) cause).getDetailedMessage().contains("illegal_argument_exception")
                &&((ElasticsearchException) cause).getDetailedMessage().contains(DOCUMENTS_LENGTH)){
            throw new TapPdkWriteLengthEx(PDK_ID, "documents", null, data, ErrorKit.getLastCause(cause));
        }

    }
}
