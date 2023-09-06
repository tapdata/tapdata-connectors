package io.tapdata.connector.kafka;

import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.exception.*;
import io.tapdata.kit.ErrorKit;
import org.apache.kafka.common.errors.*;
import org.ietf.jgss.GSSException;
import sun.security.krb5.KrbException;

import javax.security.auth.login.LoginException;
import java.util.List;


public class KafkaExceptionCollector extends AbstractExceptionCollector implements ExceptionCollector {
    private final static String pdkId = "kafka";

    @Override
    protected String getPdkId() {
        return pdkId;
    }

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof BrokerNotAvailableException || cause instanceof DisconnectException) {
            throw new TapPdkTerminateByServerEx(pdkId, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause){
        if(cause instanceof AuthenticationException || cause instanceof LoginException || cause instanceof GSSException || cause instanceof KrbException ){
            throw new TapPdkUserPwdInvalidEx(pdkId,username,ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof AuthorizationException) {
            throw new TapPdkReadMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof AuthorizationException ) {
            throw new TapPdkWriteMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        if(cause instanceof RecordTooLargeException){
            throw new TapPdkWriteLengthEx(pdkId, null, null, data, ErrorKit.getLastCause(cause));
        }
    }
}
