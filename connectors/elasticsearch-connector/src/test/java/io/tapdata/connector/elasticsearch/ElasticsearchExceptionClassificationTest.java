package io.tapdata.connector.elasticsearch;

import io.tapdata.exception.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.Test;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;

class ElasticsearchExceptionClassificationTest {
    private final ElasticsearchExceptionCollector collector = new ElasticsearchExceptionCollector();

    @Test
    void unavailableServersAndThrottlingRequestRetry() {
        for (RestStatus status : new RestStatus[]{RestStatus.GATEWAY_TIMEOUT, RestStatus.TOO_MANY_REQUESTS, RestStatus.BAD_GATEWAY}) {
            ElasticsearchStatusException cause = new ElasticsearchStatusException("unavailable", status);
            assertSame(cause, assertThrows(TapPdkTerminateByServerEx.class,
                    () -> collector.collectTerminateByServer(cause)).getCause());
        }
        assertDoesNotThrow(() -> collector.collectTerminateByServer(new IllegalStateException("local failure")));
    }

    @Test
    void authenticationAndAuthorizationHaveDifferentDiagnostics() {
        ElasticsearchStatusException auth = new ElasticsearchStatusException("credentials", RestStatus.UNAUTHORIZED);
        assertSame(auth, assertThrows(TapPdkUserPwdInvalidEx.class,
                () -> collector.collectUserPwdInvalid("reader", auth)).getCause());
        ElasticsearchStatusException forbidden = new ElasticsearchStatusException("privileges", RestStatus.FORBIDDEN);
        assertSame(forbidden, assertThrows(TapPdkWriteMissingPrivilegesEx.class,
                () -> collector.collectWritePrivileges("insert", Collections.singletonList("write"), forbidden)).getCause());
        assertDoesNotThrow(() -> collector.collectUserPwdInvalid("reader", forbidden));
    }

    @Test
    void mappingAndDocumentLimitFailuresRetainTheirCategory() {
        ElasticsearchException mapping = new ElasticsearchException("mapper_parsing_exception field [price]");
        assertThrows(TapPdkWriteTypeEx.class, () -> collector.collectWriteType("price", "number", "text", mapping));
        ElasticsearchException limit = new ElasticsearchException("illegal_argument_exception documents in the index cannot exceed the configured limit");
        assertThrows(TapPdkWriteLengthEx.class, () -> collector.collectWriteLength("document", null, "data", limit));
        assertDoesNotThrow(() -> collector.collectWriteLength("document", null, "data", mapping));
    }
}
