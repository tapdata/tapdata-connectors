package io.tapdata.connector.mysql;

import io.tapdata.it.UnderTest;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

/** Selects streamRead coverage for the MySQL high-performance CDC configuration. */
final class MySQLCdcOnlyCondition implements ExecutionCondition {
    private static final Set<String> ALLOWED_CAPABILITIES = Set.of("streamRead", "getStreamOffset");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (context.getElement().filter(Method.class::isInstance).isEmpty()) {
            return ConditionEvaluationResult.enabled("MySQL CDC-only selection applies to test methods");
        }
        if (context.getTags().contains("tpcc") || context.getTags().contains("performance")
                || context.getTags().contains("long-transaction")) {
            return ConditionEvaluationResult.disabled("TPCC, performance, and long-transaction tests are excluded from CDC-only coverage");
        }
        Method method = (Method) context.getElement().get();
        UnderTest[] capabilities = method.getAnnotationsByType(UnderTest.class);
        boolean streamRead = Arrays.stream(capabilities).anyMatch(test -> "streamRead".equals(test.value()));
        boolean hasNonCdcCapability = Arrays.stream(capabilities)
                .anyMatch(test -> !ALLOWED_CAPABILITIES.contains(test.value()));
        if (!streamRead || hasNonCdcCapability) {
            return ConditionEvaluationResult.disabled("high-performance suite includes streamRead-only tests");
        }
        return ConditionEvaluationResult.enabled("test exercises MySQL streamRead CDC");
    }
}
