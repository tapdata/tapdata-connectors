package io.tapdata.connector.postgres;

import io.tapdata.it.UnderTest;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

/** Keeps tests that mutate the read-only EFM standby out of the topology suite. */
final class PostgresEfmReadOnlyCondition implements ExecutionCondition {
    private static final Set<String> MUTATING_CAPABILITIES = Set.of(
            "writeRecord", "createTableV2", "dropTable", "clearTable", "createIndex", "deleteIndex",
            "alterTableCharset", "alterTableTTL", "alterDatabaseTimeZone");
    private static final Set<String> STANDBY_WRITING_FIXTURES = Set.of(
            "should_fail_truncate_referenced_parent_and_keep_child_when_cascade_truncate_off",
            "should_truncate_parent_and_child_when_cascade_truncate_on",
            "should_keep_drop_cascade_when_cascade_truncate_off",
            "should_clear_and_drop_plain_table_when_cascade_truncate_off");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (context.getElement().filter(Method.class::isInstance).isEmpty()) {
            return ConditionEvaluationResult.enabled("EFM read-only test selection applies to test methods");
        }

        Set<String> tags = context.getTags();
        if (tags.contains("tpcc") || tags.contains("performance")) {
            return ConditionEvaluationResult.disabled("TPCC and performance fixtures write to the read-only EFM standby");
        }

        Method method = (Method) context.getElement().get();
        if ("should_test_connection".equals(method.getName())) {
            return ConditionEvaluationResult.disabled("connectionTest does not model the EFM primary-verifier and standby-CDC topology");
        }
        if (STANDBY_WRITING_FIXTURES.contains(method.getName())) {
            return ConditionEvaluationResult.disabled("test fixture writes directly to the read-only EFM standby");
        }
        UnderTest[] capabilities = method.getAnnotationsByType(UnderTest.class);
        boolean streamRead = Arrays.stream(capabilities).anyMatch(test -> "streamRead".equals(test.value()));
        if (!streamRead && Arrays.stream(capabilities).anyMatch(UnderTest::requiresVerifier)) {
            return ConditionEvaluationResult.disabled("test setup or assertions require database writes on the read-only EFM standby");
        }
        String mutating = Arrays.stream(capabilities).map(UnderTest::value)
                .filter(MUTATING_CAPABILITIES::contains).findFirst().orElse(null);
        if (mutating != null) {
            return ConditionEvaluationResult.disabled("capability " + mutating + " writes to the read-only EFM standby");
        }
        return ConditionEvaluationResult.enabled("test does not write to the read-only EFM standby");
    }
}
