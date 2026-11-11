package io.tapdata.connector.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresEfmEnvironmentTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "should_discover_and_read_ordinary_view_with_projected_columns",
            "should_read_only_granted_view_and_isolate_schema_metadata",
            "should_discover_reference_key_when_referenced_table_not_granted",
            "should_fail_truncate_referenced_parent_and_keep_child_when_cascade_truncate_off",
            "should_truncate_parent_and_child_when_cascade_truncate_on",
            "should_keep_drop_cascade_when_cascade_truncate_off",
            "should_clear_and_drop_plain_table_when_cascade_truncate_off"
    })
    void dbForgeEfmSkipsFixturesThatWriteDirectlyToStandby(String methodName) throws Exception {
        assertEfmMethodSelection(PostgresConnectorIT.class.getDeclaredMethod(methodName), true);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "should_stream_composite_add_columns_ddl",
            "should_stream_read_ddl_dml_drop_ddl_dml_sequence"
    })
    void dbForgeEfmKeepsSourceDdlCoverage(String methodName) throws Exception {
        assertEfmMethodSelection(io.tapdata.it.ConnectorIT.class.getDeclaredMethod(methodName), false);
    }

    private void assertEfmMethodSelection(Method method, boolean disabled) {
        String previous = System.getProperty("connector.it.config.source");
        try {
            System.setProperty("connector.it.config.source", "dbforge");
            ExtensionContext context = mock(ExtensionContext.class);
            when(context.getElement()).thenReturn(Optional.of(method));
            when(context.getTags()).thenReturn(Set.of());
            assertEquals(disabled,
                    new PostgresEfmReadOnlyCondition().evaluateExecutionCondition(context).isDisabled());
        } finally {
            if (previous == null) System.clearProperty("connector.it.config.source");
            else System.setProperty("connector.it.config.source", previous);
        }
    }

    @Test
    void jsonConfigDoesNotRunEfmAgainstTheSingleNodeDatabase() {
        String previous = System.getProperty("connector.it.config.source");
        try {
            System.setProperty("connector.it.config.source", "json");
            ExtensionContext context = mock(ExtensionContext.class);
            assertTrue(new PostgresEfmReadOnlyCondition().evaluateExecutionCondition(context).isDisabled());
            assertThrows(IllegalStateException.class, () -> new PostgresEfmTdeConnectorIT().createVerifier());

            System.setProperty("connector.it.config.source", "dbforge");
            when(context.getElement()).thenReturn(Optional.of(PostgresEfmTdeConnectorIT.class));
            assertFalse(new PostgresEfmReadOnlyCondition().evaluateExecutionCondition(context).isDisabled());
        } finally {
            if (previous == null) System.clearProperty("connector.it.config.source");
            else System.setProperty("connector.it.config.source", previous);
        }
    }
}
