package io.tapdata.connector.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresEfmEnvironmentTest {
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
