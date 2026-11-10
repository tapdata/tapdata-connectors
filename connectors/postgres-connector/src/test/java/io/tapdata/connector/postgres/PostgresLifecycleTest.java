package io.tapdata.connector.postgres;

import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.codec.TapCodecsRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresLifecycleTest {
    @Test
    void releaseExternalClosesExistingPoolsWithoutStartingReplacementPools() throws Throwable {
        PostgresConnector connector = spy(new PostgresConnector());
        PostgresJdbcContext jdbc = mock(PostgresJdbcContext.class);
        PostgresTest connectionTest = mock(PostgresTest.class);
        PostgresConfig config = new PostgresConfig();
        config.setLogPluginName("physical");
        ReflectionTestUtils.setField(connector, "postgresConfig", config);
        ReflectionTestUtils.setField(connector, "postgresJdbcContext", jdbc);
        ReflectionTestUtils.setField(connector, "jdbcContext", jdbc);
        ReflectionTestUtils.setField(connector, "postgresTest", connectionTest);
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());

        // A second start must preserve the pool captured by the running CDC/verifier.
        connector.onStart(null);
        assertSame(jdbc, ReflectionTestUtils.getField(connector, "postgresJdbcContext"));
        functions.getReleaseExternalFunction().release(mock(TapConnectorContext.class));

        verify(jdbc).close();
        verify(connectionTest).close();
        assertNull(ReflectionTestUtils.getField(connector, "postgresJdbcContext"));
        assertNull(ReflectionTestUtils.getField(connector, "postgresTest"));
        assertNull(ReflectionTestUtils.getField(connector, "jdbcContext"));

        // ConnectorIT calls stop after releaseExternal; do not close stale pools again.
        connector.onStop(null);
        verify(jdbc, times(1)).close();
        verify(connectionTest, times(1)).close();
    }

}
