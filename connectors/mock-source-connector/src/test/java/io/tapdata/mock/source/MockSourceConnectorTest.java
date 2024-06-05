package io.tapdata.mock.source;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

public class MockSourceConnectorTest {
    private MockSourceConnector sourceConnector;
    private TapConnectionContext connectionContext;
    @BeforeEach
    void beforeEach(){
        sourceConnector = mock(MockSourceConnector.class);
        connectionContext = mock(TapConnectionContext.class);
    }
    @Nested
    class OnStartTest{
        @Test
        void testOnStartNormal() throws Throwable {
            when(connectionContext.getConnectionConfig()).thenReturn(mock(DataMap.class));
            when(connectionContext.getLog()).thenReturn(mock(Log.class));
            doCallRealMethod().when(sourceConnector).onStart(connectionContext);
            sourceConnector.onStart(connectionContext);
            verify(connectionContext).getLog();
        }
    }
}
