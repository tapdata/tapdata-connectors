package io.tapdata.connector.paimon;

import io.tapdata.connector.paimon.service.PaimonService;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonConnectorControlTest {

    @Test
    @SuppressWarnings("unchecked")
    void onStartMustBindFlushCallbackBeforeServiceInitialization() throws Throwable {
        PaimonConnector connector = new PaimonConnector();
        TapConnectionContext context = mock(TapConnectionContext.class);
        Consumer<Object> callback = mock(Consumer.class);
        Log log = mock(Log.class);
        when(context.getConnectionConfig())
                .thenReturn(DataMap.create().kv("warehouse", "/tmp/paimon-test"));
        when(context.getNodeConfig()).thenReturn(DataMap.create());
        when(context.getLog()).thenReturn(log);
        when(context.getFlushOffsetCallback()).thenReturn(callback);

        try (MockedConstruction<PaimonConfig> ignored =
                        mockConstruction(
                                PaimonConfig.class,
                                (config, constructionContext) ->
                                        when(config.load(anyMap())).thenReturn(config));
                MockedConstruction<PaimonService> construction =
                        mockConstruction(PaimonService.class)) {
            connector.onStart(context);

            PaimonService service = construction.constructed().get(0);
            InOrder order = inOrder(service);
            order.verify(service).setFlushOffsetCallback(callback);
            order.verify(service).init();
        }
    }

    @Test
    void heartbeatMustBeForwardedToService() throws Throwable {
        PaimonConnector connector = new PaimonConnector();
        PaimonService service = mock(PaimonService.class);
        setService(connector, service);
        HeartbeatEvent heartbeat = new HeartbeatEvent().init();

        connector.processControl(mock(TapConnectorContext.class), heartbeat);

        verify(service).processHeartbeat(heartbeat);
    }

    @Test
    void heartbeatFailureMustPropagateWithoutBeingReportedAsSuccess() throws Exception {
        PaimonConnector connector = new PaimonConnector();
        PaimonService service = mock(PaimonService.class);
        setService(connector, service);
        HeartbeatEvent heartbeat = new HeartbeatEvent().init();
        IOException failure = new IOException("heartbeat callback failed");
        doThrow(failure).when(service).processHeartbeat(heartbeat);

        Throwable thrown =
                assertThrows(
                        Throwable.class,
                        () -> connector.processControl(mock(TapConnectorContext.class), heartbeat));

        assertSame(failure, thrown);
    }

    private static void setService(PaimonConnector connector, PaimonService service)
            throws Exception {
        Field field = PaimonConnector.class.getDeclaredField("paimonService");
        field.setAccessible(true);
        field.set(connector, service);
    }
}
