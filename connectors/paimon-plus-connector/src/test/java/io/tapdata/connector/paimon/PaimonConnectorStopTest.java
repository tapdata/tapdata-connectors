package io.tapdata.connector.paimon;

import io.tapdata.connector.paimon.service.PaimonService;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonConnectorStopTest {

    @Test
    void closeFailureMustBePropagatedAndServiceReferenceCleared() throws Exception {
        PaimonConnector connector = new PaimonConnector();
        PaimonService service = mock(PaimonService.class);
        IOException failure = new IOException("flush failed");
        doThrow(failure).when(service).close();
        setService(connector, service);

        TapConnectionContext context = mock(TapConnectionContext.class);
        when(context.getLog()).thenReturn(mock(Log.class));

        Throwable thrown = assertThrows(Throwable.class, () -> connector.onStop(context));

        assertEquals(failure, thrown);
        assertNull(getService(connector));
    }

    private static void setService(PaimonConnector connector, PaimonService service)
            throws Exception {
        Field field = PaimonConnector.class.getDeclaredField("paimonService");
        field.setAccessible(true);
        field.set(connector, service);
    }

    private static PaimonService getService(PaimonConnector connector) throws Exception {
        Field field = PaimonConnector.class.getDeclaredField("paimonService");
        field.setAccessible(true);
        return (PaimonService) field.get(connector);
    }
}
