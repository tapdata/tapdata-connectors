package io.tapdata.connector.gauss.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.mockStatic;

public class GaussDBExceptionCollectorTest {
    @Test
    void testInstance() {
        try (MockedStatic<GaussDBExceptionCollector> gr = mockStatic(GaussDBExceptionCollector.class)){
            gr.when(GaussDBExceptionCollector::instance).thenCallRealMethod();
            Assertions.assertNotNull(GaussDBExceptionCollector.instance());
        }
    }
}
