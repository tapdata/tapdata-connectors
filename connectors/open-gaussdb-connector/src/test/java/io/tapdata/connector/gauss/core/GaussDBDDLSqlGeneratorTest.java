package io.tapdata.connector.gauss.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.mockStatic;

public class GaussDBDDLSqlGeneratorTest {
    @Test
    void testInstance() {
        try (MockedStatic<GaussDBDDLSqlGenerator> gr = mockStatic(GaussDBDDLSqlGenerator.class)){
            gr.when(GaussDBDDLSqlGenerator::instance).thenCallRealMethod();
            Assertions.assertNotNull(GaussDBDDLSqlGenerator.instance());
        }
    }
}
