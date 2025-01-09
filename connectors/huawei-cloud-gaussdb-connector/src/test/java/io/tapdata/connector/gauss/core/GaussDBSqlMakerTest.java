package io.tapdata.connector.gauss.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.mockStatic;

@Disabled
public class GaussDBSqlMakerTest {
    @Test
    void testInstance() {
        try (MockedStatic<GaussDBSqlMaker> gr = mockStatic(GaussDBSqlMaker.class)){
            gr.when(GaussDBSqlMaker::instance).thenCallRealMethod();
            Assertions.assertNotNull(GaussDBSqlMaker.instance());
        }
    }
}
