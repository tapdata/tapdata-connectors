package io.tapdata.connector.gauss.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class GaussDBConfigTest {
    GaussDBConfig config;
    @BeforeEach
    void init() {
        config = mock(GaussDBConfig.class);
    }

    @Test
    void testInstance() {
        try (MockedStatic<GaussDBConfig> gr = mockStatic(GaussDBConfig.class)){
            gr.when(GaussDBConfig::instance).thenCallRealMethod();
            Assertions.assertNotNull(GaussDBConfig.instance());
        }
    }

    @Test
    void testParams() {
        Assertions.assertEquals("opengauss", GaussDBConfig.DB_TYPE);
        Assertions.assertEquals("com.huawei.opengauss.jdbc.Driver", GaussDBConfig.JDBC_DRIVER);
    }

    @Test
    void testGetHaPort() {
        when(config.getHaPort()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> config.getHaPort());
    }
    @Test
    void testSetHaPort() {
        doCallRealMethod().when(config).setHaPort(anyInt());
        Assertions.assertDoesNotThrow(() -> config.setHaPort(8000));
    }
    @Test
    void testGetHaHost() {
        when(config.getHaHost()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> config.getHaHost());
    }
    @Test
    void testSetHaHost() {
        doCallRealMethod().when(config).setHaHost(anyString());
        Assertions.assertDoesNotThrow(() -> config.setHaHost("8000"));
    }
}
