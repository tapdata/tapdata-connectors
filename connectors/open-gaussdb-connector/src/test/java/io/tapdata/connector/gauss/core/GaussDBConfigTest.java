package io.tapdata.connector.gauss.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GaussDBConfigTest {
    GaussDBConfig config;
    @BeforeEach
    void init() {
        config = mock(GaussDBConfig.class);
    }
    @Test
    void testParams() {
        Assertions.assertEquals("opengauss", GaussDBConfig.dbType);
        Assertions.assertEquals("com.huawei.opengauss.jdbc.Driver", GaussDBConfig.jdbcDriver);
    }

    @Test
    void testGetHaPort() {
        when(config.getHaPort()).thenCallRealMethod();
        int haPort = config.getHaPort();
    }
    @Test
    void testSetHaPort() {
        doCallRealMethod().when(config).setHaPort(anyInt());
        config.setHaPort(8000);
    }
    @Test
    void testGetHaHost() {
        when(config.getHaHost()).thenCallRealMethod();
        String haPort = config.getHaHost();
    }
    @Test
    void testSetHaHost() {
        doCallRealMethod().when(config).setHaHost(anyString());
        config.setHaHost("8000");
    }
}
