package io.tapdata.connector.gauss.cdc.logic.event;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EventFactoryTest {
    @Test
    void testParams() {
        Assertions.assertEquals(10 * 60 * 1000L, EventFactory.DEFAULT_CACHE_TIME);
        Assertions.assertEquals(1000L, EventFactory.DEFAULT_CACHE_COUNT);
    }
}
