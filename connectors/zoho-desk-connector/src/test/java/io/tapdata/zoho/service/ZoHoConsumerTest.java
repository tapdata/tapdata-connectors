package io.tapdata.zoho.service;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.zoho.entity.ZoHoOffset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ZoHoConsumerTest {
    BiConsumer<List<TapEvent>, Object> consumer;
    @BeforeEach
    void init() {
        consumer = mock(BiConsumer.class);
        when(consumer.andThen(any(BiConsumer.class))).thenReturn(mock(BiConsumer.class));
        doNothing().when(consumer).accept(anyList(), any(Object.class));
    }
    @Nested
    class AcceptTest {

        @Test
        void testNormal() {
            ZoHoConsumer zoHoConsumer = new ZoHoConsumer(consumer);
            zoHoConsumer.accept(new ArrayList<>(), 1L);
            verify(consumer).accept(anyList(), any(Object.class));
        }
        @Test
        void testZoHoOffset() {
            ZoHoConsumer zoHoConsumer = new ZoHoConsumer(consumer);
            zoHoConsumer.accept(new ArrayList<>(), new ZoHoOffset());
            verify(consumer).accept(anyList(), any(Object.class));
        }
    }

    @Nested
    class AndThenTest {
        @Test
        void testNormal() {
            ZoHoConsumer zoHoConsumer = new ZoHoConsumer(consumer);
            Assertions.assertNotNull(zoHoConsumer.andThen(consumer));
            verify(consumer).andThen(consumer);
        }
    }
}