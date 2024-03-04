package io.tapdata.connector.gauss.cdc.logic.event.dml;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class UpdateEventTest {
    UpdateEvent event;

    @BeforeEach
    void init() {
        event = mock(UpdateEvent.class);
    }

    @Nested
    class InstanceTest {
        @Test
        void testNormal() {
            try (MockedStatic<UpdateEvent> util = mockStatic(UpdateEvent.class)){
                util.when(UpdateEvent::instance).thenCallRealMethod();
                UpdateEvent instance = UpdateEvent.instance();
                Assertions.assertNotNull(instance);
            }
        }
        @Test
        void testNotNull() {
            UpdateEvent.instance = mock(UpdateEvent.class);
            try (MockedStatic<UpdateEvent> util = mockStatic(UpdateEvent.class)){
                util.when(UpdateEvent::instance).thenCallRealMethod();
                UpdateEvent instance = UpdateEvent.instance();
                Assertions.assertNotNull(instance);
            }
        }
    }

    @Nested
    class ProcessTest {
        ByteBuffer logEvent;
        EventParam processParam;
        @BeforeEach
        void init() {
            logEvent = mock(ByteBuffer.class);
            processParam = mock(EventParam.class);

            when(event.process(logEvent, processParam)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Event.EventEntity<TapEvent> process = event.process(logEvent, processParam);
            Assertions.assertNull(process);
        }
    }

    @Nested
    class CollectTest {
        CollectEntity entity;
        TapUpdateRecordEvent e;
        @BeforeEach
        void init() {
            entity = mock(CollectEntity.class);
            when(entity.getTable()).thenReturn("table");
            when(entity.getBefore()).thenReturn(mock(Map.class));
            when(entity.getAfter()).thenReturn(mock(Map.class));

            e = mock(TapUpdateRecordEvent.class);
            when(e.after(anyMap())).thenReturn(e);
            when(e.before(anyMap())).thenReturn(e);
            when(e.table(anyString())).thenReturn(e);

            when(event.collect(entity)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            try (MockedStatic<TapUpdateRecordEvent> tap = mockStatic(TapUpdateRecordEvent.class)){
                tap.when(TapUpdateRecordEvent::create).thenReturn(e);
                Event.EventEntity<TapEvent> collect = event.collect(entity);
                Assertions.assertNotNull(collect);
            }
        }
    }
}
