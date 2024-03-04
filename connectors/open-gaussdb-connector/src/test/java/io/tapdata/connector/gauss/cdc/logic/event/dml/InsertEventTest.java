package io.tapdata.connector.gauss.cdc.logic.event.dml;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
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

public class InsertEventTest {
    InsertEvent event;

    @BeforeEach
    void init() {
        event = mock(InsertEvent.class);
    }

    @Nested
    class InstanceTest {
        @Test
        void testNormal() {
            try (MockedStatic<InsertEvent> util = mockStatic(InsertEvent.class)){
                util.when(InsertEvent::instance).thenCallRealMethod();
                InsertEvent instance = InsertEvent.instance();
                Assertions.assertNotNull(instance);
            }
        }
        @Test
        void testNotNull() {
            InsertEvent.instance = mock(InsertEvent.class);
            try (MockedStatic<InsertEvent> util = mockStatic(InsertEvent.class)){
                util.when(InsertEvent::instance).thenCallRealMethod();
                InsertEvent instance = InsertEvent.instance();
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
        TapInsertRecordEvent e;
        @BeforeEach
        void init() {
            entity = mock(CollectEntity.class);
            when(entity.getTable()).thenReturn("table");
            when(entity.getBefore()).thenReturn(mock(Map.class));

            e = mock(TapInsertRecordEvent.class);
            when(e.after(anyMap())).thenReturn(e);
            when(e.table(anyString())).thenReturn(e);

            when(event.collect(entity)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            try (MockedStatic<TapInsertRecordEvent> tap = mockStatic(TapInsertRecordEvent.class)){
                tap.when(TapInsertRecordEvent::create).thenReturn(e);
                Event.EventEntity<TapEvent> collect = event.collect(entity);
                Assertions.assertNotNull(collect);
            }
        }
    }
}
