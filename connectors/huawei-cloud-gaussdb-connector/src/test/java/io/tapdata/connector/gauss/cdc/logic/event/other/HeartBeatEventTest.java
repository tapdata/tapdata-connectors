package io.tapdata.connector.gauss.cdc.logic.event.other;

import io.tapdata.connector.gauss.cdc.logic.AnalyzeLog;
import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.entity.event.TapEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class HeartBeatEventTest {

    HeartBeatEvent event;

    @BeforeEach
    void init() {
        event = mock(HeartBeatEvent.class);
    }

    @Nested
    class InstanceTest {
        @Test
        void testNormal() {
            try (MockedStatic<HeartBeatEvent> util = mockStatic(HeartBeatEvent.class)){
                util.when(HeartBeatEvent::instance).thenCallRealMethod();
                HeartBeatEvent instance = HeartBeatEvent.instance();
                Assertions.assertNotNull(instance);
            }
        }
        @Test
        void testNotNull() {
            try (MockedStatic<HeartBeatEvent> util = mockStatic(HeartBeatEvent.class)){
                util.when(HeartBeatEvent::instance).thenCallRealMethod();
                HeartBeatEvent instance = HeartBeatEvent.instance();
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
        ByteBuffer logEvent;
        byte[] bytes;

        @BeforeEach
        void init() {
            logEvent = mock(ByteBuffer.class);
            bytes = new byte[0];

            when(event.analyze(logEvent, new HashMap<>())).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            try (MockedStatic<LogicUtil> tap = mockStatic(LogicUtil.class)){
                tap.when(() -> {
                    LogicUtil.read(any(ByteBuffer.class), anyInt());
                }).thenReturn(bytes);
                tap.when(() -> {
                    LogicUtil.byteToLong(any(byte[].class));
                }).thenReturn(1L);
                Event.EventEntity<TapEvent> collect = event.analyze(logEvent, new HashMap<>());
                Assertions.assertNotNull(collect);
                tap.verify(() -> {
                    LogicUtil.read(any(ByteBuffer.class), anyInt());
                }, times(4));
                tap.verify(() -> {
                    LogicUtil.byteToLong(any(byte[].class));
                }, times(1));
            }
        }
    }
}
