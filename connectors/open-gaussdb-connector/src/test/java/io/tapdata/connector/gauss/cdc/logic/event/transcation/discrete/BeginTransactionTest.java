package io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete;

import io.tapdata.connector.gauss.cdc.logic.AnalyzeLog;
import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.connector.gauss.util.TimeUtil;
import io.tapdata.entity.event.TapEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class BeginTransactionTest {

    BeginTransaction event;

    @BeforeEach
    void init() {
        event = mock(BeginTransaction.class);
    }

    @Nested
    class InstanceTest {
        @Test
        void testNormal() {
            try (MockedStatic<BeginTransaction> util = mockStatic(BeginTransaction.class)){
                util.when(BeginTransaction::instance).thenCallRealMethod();
                BeginTransaction instance = BeginTransaction.instance();
                Assertions.assertNotNull(instance);
            }
        }
        @Test
        void testNotNull() {
            try (MockedStatic<BeginTransaction> util = mockStatic(BeginTransaction.class)){
                util.when(BeginTransaction::instance).thenCallRealMethod();
                BeginTransaction instance = BeginTransaction.instance();
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
        AnalyzeLog.AnalyzeParam param;

        int readIndexV1;
        int readIndexV2;

        @BeforeEach
        void init() {
            readIndexV1 = 0;
            readIndexV2 = 0;
            logEvent = mock(ByteBuffer.class);
            param = mock(AnalyzeLog.AnalyzeParam.class);

            when(event.analyze(logEvent, param)).thenCallRealMethod();
        }

        void assertVerify(String commitTag, String userTag,
                          byte[][] bytesV2,
                          int v1Times, int v2Times,
                          int timeUtilTimes, int byteToLongTimes) {
            try (MockedStatic<LogicUtil> util = mockStatic(LogicUtil.class);
                 MockedStatic<TimeUtil> time = mockStatic(TimeUtil.class)) {
                util.when(() -> {
                    LogicUtil.byteToLong(any(byte[].class));
                }).thenReturn(0L);
                util.when(() -> {
                    LogicUtil.read(any(ByteBuffer.class), anyInt());
                }).then(a -> {
                    readIndexV1++;
                    switch (readIndexV1) {
                        case 1: return new byte[0];
                        case 2: return new byte[0];
                        case 3: return commitTag.getBytes();
                        case 4: return userTag.getBytes();
                        default: return new byte[0];
                    }
                });
                util.when(() -> {
                    LogicUtil.read(any(ByteBuffer.class), anyInt(), anyInt());
                }).then(a -> {
                    readIndexV2++;
                    switch (readIndexV2) {
                        case 1: return bytesV2[0];
                        default: return bytesV2[1];
                    }
                });
                time.when(() -> {
                    TimeUtil.parseTimestamp(anyString(), anyInt());
                }).thenReturn(0L);
                Event.EventEntity<TapEvent> analyze = event.analyze(logEvent, param);
                Assertions.assertNotNull(analyze);
                util.verify(() -> {
                    LogicUtil.read(any(ByteBuffer.class), anyInt());
                }, times(v1Times));
                util.verify(() -> {
                    LogicUtil.read(any(ByteBuffer.class), anyInt(), anyInt());
                }, times(v2Times));
                time.verify(() -> {
                    TimeUtil.parseTimestamp(anyString(), anyInt());
                }, times(timeUtilTimes));
                util.verify(() -> {
                    LogicUtil.byteToLong(any(byte[].class));
                }, times(byteToLongTimes));
            }
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify("", "",
                        new byte[][]{new byte[]{1}, new byte[]{1}},
                        5 ,0,
                        0, 2);
            });
        }

        @Test
        void testCommitTimeTagCharIsT() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify("T", "",
                        new byte[][]{null, new byte[]{1}},
                        5 ,1,
                        0, 2);
            });
        }

        @Test
        void testUserTagCharIsN(){
            Assertions.assertDoesNotThrow(() -> {
                assertVerify("", "N",
                        new byte[][]{null, new byte[]{1}},
                        5 ,1,
                        0, 2);
            });
        }

        @Test
        void testNullCommitTime() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify("T", "",
                        new byte[][]{null, new byte[]{1}},
                        5 ,1,
                        0, 2);
            });
        }

        @Test
        void testNotNullCommitTime() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify("T", "",
                        new byte[][]{"2024-03-04 18:00:00.001".getBytes(), new byte[]{1}},
                        5 ,1,
                        1, 2);
            });
        }
    }
}
