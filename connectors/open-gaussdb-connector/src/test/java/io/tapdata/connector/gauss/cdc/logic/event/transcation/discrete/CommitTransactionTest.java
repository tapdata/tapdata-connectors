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
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class CommitTransactionTest {

    CommitTransaction event;

    @BeforeEach
    void init() {
        event = mock(CommitTransaction.class);
    }

    @Nested
    class InstanceTest {
        @Test
        void testNormal() {
            try (MockedStatic<CommitTransaction> util = mockStatic(CommitTransaction.class)){
                util.when(CommitTransaction::instance).thenCallRealMethod();
                CommitTransaction instance = CommitTransaction.instance();
                Assertions.assertNotNull(instance);
            }
        }
        @Test
        void testNotNull() {
            try (MockedStatic<CommitTransaction> util = mockStatic(CommitTransaction.class)){
                util.when(CommitTransaction::instance).thenCallRealMethod();
                CommitTransaction instance = CommitTransaction.instance();
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

        @BeforeEach
        void init() {
            readIndexV1 = 0;
            logEvent = mock(ByteBuffer.class);
            param = mock(AnalyzeLog.AnalyzeParam.class);

            when(event.analyze(logEvent, param)).thenCallRealMethod();
        }

        void assertVerify(byte[][] v1Bytes, byte[] timestamp,
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
                        case 1: return v1Bytes[0];
                        case 2: return v1Bytes[1];
                        case 3: return v1Bytes[2];
                        case 4: return v1Bytes[3];
                        default: return v1Bytes[4];
                    }
                });
                util.when(() -> {
                    LogicUtil.read(any(ByteBuffer.class), anyInt(), anyInt());
                }).thenReturn(timestamp);
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
                assertVerify(
                        new byte[][]{"X".getBytes(), new byte[]{9}, "T".getBytes(), new byte[0], new byte[0]},
                        "2024-03-04 22:43:00.003".getBytes(),
                         4, 1,
                        1, 1);
            });
        }


        @Test
        void testXidTagNotXAndTimestampTagNotT() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(
                        new byte[][]{"".getBytes(), "".getBytes(), "".getBytes(), new byte[0], new byte[0]},
                        new byte[]{},
                        3 , 0,
                        0, 0);
            });
        }

        @Test
        void testXidTagIsX() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(new byte[][]{"X".getBytes(), new byte[]{9}, "".getBytes(), new byte[0], new byte[0]},
                        "2024-03-04 22:43:00.003".getBytes(),
                        4 , 0,
                        0, 1);
            });
        }

        @Test
        void testXidTagIsXAndXidIsNull() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(
                        new byte[][]{"X".getBytes(), null, "".getBytes(), new byte[0], new byte[0]},
                        new byte[]{},
                        4 , 0,
                        0, 0);
            });
        }

        @Test
        void testTimestampTagIsT(){
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(
                        new byte[][]{"".getBytes(), "T".getBytes(), "".getBytes(), new byte[0], new byte[0]},
                        null,
                        3, 1,
                        0, 0);
            });
        }

        @Test
        void testNullCommitTime() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(
                        new byte[][]{"".getBytes(), "T".getBytes(), "".getBytes(), new byte[0], new byte[0]},
                        null,
                        3, 1,
                        0, 0);
            });
        }

        @Test
        void testNotNullCommitTime() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(
                        new byte[][]{"".getBytes(), "T".getBytes(), "".getBytes(), new byte[0], new byte[0]},
                        "2024-03-04 22:43:00.003".getBytes(),
                        3, 1,
                        1, 0);
            });
        }
    }
}
