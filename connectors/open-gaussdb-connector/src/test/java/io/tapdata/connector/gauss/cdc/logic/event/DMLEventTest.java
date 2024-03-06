package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.connector.gauss.cdc.logic.AnalyzeLog;
import io.tapdata.connector.gauss.cdc.logic.event.dml.CollectEntity;
import io.tapdata.connector.gauss.entity.IllegalDataLengthException;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.entity.event.TapEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DMLEventTest {
    ByteBuffer logEvent;
    AnalyzeLog.AnalyzeParam param;
    DMLEvent dmlEvent;
    @BeforeEach
    void init() {
        dmlEvent = mock(DMLEvent.class);
        logEvent = mock(ByteBuffer.class);
        param = mock(AnalyzeLog.AnalyzeParam.class);
    }

    @Nested
    class AnalyzeTest {
        Event.EventEntity<TapEvent> entity;
        CollectEntity instance;
        int hasRemainingIndex;
        int hasRemainingTimes;

        boolean readTag;
        boolean readV2Tag;

        @BeforeEach
        void init() {
            entity = mock(Event.EventEntity.class);
            readTag = true;
            readV2Tag = true;
            instance = mock(CollectEntity.class);
            when(instance.withSchema(anyString())).thenReturn(instance);
            when(instance.withTable(anyString())).thenReturn(instance);
            when(instance.withAfter(anyMap())).thenReturn(instance);
            when(instance.withFieldType(anyMap())).thenReturn(instance);
            when(instance.withBefore(anyMap())).thenReturn(instance);

            when(logEvent.hasRemaining()).then(a -> {
                if (hasRemainingIndex < hasRemainingTimes) {
                    hasRemainingIndex++;
                    return true;
                }
                return false;
            });
            doNothing().when(dmlEvent).collectAttr(any(ByteBuffer.class), anyMap(), anyMap(), any(AnalyzeLog.AnalyzeParam.class));
            when(dmlEvent.collect(instance)).thenReturn(entity);
            when(dmlEvent.analyze(logEvent,param)).thenCallRealMethod();
        }

        void assertVerify(byte[][] read, int isNTimes, int isOTimes, int isOOrNTimes, int readTimes, int hasRemainingTimes) {
            try (MockedStatic<CollectEntity> c = mockStatic(CollectEntity.class);
                 MockedStatic<LogicUtil> l = mockStatic(LogicUtil.class)){
                c.when(CollectEntity::instance).thenReturn(instance);
                l.when(() -> { LogicUtil.read(logEvent, 1); }).then(a -> {return read[hasRemainingIndex-1];});
                l.when(() -> { LogicUtil.read(logEvent, 2, 32); }).then(a -> {
                    if (readV2Tag) {
                        readV2Tag = false;
                        return "schema".getBytes();
                    }
                    return "table".getBytes();
                });

                Event.EventEntity<TapEvent> analyze = dmlEvent.analyze(logEvent, param);
                Assertions.assertNotNull(analyze);
                c.verify(CollectEntity::instance, times(1));
                l.verify(() -> { LogicUtil.read(logEvent, 1); }, times(readTimes));
                l.verify(() -> { LogicUtil.read(logEvent, 2, 32); }, times(2));
            } finally {
                verify(instance, times(1)).withSchema(anyString());
                verify(instance, times(1)).withTable(anyString());
                verify(instance, times(isNTimes)).withAfter(anyMap());
                verify(instance, times(isNTimes)).withFieldType(anyMap());
                verify(instance, times(isOTimes)).withBefore(anyMap());

                verify(logEvent, times(hasRemainingTimes)).hasRemaining();
                verify(dmlEvent, times(isOOrNTimes)).collectAttr(any(ByteBuffer.class), anyMap(), anyMap(), any(AnalyzeLog.AnalyzeParam.class));
                verify(dmlEvent, times(1)).collect(instance);
            }
        }

        @Test
        void testHasNextTagIsN() {
            hasRemainingTimes = 1;
            hasRemainingIndex = 0;
            byte[][] read = new byte[][]{"N".getBytes(), "P".getBytes()};
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(read, 1, 0, 1, 2, 2);
            });
        }
        @Test
        void testHasNextTagIsO() {
            hasRemainingTimes = 1;
            hasRemainingIndex = 0;
            byte[][] read = new byte[][]{"O".getBytes(), "P".getBytes()};
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(read, 0, 1, 1, 2, 2);
            });
        }
        @Test
        void testHasNextTagIsP() {
            hasRemainingTimes = 1;
            hasRemainingIndex = 0;
            byte[][] read = new byte[][]{"P".getBytes(), "P".getBytes()};
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(read, 0, 0, 0, 1, 1);
            });
        }
        @Test
        void testHasNextTagIsOtherChar() {
            hasRemainingTimes = 1;
            hasRemainingIndex = 0;
            byte[][] read = new byte[][]{"C".getBytes(), "P".getBytes()};
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(read, 0, 0, 0, 2, 1);
            });
        }

        @Test
        void testHasNextTagAreNAndO() {
            hasRemainingTimes = 2;
            hasRemainingIndex = 0;
            byte[][] read = new byte[][]{"O".getBytes(), "N".getBytes(), "P".getBytes()};
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(read, 1, 1, 2, 3, 2);
            });
        }

    }

    @Nested
    class CollectAttrTest {
        Map<String, Object> kvMap;
        java.util.Map<String, Integer> kTypeMap;
        byte[] attrNum;
        byte[] oid;

        @BeforeEach
        void init() {
            attrNum = new byte[]{1};
            oid = new byte[]{2};

            kvMap = mock(Map.class);
            doNothing().when(kvMap).putAll(anyMap());
            kTypeMap = mock(Map.class);
            when(kTypeMap.put(anyString(), anyInt())).thenReturn(1);

            doCallRealMethod().when(dmlEvent).collectAttr(logEvent, kvMap, kTypeMap, param);
        }
        void assertVerify(int attrNumValue, int oidValue,
                          byte[] attrName, byte[] value,
                          int readNumTimes, int numToIntTimes, int readNameTimes, int readValueTimes, int readOidTimes, int oidToIntTimes,
                          int putAll, int put) {
            try (MockedStatic<LogicUtil> l = mockStatic(LogicUtil.class)){
                l.when(() -> { LogicUtil.read(logEvent, 2); }).thenReturn(attrNum);
                l.when(() -> { LogicUtil.read(logEvent, 4); }).thenReturn(oid);
                l.when(() -> { LogicUtil.read(logEvent, 2, 32); }).thenReturn(attrName);
                l.when(() -> { LogicUtil.readValue(logEvent, 4, 32); }).thenReturn(value);
                l.when(() -> { LogicUtil.bytesToInt(attrNum); }).thenReturn(attrNumValue);
                l.when(() -> { LogicUtil.bytesToInt(oid); }).thenReturn(oidValue);
                dmlEvent.collectAttr(logEvent, kvMap, kTypeMap, param);
                l.verify(() -> { LogicUtil.read(logEvent, 2); }, times(readNumTimes));
                l.verify(() -> { LogicUtil.read(logEvent, 4); }, times(readOidTimes));
                l.verify(() -> { LogicUtil.read(logEvent, 2, 32); }, times(readNameTimes));
                l.verify(() -> { LogicUtil.readValue(logEvent, 4, 32); }, times(readValueTimes));
                l.verify(() -> { LogicUtil.bytesToInt(attrNum); }, times(numToIntTimes));
                l.verify(() -> { LogicUtil.bytesToInt(oid); }, times(oidToIntTimes));
            } finally {
                verify(kvMap, times(putAll)).putAll(anyMap());
                verify(kTypeMap, times(put)).put(anyString(), anyInt());
            }
        }

        @Test
        void testNotAnyAttrs() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(0, 1,
                        "key".getBytes(), "value".getBytes(),
                        1, 1, 0, 0, 0, 0,
                        1 , 0);
            });
        }

        @Test
        void testHasAttrs(){
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1, 1,
                        "key".getBytes(), "value".getBytes(),
                        1, 1, 1, 1, 1, 1,
                        1,1);
            });
        }

        @Test
        void testStartWith() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1, 1,
                        "\"key".getBytes(), "value".getBytes(),
                        1, 1, 1, 1, 1, 1,
                        1, 1);
            });
        }

        @Test
        void testEndWith() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1, 1,
                        "key\"".getBytes(), "value".getBytes(),
                        1, 1, 1, 1, 1, 1,
                        1, 1);
            });
        }

        @Test
        void testExceptionButMapContainNullValue() {
            when(kTypeMap.put(anyString(), anyInt())).then(a -> {
                throw new IllegalDataLengthException("Failed");
            });
            Assertions.assertThrows(IllegalDataLengthException.class, () -> {
                assertVerify(1, 1,
                        "key".getBytes(), null,
                        1, 1, 1, 1, 1, 1,
                        0, 1);
            });
        }

        @Test
        void testException() {
            when(kTypeMap.put(anyString(), anyInt())).then(a -> {
                throw new IllegalDataLengthException("Failed");
            });
            Assertions.assertThrows(IllegalDataLengthException.class,() -> {
                assertVerify(1, 1,
                        "key".getBytes(), "value".getBytes(),
                        1, 1, 1, 1, 1, 1,
                        0, 1);
            });
        }
    }
}
