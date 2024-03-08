package io.tapdata.connector.gauss.util;

import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.gauss.entity.IllegalDataLengthException;
import io.tapdata.connector.gauss.enums.CdcConstant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicUtilTest {

    @Nested
    class BytesToIntTest {
        void assertVerify(Number value, Number excepted, byte[] bytes) {
            try (MockedStatic<LogicUtil> util = mockStatic(LogicUtil.class)) {
                util.when(() -> LogicUtil.byteToNumber(bytes)).thenReturn(value);
                util.when(() -> LogicUtil.bytesToInt(bytes)).thenCallRealMethod();
                int bytesToInt = LogicUtil.bytesToInt(bytes);
                Assertions.assertEquals(excepted, bytesToInt);
            }
        }

        @Test
        public void testZeroToInt() {
            assertVerify(0L, 0, null);
        }
    }
    @Nested
    class BytesToNumberTest {
        @Test
        public void testNullToNumber() {
            long number = LogicUtil.byteToNumber(null);
            Assertions.assertEquals(0, number);
        }

        @Test
        public void testEmptyToNumber() {
            long number = LogicUtil.byteToNumber(new byte[]{});
            Assertions.assertEquals(0, number);
        }
        @Test
        public void testNumber() {
            long number = LogicUtil.byteToNumber(new byte[]{0,0,0,93});
            Assertions.assertEquals(93, number);
        }
    }
    @Nested
    class BytesToLongTest {
        void assertVerify(Number value, Number excepted, byte[] bytes) {
            GaussDBConfig mock = mock(GaussDBConfig.class);
            try (MockedStatic<LogicUtil> util = mockStatic(LogicUtil.class)) {
                util.when(() -> LogicUtil.byteToNumber(bytes)).thenReturn(value);
                util.when(() -> LogicUtil.byteToLong(null)).thenCallRealMethod();
                long bytesToInt = LogicUtil.byteToLong(bytes);
                Assertions.assertEquals(excepted, bytesToInt);
            }
        }
        @Test
        public void testLongToLong() {
            assertVerify(Long.MAX_VALUE,Long.MAX_VALUE, null);
        }

        @Test
        public void testShortToLong() {
            assertVerify((long)Short.MAX_VALUE, (long)Short.MAX_VALUE, null);
        }
        @Test
        public void testZeroToInt() {
            assertVerify(0L, 0L, null);
        }
    }
    @Nested
    class Read2Test {
        ByteBuffer buffer;
        @BeforeEach
        void init() {
            buffer = mock(ByteBuffer.class);
        }
        @Test
        void testNormal() {
            when(buffer.hasRemaining()).thenReturn(true);
            when(buffer.get()).thenReturn((byte)1);
            byte[] read = LogicUtil.read(buffer, 2);
            Assertions.assertEquals(2, read.length);
            Assertions.assertEquals((byte)1, read[0]);
            Assertions.assertEquals((byte)1, read[1]);
        }
        @Test
        void testNotRemain() {
            when(buffer.hasRemaining()).thenReturn(false);
            when(buffer.get()).thenReturn((byte)1);
            byte[] read = LogicUtil.read(buffer, 2);
            Assertions.assertEquals(2, read.length);
            Assertions.assertEquals((byte)0, read[0]);
            Assertions.assertEquals((byte)0, read[1]);
        }
    }
    @Nested
    class Read3Test {
        ByteBuffer buffer;
        @BeforeEach
        void init() {
            buffer = mock(ByteBuffer.class);
        }
        void assertVerify(int excepted) {
            try (MockedStatic<LogicUtil> util = mockStatic(LogicUtil.class)) {
                util.when(() -> LogicUtil.read(any(ByteBuffer.class), anyInt())).thenReturn(null);
                util.when(() -> LogicUtil.bytesToInt(null)).thenReturn(excepted);
                util.when(() -> LogicUtil.read(any(ByteBuffer.class), anyInt(), anyInt())).thenCallRealMethod();
                byte[] bytesToInt = LogicUtil.read(buffer, 1, 1);
            }
        }

        @Test
        void testValueLessThanZero() {
            Assertions.assertThrows(IllegalDataLengthException.class, () ->{
                assertVerify(-1);
            });
        }

        @Test
        void testValueMoreThanZero() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1);
            });
        }
    }
    @Nested
    class ReadValueTest {
        ByteBuffer buffer;
        @BeforeEach
        void init() {
            buffer = mock(ByteBuffer.class);
        }
        byte[] assertVerify(int excepted) {
            try (MockedStatic<LogicUtil> util = mockStatic(LogicUtil.class)) {
                util.when(() -> LogicUtil.read(any(ByteBuffer.class), anyInt())).thenReturn(null);
                util.when(() -> LogicUtil.bytesToInt(null)).thenReturn(excepted);
                util.when(() -> LogicUtil.readValue(any(ByteBuffer.class), anyInt(), anyInt())).thenCallRealMethod();
                return LogicUtil.readValue(buffer, 1, 1);
            }
        }

        @Test
        void testValueLessThanZero() {
            Assertions.assertThrows(IllegalDataLengthException.class, () ->{
                assertVerify(-2);
            });
        }

        @Test
        void testValueMoreThanZero() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1);
            });
        }

        @Test
        void testValueIsBYTES_VALUE_OF_NULL() {
            Assertions.assertDoesNotThrow(() -> {
                byte[] bytes = assertVerify(CdcConstant.BYTES_VALUE_OF_NULL);
                Assertions.assertNull(bytes);
            });
        }

        @Test
        void testValueIsBYTES_VALUE_OF_EMPTY_CHAR() {
            Assertions.assertDoesNotThrow(() -> {
                byte[] bytes = assertVerify(CdcConstant.BYTES_VALUE_OF_EMPTY_CHAR);
                Assertions.assertEquals("", new String(bytes));
            });
        }
    }
}
