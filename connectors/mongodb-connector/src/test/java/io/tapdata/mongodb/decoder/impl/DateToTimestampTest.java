package io.tapdata.mongodb.decoder.impl;


import io.tapdata.entity.error.CoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DateToTimestampTest {
    DateToTimestamp dateToTimestamp;
    @BeforeEach
    void init() {
        dateToTimestamp = mock(DateToTimestamp.class);
    }
    @Test
    void testParams() {
        Assertions.assertEquals("$dateToTimestamp", DateToTimestamp.DATE_TO_TIMESTAMP);
    }
    @Nested
    class GetFunctionNameTest{
        @Test
        void testNormal() {
            when(dateToTimestamp.getFunctionName()).thenCallRealMethod();
            Assertions.assertEquals("$dateToTimestamp", dateToTimestamp.getFunctionName());
        }
    }
    @Nested
    class ExecuteTest{
        @Test
        void testDate() {
            Date date = mock(Date.class);
            when(dateToTimestamp.execute(date, null)).thenCallRealMethod();
            when(date.getTime()).thenReturn(0L);
            Assertions.assertDoesNotThrow(() -> dateToTimestamp.execute(date, null));
            verify(date, times(1)).getTime();
        }

        @Test
        void testNull() {
            when(dateToTimestamp.execute(null, null)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> dateToTimestamp.execute(null, null));
        }

        @Test
        void testNotDate() {
            String date = "2024-04-08 22:00:00.000";
            when(dateToTimestamp.execute(date, null)).thenCallRealMethod();
            Assertions.assertThrows(CoreException.class, () -> dateToTimestamp.execute(date, null));
        }
    }
}