package io.tapdata.connector.gauss.cdc.logic.event;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventTest {

    @Nested
    class EventEntityTest {
        Event.EventEntity<String> entity;
        @BeforeEach
        void init() {
            entity = mock(Event.EventEntity.class);
        }

        @Nested
        class EventParamTest {
            @Test
            void testEvent() {
                when(entity.event()).thenCallRealMethod();
                when(entity.event(anyString())).thenCallRealMethod();
                Assertions.assertDoesNotThrow(()-> entity.event());
                Assertions.assertDoesNotThrow(()-> entity.event(""));
            }
        }

        @Nested
        class XidParamTest {
            @Test
            void testEvent() {
                when(entity.xid()).thenCallRealMethod();
                when(entity.xid(anyString())).thenCallRealMethod();
                Assertions.assertDoesNotThrow(()-> entity.xid());
                Assertions.assertDoesNotThrow(()-> entity.xid(""));
            }
        }

        @Nested
        class TimestampParamTest {
            @Test
            void testEvent() {
                when(entity.timestamp()).thenCallRealMethod();
                when(entity.timestamp(1L)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(()-> entity.timestamp());
                Assertions.assertDoesNotThrow(()-> entity.timestamp(1L));
            }
        }

        @Nested
        class CsnParamTest {
            @Test
            void testEvent() {
                when(entity.csn()).thenCallRealMethod();
                when(entity.csn(1L)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(()-> entity.csn());
                Assertions.assertDoesNotThrow(()-> entity.csn(1L));
            }
        }

        @Nested
        class LsnParamTest {
            @Test
            void testEvent() {
                when(entity.lsn()).thenCallRealMethod();
                when(entity.lsn(1L)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(()-> entity.lsn());
                Assertions.assertDoesNotThrow(()-> entity.lsn(1L));
            }
        }
    }
}
