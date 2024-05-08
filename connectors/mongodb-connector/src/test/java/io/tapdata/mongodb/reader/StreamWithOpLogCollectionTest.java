package io.tapdata.mongodb.reader;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamWithOpLogCollectionTest {
    @Test
    void testParam() {
        Assertions.assertEquals("oplog.rs", StreamWithOpLogCollection.OP_LOG_COLLECTION);
        Assertions.assertEquals("local", StreamWithOpLogCollection.OP_LOG_DB);
        Assertions.assertEquals("local.oplog.rs", StreamWithOpLogCollection.OP_LOG_FULL_NAME);
    }

    @Nested
    class DefaultFunctionTest {
        StreamWithOpLogCollection streamWithOpLogCollection;
        @BeforeEach
        void init() {
            streamWithOpLogCollection = mock(StreamWithOpLogCollection.class);
        }
        @Nested
        class ToOpLogTest {
            Document opLogEvent;
            Set<String> namespaces;
            @BeforeEach
            void init() {
                opLogEvent = mock(Document.class);
                namespaces = mock(Set.class);
                when(streamWithOpLogCollection.toOpLog(opLogEvent, namespaces)).thenCallRealMethod();
            }
            @Test
            void testNotContainsOP_LOG_FULL_NAME() {
                when(namespaces.contains(StreamWithOpLogCollection.OP_LOG_FULL_NAME)).thenReturn(false);
                TapBaseEvent tapBaseEvent = streamWithOpLogCollection.toOpLog(opLogEvent, namespaces);
                Assertions.assertNull(tapBaseEvent);
                verify(namespaces).contains(StreamWithOpLogCollection.OP_LOG_FULL_NAME);
            }
            @Test
            void testContainsOP_LOG_FULL_NAME() {
                when(namespaces.contains(StreamWithOpLogCollection.OP_LOG_FULL_NAME)).thenReturn(true);
                TapBaseEvent tapBaseEvent = streamWithOpLogCollection.toOpLog(opLogEvent, namespaces);
                Assertions.assertNotNull(tapBaseEvent);
                Assertions.assertEquals(TapInsertRecordEvent.class.getName(), tapBaseEvent.getClass().getName());
                Assertions.assertEquals(StreamWithOpLogCollection.OP_LOG_COLLECTION, tapBaseEvent.getTableId());
                Assertions.assertEquals(opLogEvent, ((TapInsertRecordEvent)tapBaseEvent).getAfter());
                verify(namespaces).contains(StreamWithOpLogCollection.OP_LOG_FULL_NAME);
            }
        }
    }
}