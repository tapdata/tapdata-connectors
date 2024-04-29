package io.tapdata.mongodb.reader;

import io.tapdata.entity.event.TapBaseEvent;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MongodbOpLogStreamV3ReaderTest {
    MongodbOpLogStreamV3Reader mongodbOpLogStreamV3Reader;
    Set<String> namespaces;
    @BeforeEach
    void init() {
        namespaces = new HashSet<>();
        mongodbOpLogStreamV3Reader = mock(MongodbOpLogStreamV3Reader.class);
        ReflectionTestUtils.setField(mongodbOpLogStreamV3Reader, "namespaces", namespaces);
    }

    @Test
    void testOf() {
        Assertions.assertDoesNotThrow(MongodbOpLogStreamV3Reader::of);
    }

    @Nested
    class HandleOpLogEventTest {
        Document event;
        TapBaseEvent tapBaseEvent;
        @BeforeEach
        void init() {
            event = mock(Document.class);
            tapBaseEvent = mock(TapBaseEvent.class);
            when(mongodbOpLogStreamV3Reader.toOpLog(event, namespaces)).thenReturn(tapBaseEvent);

        }
        @Test
        void testNormal() {
            when(mongodbOpLogStreamV3Reader.handleOplogEvent(event)).thenCallRealMethod();
            TapBaseEvent e = mongodbOpLogStreamV3Reader.handleOplogEvent(event);
            Assertions.assertNotNull(e);
            Assertions.assertEquals(tapBaseEvent, e);
            verify(mongodbOpLogStreamV3Reader).toOpLog(event, namespaces);
        }
        @Test
        void testNullDocument() {
            when(mongodbOpLogStreamV3Reader.handleOplogEvent(null)).thenCallRealMethod();
            TapBaseEvent e = mongodbOpLogStreamV3Reader.handleOplogEvent(null);
            Assertions.assertNull(e);
            verify(mongodbOpLogStreamV3Reader, times(0)).toOpLog(event, namespaces);
        }
    }
}