//package io.tapdata.mongodb.batch;
//
//import com.mongodb.MongoInterruptedException;
//import com.mongodb.client.FindIterable;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoCursor;
//import io.tapdata.base.ConnectorBase;
//import io.tapdata.entity.event.TapEvent;
//import io.tapdata.entity.logger.Log;
//import io.tapdata.entity.schema.TapTable;
//import io.tapdata.mongodb.MongoBatchOffset;
//import io.tapdata.mongodb.MongodbExceptionCollector;
//import io.tapdata.mongodb.entity.MongodbConfig;
//import io.tapdata.mongodb.entity.ReadParam;
//import io.tapdata.mongodb.reader.StreamWithOpLogCollection;
//import io.tapdata.pdk.apis.context.TapConnectorContext;
//import org.bson.Document;
//import org.bson.conversions.Bson;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Nested;
//import org.junit.jupiter.api.Test;
//import org.mockito.MockedStatic;
//import org.springframework.test.util.ReflectionTestUtils;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.function.BiConsumer;
//import java.util.function.BooleanSupplier;
//
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.ArgumentMatchers.anyInt;
//import static org.mockito.ArgumentMatchers.anyList;
//import static org.mockito.ArgumentMatchers.anyString;
//import static org.mockito.Mockito.doCallRealMethod;
//import static org.mockito.Mockito.doNothing;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.mockStatic;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//class BatchReadEventTest {
//    MongoBatchReader batchReadEvent;
//    BiConsumer<List<TapEvent>, Object> tapReadOffsetConsumer;
//    MongodbExceptionCollector exceptionCollector;
//    TapConnectorContext connectorContext;
//    MongoBatchOffset batchOffset;
//    BooleanSupplier checkAlive;
//    ErrorHandler errorHandler;
//    MongodbConfig mongoConfig;
//    int eventBatchSize;
//    TapTable table;
//    MongoBatchOffset offset;
//    CollectionCollector collectionCollector;
//    ReadParam param;
//    @BeforeEach
//    void init() {
//        batchReadEvent = mock(MongoBatchReader.class);
//        tapReadOffsetConsumer = mock(BiConsumer.class);
//        ReflectionTestUtils.setField(batchReadEvent, "tapReadOffsetConsumer", tapReadOffsetConsumer);
//        exceptionCollector = new MongodbExceptionCollector();
//        ReflectionTestUtils.setField(batchReadEvent, "exceptionCollector", exceptionCollector);
//        connectorContext = mock(TapConnectorContext.class);
//        ReflectionTestUtils.setField(batchReadEvent, "connectorContext", connectorContext);
//        batchOffset = mock(MongoBatchOffset.class);
//        ReflectionTestUtils.setField(batchReadEvent, "batchOffset", batchOffset);
//        checkAlive = mock(BooleanSupplier.class);
//        ReflectionTestUtils.setField(batchReadEvent, "checkAlive", checkAlive);
//        errorHandler = mock(ErrorHandler.class);
//        ReflectionTestUtils.setField(batchReadEvent, "errorHandler", errorHandler);
//        mongoConfig = mock(MongodbConfig.class);
//        ReflectionTestUtils.setField(batchReadEvent, "mongoConfig", mongoConfig);
//        eventBatchSize = 100;
//        ReflectionTestUtils.setField(batchReadEvent, "eventBatchSize", eventBatchSize);
//        table = mock(TapTable.class);
//        ReflectionTestUtils.setField(batchReadEvent, "table", table);
//        offset = mock(MongoBatchOffset.class);
//        ReflectionTestUtils.setField(batchReadEvent, "offset", offset);
//        collectionCollector = mock(CollectionCollector.class);
//
//        param = ReadParam.of()
//                .withConnectorContext(connectorContext)
//                .withTapTable(table)
//                .withOffset(offset)
//                .withEventBatchSize(eventBatchSize)
//                .withTapReadOffsetConsumer(tapReadOffsetConsumer)
//                .withCheckAlive(checkAlive)
//                .withMongodbConfig(mongoConfig)
//                .withBatchOffset(batchOffset)
//                .withMongodbExceptionCollector(exceptionCollector)
//                .withMongoCollection(collectionCollector)
//                .withErrorHandler(errorHandler);
//    }
//
//    @Nested
//    class NewObjectTest {
//        @Test
//        void testNewByOf() {
//            when(mongoConfig.getDatabase()).thenReturn("test");
//            when(table.getId()).thenReturn(StreamWithOpLogCollection.OP_LOG_COLLECTION);
//            Assertions.assertDoesNotThrow(() -> MongoBatchReader.of(param));
//            verify(mongoConfig).getDatabase();
//            verify(table, times(0)).getId();
//        }
//        @Test
//        void testDataBaseNotLocal() {
//            when(mongoConfig.getDatabase()).thenReturn("test");
//            when(table.getId()).thenReturn(StreamWithOpLogCollection.OP_LOG_COLLECTION);
//            Assertions.assertDoesNotThrow(() -> new MongoBatchReader(param));
//            verify(mongoConfig).getDatabase();
//            verify(table, times(0)).getId();
//        }
//        @Test
//        void testCollectionNotOpLogDotRS() {
//            when(mongoConfig.getDatabase()).thenReturn("test");
//            when(table.getId()).thenReturn("TapData");
//            Assertions.assertDoesNotThrow(() -> new MongoBatchReader(param));
//            verify(mongoConfig).getDatabase();
//            verify(table, times(0)).getId();
//        }
//        @Test
//        void testReadOpLog() {
//            when(mongoConfig.getDatabase()).thenReturn(StreamWithOpLogCollection.OP_LOG_DB);
//            when(table.getId()).thenReturn(StreamWithOpLogCollection.OP_LOG_COLLECTION);
//            Assertions.assertDoesNotThrow(() -> new MongoBatchReader(param));
//            verify(mongoConfig).getDatabase();
//            verify(table).getId();
//        }
//    }
//
//    @Nested
//    class ConvertTest {
//        @Test
//        void testNormal() {
//            Document document = mock(Document.class);
//            when(batchReadEvent.convert(document)).thenCallRealMethod();
//            Assertions.assertEquals(document, batchReadEvent.convert(document));
//        }
//    }
//
//    @Nested
//    class FindIterableTest {
//        MongoCollection<Document> collection;
//        Log log;
//        FindIterable<Document> findIterable;
//        Object offsetValue;
//        @BeforeEach
//        void init() {
//            batchReadEvent.offsetKey = "key";
//            batchReadEvent.sort = mock(Bson.class);
//            findIterable = mock(FindIterable.class);
//            collection = mock(MongoCollection.class);
//            log = mock(Log.class);
//            offsetValue = 0L;
//            when(connectorContext.getLog()).thenReturn(log);
//            when(table.getId()).thenReturn("id");
//            when(collectionCollector.collectCollection("id")).thenReturn(collection);
//            when(collection.find()).thenReturn(findIterable);
//            when(findIterable.sort(any(Bson.class))).thenReturn(findIterable);
//            when(findIterable.batchSize(anyInt())).thenReturn(findIterable);
//            when(offset.value()).thenReturn(offsetValue);
//            when(batchReadEvent.queryCondition(anyString(), any())).thenReturn(mock(Bson.class));
//            when(collection.find(any(Bson.class))).thenReturn(findIterable);
//            doNothing().when(log).warn("Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
//            when(mongoConfig.isNoCursorTimeout()).thenReturn(true);
//            when(findIterable.noCursorTimeout(true)).thenReturn(findIterable);
//
//            when(batchReadEvent.findIterable(param)).thenCallRealMethod();
//        }
//        @Test
//        void testNormal() {
//            Assertions.assertEquals(findIterable, batchReadEvent.findIterable(param));
//            verify(connectorContext).getLog();
//            verify(table).getId();
//            verify(collectionCollector).collectCollection("id");
//            verify(collection, times(0)).find();
//            verify(findIterable).sort(any(Bson.class));
//            verify(findIterable).batchSize(anyInt());
//            verify(offset).value();
//            verify(batchReadEvent).queryCondition(anyString(), any());
//            verify(collection).find(any(Bson.class));
//            verify(log, times(0)).warn("Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
//            verify(mongoConfig).isNoCursorTimeout();
//            verify(findIterable).noCursorTimeout(true);
//        }
//        @Test
//        void testEventBatchSizeLessThanZero() {
//            ReflectionTestUtils.setField(batchReadEvent, "eventBatchSize", 0);
//            ReflectionTestUtils.setField(batchReadEvent, "offset", null);
//            when(mongoConfig.isNoCursorTimeout()).thenReturn(false);
//            Assertions.assertEquals(findIterable, batchReadEvent.findIterable(param));
//            verify(connectorContext).getLog();
//            verify(table).getId();
//            verify(collectionCollector).collectCollection("id");
//            verify(collection).find();
//            verify(findIterable).sort(any(Bson.class));
//            verify(findIterable).batchSize(anyInt());
//            verify(offset, times(0)).value();
//            verify(batchReadEvent, times(0)).queryCondition(anyString(), any());
//            verify(collection, times(0)).find(any(Bson.class));
//            verify(log, times(0)).warn("Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
//            verify(mongoConfig).isNoCursorTimeout();
//            verify(findIterable, times(0)).noCursorTimeout(true);
//        }
//        @Test
//        void testOffsetValueIsNull() {
//            when(offset.value()).thenReturn(null);
//            when(mongoConfig.isNoCursorTimeout()).thenReturn(false);
//            Assertions.assertEquals(findIterable, batchReadEvent.findIterable(param));
//            verify(connectorContext).getLog();
//            verify(table).getId();
//            verify(collectionCollector).collectCollection("id");
//            verify(collection).find();
//            verify(findIterable).sort(any(Bson.class));
//            verify(findIterable).batchSize(anyInt());
//            verify(offset).value();
//            verify(batchReadEvent, times(0)).queryCondition(anyString(), any());
//            verify(collection, times(0)).find(any(Bson.class));
//            verify(log, times(1)).warn("Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
//            verify(mongoConfig).isNoCursorTimeout();
//            verify(findIterable, times(0)).noCursorTimeout(true);
//        }
//    }
//
//    @Nested
//    class BatchReadCollectionTest {
//        FindIterable<Document> findIterable;
//        MongoCursor<Document> mongoCursor;
//        Document lastDocument;
//        @BeforeEach
//        void init() {
//            findIterable = mock(FindIterable.class);
//            mongoCursor = mock(MongoCursor.class);
//            lastDocument= mock(Document.class);
//            when(batchReadEvent.findIterable(param)).thenReturn(findIterable);
//            when(findIterable.iterator()).thenReturn(mongoCursor);
//            when(mongoCursor.hasNext()).thenReturn(true, true, false);
//            when(checkAlive.getAsBoolean()).thenReturn(false, true);
//            when(mongoCursor.next()).thenReturn(lastDocument);
//            when(batchReadEvent.emit(any(Document.class), anyList())).thenReturn(mock(List.class));
//            doNothing().when(batchReadEvent).doException(any(Exception.class));
//            doNothing().when(batchReadEvent).afterEmit(anyList(), any(Document.class));
//            doNothing().when(mongoCursor).close();
//            doCallRealMethod().when(batchReadEvent).batchReadCollection(param);
//        }
//        @Test
//        void testReadNormal() {
//            Assertions.assertDoesNotThrow(() -> batchReadEvent.batchReadCollection(param));
//            verify(batchReadEvent).findIterable(param);
//            verify(findIterable).iterator();
//            verify(mongoCursor, times(1)).hasNext();
//            verify(checkAlive, times(1)).getAsBoolean();
//            verify(mongoCursor, times(1)).next();
//            verify(batchReadEvent, times(1)).emit(any(Document.class), anyList());
//            verify(batchReadEvent, times(0)).doException(any(Exception.class));
//            verify(batchReadEvent).afterEmit(anyList(), any(Document.class));
//            verify(mongoCursor).close();
//        }
//        @Test
//        void testException() {
//            List<TapEvent> l = mock(List.class);
//            try (MockedStatic<ConnectorBase> cb = mockStatic(ConnectorBase.class)) {
//                cb.when(ConnectorBase::list).thenReturn(l);
//                doNothing().when(batchReadEvent).afterEmit(l, null);
//                when(mongoCursor.hasNext()).thenAnswer(a -> {
//                    throw new IOException("io");
//                });
//                Assertions.assertDoesNotThrow(() -> batchReadEvent.batchReadCollection(param));
//                verify(batchReadEvent).findIterable(param);
//                verify(findIterable).iterator();
//                verify(mongoCursor, times(1)).hasNext();
//                verify(checkAlive, times(0)).getAsBoolean();
//                verify(mongoCursor, times(0)).next();
//                verify(batchReadEvent, times(0)).emit(any(Document.class), anyList());
//                verify(batchReadEvent, times(1)).doException(any(Exception.class));
//                verify(batchReadEvent).afterEmit(l, null);
//                verify(mongoCursor, times(1)).close();
//            }
//        }
//    }
//
//    @Nested
//    class EmitTest {
//        Document lastDocument;
//        List<TapEvent> tapEvents;
//        @BeforeEach
//        void init() {
//            lastDocument = mock(Document.class);
//            tapEvents = mock(List.class);
//
//            when(table.getId()).thenReturn("id");
//            when(batchReadEvent.convert(lastDocument)).thenReturn(lastDocument);
//            when(tapEvents.add(any())).thenReturn(true);
//            when(tapEvents.size()).thenReturn(batchReadEvent.eventBatchSize);
//            doNothing().when(tapReadOffsetConsumer).accept(tapEvents, batchReadEvent.batchOffset);
//            when(batchReadEvent.emit(lastDocument, tapEvents)).thenCallRealMethod();
//            when(batchReadEvent.findMongoBatchOffset(lastDocument)).thenReturn(batchOffset);
//            ReflectionTestUtils.setField(batchReadEvent, "offsetKey", "offsetKey");
//        }
//
//        @Test
//        void testNormal() {
//            List<TapEvent> emit = batchReadEvent.emit(lastDocument, tapEvents);
//            Assertions.assertNotNull(emit);
//            Assertions.assertEquals(0, emit.size());
//            verify(table).getId();
//            verify(batchReadEvent).convert(lastDocument);
//            verify(tapEvents).add(any());
//            verify(tapEvents).size();
//            verify(batchReadEvent).findMongoBatchOffset(lastDocument);
//            verify(tapReadOffsetConsumer).accept(tapEvents, batchOffset);
//        }
//
//        @Test
//        void testListSizeNotEqualsEventBatchSize() {
//            when(tapEvents.size()).thenReturn(batchReadEvent.eventBatchSize - 1);
//            List<TapEvent> emit = batchReadEvent.emit(lastDocument, tapEvents);
//            Assertions.assertNotNull(emit);
//            Assertions.assertEquals(tapEvents, emit);
//            verify(table).getId();
//            verify(batchReadEvent).convert(lastDocument);
//            verify(tapEvents).add(any());
//            verify(tapEvents).size();
//            verify(batchReadEvent, times(0)).findMongoBatchOffset(lastDocument);
//            verify(tapReadOffsetConsumer, times(0)).accept(tapEvents, batchOffset);
//        }
//    }
//
//    @Nested
//    class FindMongoBatchOffsetTest {
//        Document lastDocument;
//        @BeforeEach
//        void init() {
//            batchReadEvent.offsetKey = "key";
//            lastDocument = mock(Document.class);
//            when(lastDocument.get(anyString())).thenReturn(mock(Object.class));
//            when(batchReadEvent.findMongoBatchOffset(lastDocument)).thenCallRealMethod();
//        }
//        @Test
//        void testNormal() {
//            MongoBatchOffset mongoBatchOffset = batchReadEvent.findMongoBatchOffset(lastDocument);
//            Assertions.assertNotNull(mongoBatchOffset);
//            verify(lastDocument).get(anyString());
//        }
//
//    }
//
//    @Nested
//    class AfterEmitTest {
//        List<TapEvent> tapEvents;
//        Document lastDocument;
//        @BeforeEach
//        void init() {
//            lastDocument = mock(Document.class);
//            tapEvents = mock(List.class);
//            when(batchReadEvent.findMongoBatchOffset(lastDocument)).thenReturn(batchOffset);
//            doNothing().when(tapReadOffsetConsumer).accept(tapEvents, null);
//            doNothing().when(tapReadOffsetConsumer).accept(tapEvents, batchOffset);
//            when(tapEvents.isEmpty()).thenReturn(false);
//            doCallRealMethod().when(batchReadEvent).afterEmit(tapEvents, lastDocument);
//        }
//        @Test
//        void testNormal() {
//            batchReadEvent.afterEmit(tapEvents, lastDocument);
//            verify(tapEvents).isEmpty();
//            verify(batchReadEvent).findMongoBatchOffset(lastDocument);
//            verify(tapReadOffsetConsumer).accept(tapEvents, batchOffset);
//        }
//        @Test
//        void tetsListIsEmpty() {
//            when(tapEvents.isEmpty()).thenReturn(true);
//            batchReadEvent.afterEmit(tapEvents, lastDocument);
//            verify(tapEvents).isEmpty();
//            verify(batchReadEvent, times(0)).findMongoBatchOffset(lastDocument);
//            verify(tapReadOffsetConsumer, times(0)).accept(tapEvents, batchOffset);
//        }
//        @Test
//        void tetsLastDocumentIsNull() {
//            doCallRealMethod().when(batchReadEvent).afterEmit(tapEvents, null);
//            batchReadEvent.afterEmit(tapEvents, null);
//            verify(tapEvents).isEmpty();
//            verify(batchReadEvent, times(0)).findMongoBatchOffset(lastDocument);
//            verify(tapReadOffsetConsumer, times(1)).accept(tapEvents, null);
//        }
//    }
//
//    @Nested
//    class DoException {
//        Exception e;
//        @BeforeEach
//        void init() {
//            e = mock(Exception.class);
//            when(checkAlive.getAsBoolean()).thenReturn(true);
//            doNothing().when(errorHandler).doHandle(e);
//            doCallRealMethod().when(batchReadEvent).doException(e);
//        }
//        @Test
//        void testNormal() {
//            Assertions.assertDoesNotThrow(() -> batchReadEvent.doException(e));
//            verify(checkAlive).getAsBoolean();
//            verify(errorHandler).doHandle(e);
//        }
//        @Test
//        void testIsNotStop() {
//            when(checkAlive.getAsBoolean()).thenReturn(false);
//            Assertions.assertDoesNotThrow(() -> batchReadEvent.doException(e));
//            verify(checkAlive).getAsBoolean();
//            verify(errorHandler).doHandle(e);
//        }
//        @Test
//        void testIsNotStopAndIsMongoInterruptedException() {
//            e = mock(MongoInterruptedException.class);
//            when(checkAlive.getAsBoolean()).thenReturn(false);
//            Assertions.assertDoesNotThrow(() -> batchReadEvent.doException(e));
//            verify(checkAlive, times(0)).getAsBoolean();
//            verify(errorHandler, times(0)).doHandle(e);
//        }
//    }
//
//    @Nested
//    class QueryConditionTest {
//        @Test
//        void testNormal() {
//            when(batchReadEvent.queryCondition(anyString(), any())).thenCallRealMethod();
//            Bson key = batchReadEvent.queryCondition("key", 100);
//            Assertions.assertNotNull(key);
//        }
//    }
//}